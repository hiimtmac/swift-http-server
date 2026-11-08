//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift HTTP Server open source project
//
// Copyright (c) 2026 Apple Inc. and the Swift HTTP Server project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift HTTP Server project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import BasicContainers
import Logging
import NIOCore
import NIOPosix
import Testing

@testable import NIOHTTPServer

@Suite
struct NIOHTTPServerConnectionCloseTests {
    /// RFC 9112 section 9.6: a server that receives a `close` connection option must close the connection after
    /// sending its response, and an HTTP/1.0 request without `keep-alive` is not persistent. The handler reads the
    /// whole request, so the connection could otherwise be reused.
    @Test(
        "Server closes the connection after the response when the client asks it to",
        .timeLimit(.minutes(1)),
        arguments: [
            "GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n",
            "GET / HTTP/1.1\r\nHost: test\r\nConnection: Upgrade, close\r\n\r\n",
            "POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbody",
            "GET / HTTP/1.0\r\nHost: test\r\n\r\n",
        ]
    )
    @available(anyAppleOS 27.0, *)
    func closesWhenRequested(request: String) async throws {
        let response = try await Self.exchange(request)
        #expect(response.hasPrefix("HTTP/1.1 200 OK\r\n"))
        #expect(response.lowercased().contains("\r\nconnection: close\r\n"))
        #expect(response.hasSuffix("hello"))
    }

    @Test("Server keeps a persistent connection open", .timeLimit(.minutes(1)))
    @available(anyAppleOS 27.0, *)
    func keepsPersistentConnection() async throws {
        // Two requests on one connection; the second asks to close, which ends the exchange.
        let response = try await Self.exchange(
            "GET / HTTP/1.1\r\nHost: test\r\n\r\nGET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"
        )
        let heads = response.components(separatedBy: "HTTP/1.1 200 OK\r\n")
        #expect(heads.count == 3)
        #expect(!heads[1].lowercased().contains("connection: close"))
        #expect(heads[2].lowercased().contains("connection: close"))
    }

    /// Sends `request` and reads until the server closes the connection. Server timeouts are disabled, so only
    /// honouring the request can end the exchange before the test's time limit.
    @available(anyAppleOS 27.0, *)
    static func exchange(_ request: String) async throws -> String {
        var configuration = try NIOHTTPServerConfiguration(
            bindTarget: .hostAndPort(host: "127.0.0.1", port: 0),
            supportedHTTPVersions: [.http1_1],
            transportSecurity: .plaintext
        )
        configuration.connectionTimeouts = .init(idle: nil, readHeader: nil, readBody: nil)
        let server = NIOHTTPServer(logger: Logger(label: "NIOHTTPServerConnectionCloseTests"), configuration: configuration)
        let handler: HTTPServerClosureRequestHandler<
            NIOHTTPServer.RequestContext, NIOHTTPServer.Reader, NIOHTTPServer.ResponseSender
        > = HTTPServerClosureRequestHandler { _, _, reader, responseSender in
            var reader = reader
            var collected = UniqueArray<UInt8>()
            collected.reserveCapacity(64)
            _ = try await reader.collect(into: &collected)
            var body = UniqueArray<UInt8>(copying: Array("hello".utf8))
            try await responseSender.sendAndFinish(
                .init(status: .ok, headerFields: [.contentLength: "5"]),
                buffer: &body
            )
        }

        return try await withThrowingTaskGroup { group in
            group.addTask { try await server.serve(handler: handler) }
            let address = try #require(try await server.listeningAddresses.first)

            let client = try await ClientBootstrap(group: .singletonMultiThreadedEventLoopGroup)
                .connect(host: address.host, port: address.port) { channel in
                    channel.eventLoop.makeCompletedFuture {
                        try NIOAsyncChannel<ByteBuffer, ByteBuffer>(wrappingChannelSynchronously: channel)
                    }
                }
            let response = try await client.executeThenClose { inbound, outbound in
                try await outbound.write(ByteBuffer(string: request))
                var bytes = ByteBuffer()
                for try await chunk in inbound { bytes.writeImmutableBuffer(chunk) }
                return String(buffer: bytes)
            }
            group.cancelAll()
            return response
        }
    }
}
