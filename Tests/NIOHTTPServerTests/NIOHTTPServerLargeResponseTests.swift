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
import NIOHTTP2
import NIOHTTPTypes
import NIOPosix
import Testing

@testable import NIOHTTPServer

@Suite
struct NIOHTTPServerLargeResponseTests {
    let clientLogger = Logger(label: "NIOHTTPServerLargeResponseTests.client")
    let serverLogger = Logger(label: "NIOHTTPServerLargeResponseTests.server")

    /// A response larger than the HTTP/2 flow-control window is still being delivered when the handler returns; the
    /// stream must not be reset before the client has read it.
    @Test(
        "A buffered response larger than the flow-control window is delivered completely",
        .timeLimit(.minutes(1)),
        arguments: [NIOHTTPServer.HTTPVersion.plaintextHTTP1_1, .http1_1, .http2]
    )
    @available(anyAppleOS 27.0, *)
    func largeBufferedResponse(httpVersion: NIOHTTPServer.HTTPVersion) async throws {
        let size = 1 << 20
        let (server, clientConfiguration) = try TestHelpers.makeServerAndClientConfiguration(
            for: httpVersion,
            clientLogger: self.clientLogger,
            serverLogger: self.serverLogger
        )
        let handler: HTTPServerClosureRequestHandler<
            NIOHTTPServer.RequestContext, NIOHTTPServer.Reader, NIOHTTPServer.ResponseSender
        > = HTTPServerClosureRequestHandler { _, _, _, responseSender in
            var body = UniqueArray<UInt8>(repeating: 7, count: size)
            try await responseSender.sendAndFinish(.init(status: .ok), buffer: &body)
        }

        try await withThrowingTaskGroup { group in
            group.addTask { try await server.serve(handler: handler) }
            let address = try #require(try await server.listeningAddresses.first)

            try await TestClientConnection.withConnectedRequestChannel(
                configuration: clientConfiguration,
                serverAddress: address
            ) { inbound, outbound in
                try await outbound.write(.testHead(method: .get, for: httpVersion))
                try await outbound.write(.end(nil))

                var received = 0
                responseLoop: for try await part in inbound {
                    switch part {
                    case .head(let head):
                        #expect(head.status == .ok)
                    case .body(let body):
                        received += body.readableBytes
                    case .end:
                        break responseLoop
                    }
                }
                #expect(received == size)
            }
            group.cancelAll()
        }
    }

    @Test("An HTTP/2 stream is reset when its handler returns without a response", .timeLimit(.minutes(1)))
    @available(anyAppleOS 27.0, *)
    func unconcludedHTTP2Response() async throws {
        let (server, clientConfiguration) = try TestHelpers.makeServerAndClientConfiguration(
            for: .http2,
            clientLogger: self.clientLogger,
            serverLogger: self.serverLogger
        )
        let handler: HTTPServerClosureRequestHandler<
            NIOHTTPServer.RequestContext, NIOHTTPServer.Reader, NIOHTTPServer.ResponseSender
        > = HTTPServerClosureRequestHandler { _, _, _, _ in }

        try await withThrowingTaskGroup { group in
            group.addTask { try await server.serve(handler: handler) }
            let address = try #require(try await server.listeningAddresses.first)

            try await TestClientConnection.withConnectedRequestChannel(
                configuration: clientConfiguration,
                serverAddress: address
            ) { inbound, outbound in
                try await outbound.write(.testHead(method: .get, for: .http2))
                try await outbound.write(.end(nil))
                let error = await #expect(throws: NIOHTTP2Errors.StreamClosed.self) {
                    for try await _ in inbound {}
                }
                #expect(error?.errorCode == .cancel)
            }
            group.cancelAll()
        }
    }
}
