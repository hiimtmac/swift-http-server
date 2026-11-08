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
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTPTypes
import NIOPosix
import ServiceLifecycle
import ServiceLifecycleTestKit
import Testing

@testable import NIOHTTPServer

@Suite
struct NIOHTTPServerAcceptedChannelTests {
    let clientLogger = Logger(label: "NIOHTTPServerAcceptedChannelTests.client")
    let serverLogger = Logger(label: "NIOHTTPServerAcceptedChannelTests.server")

    @Test(
        "Serves a request on a channel accepted by the caller",
        arguments: [NIOHTTPServer.HTTPVersion.plaintextHTTP1_1, .http1_1, .http2]
    )
    @available(anyAppleOS 27.0, *)
    func servesAcceptedChannel(httpVersion: NIOHTTPServer.HTTPVersion) async throws {
        let (server, clientConfiguration) = try TestHelpers.makeServerAndClientConfiguration(
            for: httpVersion,
            clientLogger: self.clientLogger,
            serverLogger: self.serverLogger
        )
        let observedRemoteAddresses = NIOLockedValueBox<[NIOHTTPServer.SocketAddress?]>([])

        let handler: HTTPServerClosureRequestHandler<
            NIOHTTPServer.RequestContext, NIOHTTPServer.Reader, NIOHTTPServer.ResponseSender
        > = HTTPServerClosureRequestHandler { request, requestContext, requestReader, responseSender in
            var requestReader = requestReader
            var requestFinished = false
            while !requestFinished {
                try await requestReader.read { if $1 != nil { requestFinished = true } }
            }
            var buffer = UniqueArray<UInt8>(copying: [1, 2])
            try await responseSender.sendAndFinish(.init(status: .ok), buffer: &buffer)
        }

        try await AcceptingListener.withListener { channel in
            try await server.serve(acceptedChannel: channel) { connection, context in
                observedRemoteAddresses.withLockedValue { $0.append(context.remoteAddress) }
                try await connection.handleRequests(handler: handler)
            }
        } body: { address in
            try await TestClientConnection.withConnectedRequestChannel(
                configuration: clientConfiguration,
                serverAddress: address
            ) { inbound, outbound in
                try await outbound.write(.testHead(method: .post, for: httpVersion))
                try await outbound.write(.testBody)
                try await outbound.write(.testEnd)

                // An HTTP/1.1 connection stays open after the response, so stop at its end.
                var status: HTTPResponse.Status?
                var body = ByteBuffer()
                responseLoop: for try await response in inbound {
                    switch response {
                    case .head(let head):
                        status = head.status
                    case .body(let part):
                        body.writeImmutableBuffer(part)
                    case .end:
                        break responseLoop
                    }
                }
                #expect(status == .ok)
                #expect(body == ByteBuffer(bytes: [1, 2]))
            }
        }

        let remoteAddresses = observedRemoteAddresses.withLockedValue { $0 }
        #expect(remoteAddresses.count == 1)
        #expect(remoteAddresses.first??.host == "127.0.0.1")

        await #expect(throws: ListeningAddressError.self) {
            try await server.listeningAddresses
        }
    }

    @Test(
        "Accepted connection completes its request when graceful shutdown is triggered",
        arguments: [NIOHTTPServer.HTTPVersion.plaintextHTTP1_1, .http2]
    )
    @available(anyAppleOS 27.0, *)
    func acceptedChannelCompletesWhenGracefullyShutdown(httpVersion: NIOHTTPServer.HTTPVersion) async throws {
        let (server, clientConfiguration) = try TestHelpers.makeServerAndClientConfiguration(
            for: httpVersion,
            clientLogger: self.clientLogger,
            serverLogger: self.serverLogger
        )
        let elg = MultiThreadedEventLoopGroup.singletonMultiThreadedEventLoopGroup
        let firstChunkReadPromise = elg.any().makePromise(of: Void.self)
        let (addresses, addressContinuation) = AsyncStream.makeStream(of: NIOHTTPServer.SocketAddress.self)

        let handler: HTTPServerClosureRequestHandler<
            NIOHTTPServer.RequestContext, NIOHTTPServer.Reader, NIOHTTPServer.ResponseSender
        > = HTTPServerClosureRequestHandler { request, requestContext, requestReader, responseSender in
            var requestReader = requestReader
            try await requestReader.read { _, _ in }
            firstChunkReadPromise.succeed()

            var requestFinished = false
            while !requestFinished {
                try await requestReader.read { if $1 != nil { requestFinished = true } }
            }
            var buffer = UniqueArray<UInt8>(copying: [1, 2])
            try await responseSender.sendAndFinish(.init(status: .ok), buffer: &buffer)
        }

        let serverService = ClosureService {
            try await AcceptingListener.withListener { channel in
                try await server.serve(acceptedChannel: channel, handler: handler)
            } body: { address in
                addressContinuation.yield(address)
                try await gracefulShutdown()
            }
        }

        try await confirmation { responseReceived in
            try await testGracefulShutdown { trigger in
                try await withThrowingTaskGroup { group in
                    let serviceGroup = ServiceGroup(services: [serverService], logger: self.serverLogger)
                    group.addTask { try await serviceGroup.run() }

                    var addressIterator = addresses.makeAsyncIterator()
                    let address = try #require(await addressIterator.next())

                    try await TestClientConnection.withConnectedRequestChannel(
                        configuration: clientConfiguration,
                        serverAddress: address
                    ) { inbound, outbound in
                        try await outbound.write(.testHead(method: .post, for: httpVersion))
                        try await outbound.write(.testBody)
                        try await firstChunkReadPromise.futureResult.get()

                        trigger.triggerGracefulShutdown()

                        try await outbound.write(.testBody)
                        try await outbound.write(.testEnd)

                        responseLoop: for try await response in inbound {
                            switch response {
                            case .head(let head):
                                #expect(head.status == .ok)
                            case .body:
                                ()
                            case .end:
                                break responseLoop
                            }
                        }
                        responseReceived()
                    }

                    try await group.waitForAll()
                }
            }
        }
    }
}

/// Accepts TCP connections with its own `ServerBootstrap`, standing in for a transport that `NIOHTTPServer` doesn't
/// bind itself, and hands each connection to `serveConnection`. Once `body` returns, the listener stops accepting
/// and waits for the connections it accepted to finish.
@available(anyAppleOS 27.0, *)
enum AcceptingListener {
    static func withListener(
        serveConnection: @escaping @Sendable (any Channel) async throws -> Void,
        body: (NIOHTTPServer.SocketAddress) async throws -> Void
    ) async throws {
        let listener = try await ServerBootstrap(group: MultiThreadedEventLoopGroup.singletonMultiThreadedEventLoopGroup)
            .childChannelOption(.autoRead, value: false)
            .bind(host: "127.0.0.1", port: 0) { channel in
                channel.eventLoop.makeSucceededFuture(channel)
            }
        let address = try NIOHTTPServer.SocketAddress(listener.channel.localAddress)

        try await withThrowingTaskGroup { group in
            group.addTask {
                try await listener.executeThenClose { inbound in
                    try await withThrowingDiscardingTaskGroup { connections in
                        for try await channel in inbound {
                            connections.addTask { try await serveConnection(channel) }
                        }
                    }
                }
            }

            // Stop accepting, then let the connections already accepted run to completion.
            try await body(address)
            listener.channel.close(promise: nil)
            try await group.waitForAll()
        }
    }
}
