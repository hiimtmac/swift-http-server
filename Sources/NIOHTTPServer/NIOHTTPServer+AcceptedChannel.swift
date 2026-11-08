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

public import NIOCore
import ServiceLifecycle

@available(anyAppleOS 27.0, *)
extension NIOHTTPServer {
    /// Serves HTTP on a single connection that the caller has already accepted, using a request handler.
    ///
    /// This is the request-level counterpart to ``serve(acceptedChannel:connectionHandler:)``, which describes the
    /// requirements on `channel` and how the connection is served. A request handler reports failure by throwing, as
    /// described for ``serve(handler:)``.
    ///
    /// - Parameters:
    ///   - channel: An active, connected stream channel. The server closes it before returning.
    ///   - handler: The handler invoked for each request received on the connection.
    /// - Throws: If the configuration supports only HTTP/3, which isn't served over a stream channel, or if the
    ///   connection's handlers couldn't be installed.
    public func serve<Handler: HTTPServerRequestHandler>(
        acceptedChannel channel: any Channel,
        handler: Handler
    ) async throws
    where
        Handler.RequestContext == RequestContext,
        Handler.Reader == Reader,
        Handler.ResponseSender == ResponseSender
    {
        try await self.serve(
            acceptedChannel: channel,
            connectionHandler: NIOHTTPServerDefaultConnectionHandler(handler: handler)
        )
    }

    /// Serves HTTP on a single connection that the caller has already accepted.
    ///
    /// Use this entry point when connections arrive over a transport that `NIOHTTPServer` does not bind itself, such
    /// as a userspace network stack or an inherited socket. The channel must be active, and its pipeline must not yet
    /// carry HTTP handlers. The server installs the handlers it installs on the connections it accepts itself: TLS
    /// with ALPN negotiation of HTTP/1.1 or HTTP/2 when ``NIOHTTPServerConfiguration/transportSecurity`` is (m)TLS,
    /// otherwise plaintext HTTP/1.1, along with the configured timeouts and backpressure.
    ///
    /// Accept the channel with `ChannelOptions.autoRead` disabled. Bytes read before the server's handlers are
    /// installed would reach the end of the pipeline and be dropped; the server enables `autoRead` once its handlers
    /// are in place.
    ///
    /// The method returns once the connection has closed. It does not bind
    /// ``NIOHTTPServerConfiguration/bindTargets`` or enforce ``NIOHTTPServerConfiguration/maxConnections``: the caller
    /// owns accepting and limiting connections, and calls this method concurrently for each one. A server used this
    /// way has no listening address of its own, so ``listeningAddresses`` throws after the first call. Don't combine
    /// this method with ``serve(handler:)`` or ``serve(connectionHandler:)-(Handler)`` on the same server.
    ///
    /// Graceful shutdown triggered through `ServiceLifecycle` quiesces the connection in the same way as for
    /// connections accepted by ``serve(handler:)``: HTTP/1.1 closes once the in-flight response completes, and HTTP/2
    /// sends `GOAWAY` and lets open streams finish.
    ///
    /// Errors thrown by `connectionHandler`, and failures of an individual exchange, are handled as they are by
    /// ``serve(connectionHandler:)-(Handler)``: they're logged and close the connection rather than propagate.
    ///
    /// - Parameters:
    ///   - channel: An active, connected stream channel. The server closes it before returning.
    ///   - connectionHandler: The connection handler invoked once the connection's HTTP version is known.
    /// - Throws: If the configuration supports only HTTP/3, which isn't served over a stream channel, or if the
    ///   connection's handlers couldn't be installed.
    public func serve<Handler: NIOHTTPServerConnectionHandler>(
        acceptedChannel channel: any Channel,
        connectionHandler: Handler
    ) async throws {
        // There is no listening address to publish; fail any waiter instead of leaking the promise.
        self.finishListeningAddressPromise()

        let secureUpgrade: ListenerConfiguration.SecureUpgrade?
        switch self.configuration.makeListenerConfiguration() {
        case .plaintextHTTP1_1:
            secureUpgrade = nil
        case .secureUpgrade(let configuration):
            secureUpgrade = configuration
        #if HTTP3
        case .http3:
            channel.close(promise: nil)
            throw NIOHTTPServerConfigurationError.acceptedChannelRequiresHTTP1OrHTTP2
        case .secureUpgradeAndHTTP3(let configuration, _):
            secureUpgrade = configuration
        #endif
        }

        if Task.isShuttingDownGracefully {
            try await channel.close()
            return
        }

        try await withGracefulShutdownHandler {
            if let secureUpgrade {
                let negotiation = try await self.setUpAcceptedChannel(channel) {
                    self.setupSecureUpgradeConnection(channel: channel, configuration: secureUpgrade)
                }
                await self.dispatchSecureConnection(upgradeResult: negotiation, connectionHandler: connectionHandler)
            } else {
                let requestChannel = try await self.setUpAcceptedChannel(channel) {
                    self.setupHTTP1_1Connection(channel: channel, isSecure: false)
                }
                await self.dispatchPlaintextHTTP1_1Connection(
                    requestChannel: requestChannel.channel,
                    clientClosed: requestChannel.clientClosed,
                    connectionHandler: connectionHandler
                )
            }
        } onGracefulShutdown: {
            channel.pipeline.fireUserInboundEventTriggered(ChannelShouldQuiesceEvent())
        }
    }

    /// Convenience overload of ``serve(acceptedChannel:connectionHandler:)-(_,Handler)`` accepting a closure instead of
    /// an ``NIOHTTPServerConnectionHandler`` conformance.
    ///
    /// - Parameters:
    ///   - channel: An active, connected stream channel. The server closes it before returning.
    ///   - connectionHandler: The closure invoked once the connection's HTTP version is known.
    /// - Throws: If the configuration supports only HTTP/3, which isn't served over a stream channel, or if the
    ///   connection's handlers couldn't be installed.
    public func serve(
        acceptedChannel channel: any Channel,
        connectionHandler:
            @Sendable @escaping (
                _ connection: consuming sending Connection,
                _ context: ConnectionContext
            ) async throws -> Void
    ) async throws {
        try await self.serve(
            acceptedChannel: channel,
            connectionHandler: NIOHTTPServerClosureConnectionHandler(body: connectionHandler)
        )
    }

    /// Installs the connection's handlers on its event loop and starts reading, closing the channel if that fails.
    private func setUpAcceptedChannel<Result: Sendable>(
        _ channel: any Channel,
        _ setup: @escaping @Sendable () -> EventLoopFuture<Result>
    ) async throws -> Result {
        do {
            return try await channel.eventLoop.flatSubmit {
                setup().flatMap { result in
                    channel.setOption(ChannelOptions.autoRead, value: true).map { result }
                }
            }.get()
        } catch {
            channel.close(promise: nil)
            throw error
        }
    }
}
