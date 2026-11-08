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

import NIOCore
import NIOHTTP1

/// Closes an HTTP/1.x connection after responding to a request that is not persistent.
///
/// RFC 9112 section 9.6 requires a server that receives the `close` connection option to close the connection after
/// its response, and an HTTP/1.0 request is only persistent with `keep-alive`. That information lives in the
/// `Connection` header, which the HTTP/1 to HTTP types conversion removes as connection-specific, so this handler sits
/// between `HTTPServerPipelineHandler` and `HTTP1ToHTTPServerCodec`. The pipeline handler delivers one request at a
/// time, so the current request's persistence applies to the next response.
final class HTTP1ConnectionCloseHandler: ChannelDuplexHandler, RemovableChannelHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias InboundOut = HTTPServerRequestPart
    typealias OutboundIn = HTTPServerResponsePart
    typealias OutboundOut = HTTPServerResponsePart

    private var closeAfterResponse = false

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if case .head(let head) = Self.unwrapInboundIn(data) {
            self.closeAfterResponse = !head.isKeepAlive
        }
        context.fireChannelRead(data)
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        guard self.closeAfterResponse else {
            context.write(data, promise: promise)
            return
        }

        switch Self.unwrapOutboundIn(data) {
        case .head(var head) where head.status.code >= 200:
            // Tell the client the connection is ending; informational responses don't commit to that.
            head.headers.replaceOrAdd(name: "connection", value: "close")
            context.write(Self.wrapOutboundOut(.head(head)), promise: promise)

        case .end:
            self.closeAfterResponse = false
            let loopBoundContext = context.loopBound
            context.write(data).flatMap {
                loopBoundContext.value.close()
            }.cascade(to: promise)

        case .head, .body:
            context.write(data, promise: promise)
        }
    }
}
