//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-nio-redis open source project
//
// Copyright (c) 2018-2020 ZeeZide GmbH. and the swift-nio-redis project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIO

open class RESPChannelHandler : ChannelDuplexHandler {
  
  public  typealias InboundErr      = RESPParserError
  
  public  typealias InboundIn       = ByteBuffer
  public  typealias InboundOut      = RESPValue

  public  typealias OutboundIn      = RESPEncodable
  public  typealias OutboundOut     = ByteBuffer
  
  private final let nilStringBuffer = ConstantBuffers.nilStringBuffer
  private final let nilArrayBuffer  = ConstantBuffers.nilArrayBuffer
  
  public  final var parser          = RESPParser()
  
  public init() {}
  
  // MARK: - Channel Open/Close
  
  open func channelActive(context: ChannelHandlerContext) {
    context.fireChannelActive()
  }
  open func channelInactive(context: ChannelHandlerContext) {
    #if false // this doesn't gain us anything?
      switch parser.state {
        case .protocolError, .start: break // all good
        default:
          context.fireErrorCaught(InboundErr.ProtocolError)
      }
    #endif
    context.fireChannelInactive()
  }

  
  // MARK: - Reading

  public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
    do {
      let buffer = self.unwrapInboundIn(data)
      try parser.feed(buffer) { respValue in
        self.channelRead(context: context, value: respValue)
      }
    }
    catch {
      context.fireErrorCaught(error)
      context.close(promise: nil)
      return
    }
  }
  
  open func channelRead(context: ChannelHandlerContext, value: RESPValue) {
    context.fireChannelRead(self.wrapInboundOut(value))
  }
  
  open func errorCaught(context: ChannelHandlerContext, error: Error) {
    context.fireErrorCaught(InboundErr.TransportError(error))
  }
  
  
  // MARK: - Writing

  public func write(context: ChannelHandlerContext, data: NIOAny,
                    promise: EventLoopPromise<Void>?)
  {
    let data : RESPEncodable = self.unwrapOutboundIn(data)
    write(context: context, value: data.toRESPValue(), promise: promise)
  }
  
  public final func write(context: ChannelHandlerContext, value: RESPValue,
                          promise: EventLoopPromise<Void>?)
  {
    var out : ByteBuffer
    switch value {
      case .simpleString(var s): // +
        out = context.channel.allocator
                .buffer(capacity: 1 + s.readableBytes + 3)
        out.writeInteger(UInt8(43)) // +
        out.writeBuffer(&s)
        out.writeBytes(eol)

      case .bulkString(.some(var s)): // $
        let count = s.readableBytes
        out = context.channel.allocator.buffer(capacity: 1 + 4 + 2 + count + 3)
        out.writeInteger(UInt8(36)) // $
        out.write(integerAsString : count)
        out.writeBytes(eol)
        out.writeBuffer(&s)
        out.writeBytes(eol)

      case .bulkString(.none): // $
        out = nilStringBuffer
      
      case .integer(let i): // :
        out = context.channel.allocator.buffer(capacity: 1 + 20 + 3)
        out.writeInteger(UInt8(58)) // :
        out.write(integerAsString: i)
        out.writeBytes(eol)
      
      case .error(let error): // -
        out = context.channel.allocator.buffer(capacity: 256)
        encode(error: error, out: &out)
      
      case .array(.some(let array)): // *
        let count = array.count
        out = context.channel.allocator.buffer(capacity: 1 + 4 + 3 + count * 32)
        out.writeInteger(UInt8(42)) // *
        out.write(integerAsString: array.count)
        out.writeBytes(eol)
        for item in array {
          encode(context: context, data: item, level: 1, out: &out)
        }
      
      case .array(.none): // *
        out = nilArrayBuffer
    }

    context.write(wrapOutboundOut(out), promise: promise)
  }

  @inline(__always)
  final func encode<S: Collection>(simpleString bytes: S,
                                   out: inout ByteBuffer)
        where S.Element == UInt8
  {
    out.writeInteger(UInt8(43)) // +
    out.writeBytes(bytes)
    out.writeBytes(eol)
  }
  
  @inline(__always)
  final func encode(simpleString bytes: ByteBuffer, out: inout ByteBuffer) {
    var s = bytes
    out.writeInteger(UInt8(43)) // +
    out.writeBuffer(&s)
    out.writeBytes(eol)
  }

  @inline(__always)
  final func encode(bulkString bytes: ByteBuffer?, out: inout ByteBuffer) {
    if var s = bytes {
      out.writeInteger(UInt8(36)) // $
      out.write(integerAsString : s.readableBytes)
      out.writeBytes(eol)
      out.writeBuffer(&s)
      out.writeBytes(eol)
    }
    else {
      out.writeBytes(nilString)
    }
  }

  @inline(__always)
  final func encode<S: Collection>(bulkString bytes: S?,
                                   out: inout ByteBuffer)
         where S.Element == UInt8
  {
    if let s = bytes {
      out.writeInteger(UInt8(36)) // $
      out.write(integerAsString : Int(s.count))
      out.writeBytes(eol)
      out.writeBytes(s)
      out.writeBytes(eol)
    }
    else {
      out.writeBytes(nilString)
    }
  }
  
  @inline(__always)
  final func encode(integer i: Int, out: inout ByteBuffer) {
    out.writeInteger(UInt8(58)) // :
    out.write(integerAsString : i)
    out.writeBytes(eol)
  }
  
  @inline(__always)
  final func encode(error: RESPError, out: inout ByteBuffer) {
    out.writeInteger(UInt8(45)) // -
    out.writeString(error.code)
    out.writeInteger(UInt8(32)) // ' '
    out.writeString(error.message)
    out.writeBytes(eol)
  }
  
  final func encode(context : ChannelHandlerContext,
                    data    : RESPValue,
                    out     : inout ByteBuffer)
  {
    encode(context: context, data: data, level: 0, out: &out)
  }

  final func encode(context : ChannelHandlerContext,
                    data    : RESPValue,
                    level   : Int,
                    out     : inout ByteBuffer)
  {
    // FIXME: Creating a String for an Int is expensive, there is something
    //        something better in the HTTP-API async imp.
    switch data {
      case .simpleString(let s): // +
        encode(simpleString: s, out: &out)

      case .bulkString(let s): // $
        encode(bulkString: s, out: &out)
      
      case .integer(let i): // :
        encode(integer: i, out: &out)

      case .error(let error): // -
        encode(error: error, out: &out)

      case .array(let array): // *
        if let array = array {
          out.writeInteger(UInt8(42)) // *
          out.write(integerAsString: array.count)
          out.writeBytes(eol)
          for item in array {
            encode(context: context, data: item, level: level + 1, out: &out)
          }
        }
        else {
          out.writeBytes(nilArray)
        }
    }
  }
}

private let eol       : ContiguousArray<UInt8> = [ 13, 10 ] // \r\n
private let nilString : ContiguousArray<UInt8> = [ 36, 45, 49, 13, 10 ] // $-1\r\n
private let nilArray  : ContiguousArray<UInt8> = [ 42, 45, 49, 13, 10 ] // *-1\r\n

fileprivate enum ConstantBuffers {
  
  static let nilStringBuffer : ByteBuffer = {
    let alloc = ByteBufferAllocator()
    var bb    = alloc.buffer(capacity: 6)
    bb.writeBytes(nilString)
    return bb
  }()
  
  static let nilArrayBuffer : ByteBuffer = {
    let alloc = ByteBufferAllocator()
    var bb    = alloc.buffer(capacity: 6)
    bb.writeBytes(nilArray)
    return bb
  }()
}

