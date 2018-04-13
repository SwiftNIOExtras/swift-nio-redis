//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-nio-redis open source project
//
// Copyright (c) 2018 ZeeZide GmbH. and the swift-nio-redis project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIO

open class RedisChannelHandler : ChannelInboundHandler,
                                 ChannelOutboundHandler
{
  
  public typealias InboundErr  = RESPParserError
  
  public typealias InboundIn   = ByteBuffer
  public typealias InboundOut  = RESPValue

  public typealias OutboundIn  = RESPEncodable
  public typealias OutboundOut = ByteBuffer
  
  private final let nilStringBuffer = ConstantBuffers.nilStringBuffer
  private final let nilArrayBuffer  = ConstantBuffers.nilArrayBuffer
  
  public final var parser = RESPParser()
  
  public init() {}
  
  // MARK: - Channel Open/Close
  
  open func channelActive(ctx: ChannelHandlerContext) {
    ctx.fireChannelActive()
  }
  open func channelInactive(ctx: ChannelHandlerContext) {
    #if false // this doesn't gain us anything?
      switch parser.state {
        case .protocolError, .start: break // all good
        default:
          ctx.fireErrorCaught(InboundErr.ProtocolError)
      }
    #endif
    ctx.fireChannelInactive()
  }

  
  // MARK: - Reading

  public func channelRead(ctx: ChannelHandlerContext, data: NIOAny) {
    do {
      let buffer = self.unwrapInboundIn(data)
      try parser.feed(buffer) { respValue in
        self.channelRead(ctx: ctx, value: respValue)
      }
    }
    catch {
      ctx.fireErrorCaught(error)
      ctx.close(promise: nil)
      return
    }
  }
  
  open func channelRead(ctx: ChannelHandlerContext, value: RESPValue) {
    ctx.fireChannelRead(self.wrapInboundOut(value))
  }
  
  open func errorCaught(ctx: ChannelHandlerContext, error: Error) {
    ctx.fireErrorCaught(InboundErr.TransportError(error))
  }
  
  
  // MARK: - Writing

  public func write(ctx: ChannelHandlerContext, data: NIOAny,
                    promise: EventLoopPromise<Void>?)
  {
    let data : RESPEncodable = self.unwrapOutboundIn(data)
    write(ctx: ctx, value: data.toRESPValue(), promise: promise)
  }
  
  public final func write(ctx: ChannelHandlerContext, value: RESPValue,
                          promise: EventLoopPromise<Void>?)
  {
    var out : ByteBuffer
    switch value {
      case .simpleString(var s): // +
        out = ctx.channel.allocator.buffer(capacity: 1 + s.readableBytes + 3)
        out.write(integer : UInt8(43)) // +
        out.write(buffer  : &s)
        out.write(bytes   : eol)

      case .bulkString(.some(var s)): // $
        let count = s.readableBytes
        out = ctx.channel.allocator.buffer(capacity: 1 + 4 + 2 + count + 3)
        out.write(integer         : UInt8(36)) // $
        out.write(integerAsString : count)
        out.write(bytes           : eol)
        out.write(buffer          : &s)
        out.write(bytes           : eol)

      case .bulkString(.none): // $
        out = nilStringBuffer
      
      case .integer(let i): // :
        out = ctx.channel.allocator.buffer(capacity: 1 + 20 + 3)
        out.write(integer         : UInt8(58)) // :
        out.write(integerAsString : i)
        out.write(bytes           : eol)
      
      case .error(let error): // -
        out = ctx.channel.allocator.buffer(capacity: 256)
        encode(error: error, out: &out)
      
      case .array(.some(let array)): // *
        let count = array.count
        out = ctx.channel.allocator.buffer(capacity: 1 + 4 + 3 + count * 32)
        out.write(integer: UInt8(42)) // *
        out.write(integerAsString: array.count)
        out.write(bytes: eol)
        for item in array {
          encode(ctx: ctx, data: item, level: 1, out: &out)
        }
      
      case .array(.none): // *
        out = nilArrayBuffer
    }

    ctx.write(wrapOutboundOut(out), promise: promise)
  }

  @inline(__always)
  final func encode<S: ContiguousCollection>(simpleString bytes: S,
                                             out: inout ByteBuffer)
         where S.Element == UInt8
  {
    out.write(integer : UInt8(43)) // +
    out.write(bytes   : bytes)
    out.write(bytes   : eol)
  }
  
  @inline(__always)
  final func encode(simpleString bytes: ByteBuffer, out: inout ByteBuffer) {
    var s = bytes
    out.write(integer : UInt8(43)) // +
    out.write(buffer  : &s)
    out.write(bytes   : eol)
  }

  @inline(__always)
  final func encode(bulkString bytes: ByteBuffer?, out: inout ByteBuffer) {
    if var s = bytes {
      out.write(integer         : UInt8(36)) // $
      out.write(integerAsString : s.readableBytes)
      out.write(bytes           : eol)
      out.write(buffer          : &s)
      out.write(bytes           : eol)
    }
    else {
      out.write(bytes: nilString)
    }
  }

  @inline(__always)
  final func encode<S: ContiguousCollection>(bulkString bytes: S?,
                                             out: inout ByteBuffer)
         where S.Element == UInt8
  {
    if let s = bytes {
      out.write(integer         : UInt8(36)) // $
      out.write(integerAsString : Int(s.count))
      out.write(bytes           : eol)
      out.write(bytes           : s)
      out.write(bytes           : eol)
    }
    else {
      out.write(bytes: nilString)
    }
  }
  
  @inline(__always)
  final func encode(integer i: Int, out: inout ByteBuffer) {
    out.write(integer         : UInt8(58)) // :
    out.write(integerAsString : i)
    out.write(bytes           : eol)
  }
  
  @inline(__always)
  final func encode(error: RESPError, out: inout ByteBuffer) {
    out.write(integer : UInt8(45)) // -
    out.write(string  : error.code)
    out.write(integer : UInt8(32)) // ' '
    out.write(string  : error.message)
    out.write(bytes   : eol)
  }
  
  final func encode(ctx  : ChannelHandlerContext,
                    data : RESPValue,
                    out  : inout ByteBuffer)
  {
    encode(ctx: ctx, data: data, level: 0, out: &out)
  }

  final func encode(ctx   : ChannelHandlerContext,
                    data  : RESPValue,
                    level : Int,
                    out   : inout ByteBuffer)
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
          out.write(integer: UInt8(42)) // *
          out.write(integerAsString: array.count)
          out.write(bytes: eol)
          for item in array {
            encode(ctx: ctx, data: item, level: level + 1, out: &out)
          }
        }
        else {
          out.write(bytes: nilArray)
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
    var bb = alloc.buffer(capacity: 6)
    bb.write(bytes: nilString)
    return bb
  }()
  
  static let nilArrayBuffer : ByteBuffer = {
    let alloc = ByteBufferAllocator()
    var bb = alloc.buffer(capacity: 6)
    bb.write(bytes: nilArray)
    return bb
  }()
}

fileprivate extension BinaryInteger {
  
  var numberOfDecimalDigits : Int {
    @inline(__always) get {
      var value = self
      var count = 0
      
      repeat {
        value /= 10
        count += 1
      }
      while value != 0
      
      return count
    }
  }
}

extension ByteBuffer {
  
  @discardableResult
  public mutating func write<T: SignedInteger>(integerAsString integer: T,
                                               as: T.Type = T.self) -> Int
  {
    let bytesWritten = set(integerAsString: integer, at: self.writerIndex)
    moveWriterIndex(forwardBy: bytesWritten)
    return Int(bytesWritten)
  }

  @discardableResult
  public mutating func set<T: SignedInteger>(integerAsString integer: T,
                                             at index: Int,
                                             as: T.Type = T.self) -> Int
  {
    let charCount = integer.numberOfDecimalDigits + (integer < 0 ? 1 : 0)
    let avail     = capacity - index
    
    if avail < charCount {
      changeCapacity(to: capacity + (charCount - avail))
    }

    self.withVeryUnsafeBytes { rbpp in
      let mrbpp  = UnsafeMutableRawBufferPointer(mutating: rbpp)
      let base   = mrbpp.baseAddress!.assumingMemoryBound(to: UInt8.self)
                        .advanced(by: index)
      var cursor = base.advanced(by: charCount)
      
      let c0 : T = 48
      var negativeAbsoluteValue = integer < 0 ? integer : -integer
      repeat {
        cursor -= 1
        cursor.pointee = UInt8(c0 - (negativeAbsoluteValue % 10))
        negativeAbsoluteValue /= 10;
      }
      while negativeAbsoluteValue != 0
      
      if integer < 0 {
        cursor -= 1
        cursor.pointee = 45 // -
      }

    }
    
    return charCount
  }
}
