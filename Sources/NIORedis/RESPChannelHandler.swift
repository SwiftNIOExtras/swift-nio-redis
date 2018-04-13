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

final class RedisChannelHandler : ChannelInboundHandler,
                                  ChannelOutboundHandler
{
  
  typealias InboundErr  = RESPParserError
  
  typealias InboundIn   = ByteBuffer
  typealias InboundOut  = RESPValue

  typealias OutboundIn  = RESPEncodable
  typealias OutboundOut = ByteBuffer
  
  let nilStringBuffer = ConstantBuffers.nilStringBuffer
  let nilArrayBuffer  = ConstantBuffers.nilArrayBuffer
  
  private final var parser = RESPParser()
  
  // MARK: - Channel Open/Close
  
  func channelActive(ctx: ChannelHandlerContext) {
    ctx.fireChannelActive()
  }
  func channelInactive(ctx: ChannelHandlerContext) {
    switch state {
      case .protocolError, .start: break // all good
      default:
        ctx.fireErrorCaught(InboundErr.ProtocolError)
    }
    
    overflowBuffer = nil
    
    ctx.fireChannelInactive()
  }
  
  
  // MARK: - Reading

  public func channelRead(ctx: ChannelHandlerContext, data: NIOAny) {
    do {
      let buffer = self.unwrapInboundIn(data)
      try parser.feed(buffer) { respValue in
        ctx.fireChannelRead(self.wrapInboundOut(respValue))
      }
    }
    catch {
      ctx.fireErrorCaught(error)
      ctx.close(promise: nil)
      return
    }
  }

  public func errorCaught(ctx: ChannelHandlerContext, error: Error) {
    ctx.fireErrorCaught(InboundErr.TransportError(error))
  }
  
  
  // MARK: - Parsing

  private enum ParserState {
    case protocolError
    case start
    case error
    case integer
    case bulkStringLen
    case bulkStringValue
    case simpleString
    case arrayCount
    case telnet
  }
  
  @inline(__always)
  private func makeArrayParseContext(_ parent: ArrayParseContext? = nil,
                                     _ count: Int) -> ArrayParseContext
  {
    if parent == nil && cachedParseContext != nil {
      let ctx = cachedParseContext!
      cachedParseContext = nil
      ctx.count = count
      ctx.values.reserveCapacity(count)
      return ctx
    }
    else {
      return ArrayParseContext(parent, count)
    }
  }
  private final var cachedParseContext : ArrayParseContext? = nil
  
  final private class ArrayParseContext {
    
    let parent : ArrayParseContext?
    var values = ContiguousArray<RESPValue>()
    var count  : Int
    
    init(_ parent: ArrayParseContext?, _ count: Int) {
      self.parent = parent
      self.count  = count
    }
    
    var isDone   : Bool { return count <= values.count }
    var isNested : Bool { return parent != nil }
    
    @inline(__always)
    func append(value v: InboundOut) -> Bool {
      assert(!isNested || !isDone,
             "attempt to add to a context which is not TL or done")
      values.append(v)
      return isDone
    }
  }
  
  private var state          = ParserState.start
  private var overflowSkipNL = false
  private var hadMinus       = false
  
  private var countValue     = 0
  private var overflowBuffer : ByteBuffer?
  
  private var arrayContext   : ArrayParseContext?
  
  
  // MARK: - Writing
  
  func write(ctx: ChannelHandlerContext, data: NIOAny,
             promise: EventLoopPromise<Void>?)
  {
    let data  : RESPEncodable = self.unwrapOutboundIn(data)
    let value = data.toRESPValue()
    
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
        out.write(string: String(array.count, radix: 10))
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
  func encode<S: ContiguousCollection>(simpleString bytes: S,
                                       out: inout ByteBuffer)
         where S.Element == UInt8
  {
    out.write(integer : UInt8(43)) // +
    out.write(bytes   : bytes)
    out.write(bytes   : eol)
  }
  
  @inline(__always)
  func encode(simpleString bytes: ByteBuffer, out: inout ByteBuffer) {
    var s = bytes
    out.write(integer : UInt8(43)) // +
    out.write(buffer  : &s)
    out.write(bytes   : eol)
  }

  @inline(__always)
  func encode(bulkString bytes: ByteBuffer?, out: inout ByteBuffer) {
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
  func encode<S: ContiguousCollection>(bulkString bytes: S?,
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
  func encode(integer i: Int, out: inout ByteBuffer) {
    out.write(integer         : UInt8(58)) // :
    out.write(integerAsString : i)
    out.write(bytes           : eol)
  }
  
  @inline(__always)
  func encode(error: RESPError, out: inout ByteBuffer) {
    out.write(integer : UInt8(45)) // -
    out.write(string  : error.code)
    out.write(integer : UInt8(32)) // ' '
    out.write(string  : error.message)
    out.write(bytes   : eol)
  }
  
  func encode(ctx  : ChannelHandlerContext,
              data : RESPValue,
              out  : inout ByteBuffer)
  {
    encode(ctx: ctx, data: data, level: 0, out: &out)
  }

  func encode(ctx   : ChannelHandlerContext,
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
          out.write(string: String(array.count, radix: 10))
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

extension ByteBuffer {
  
  @discardableResult
  public mutating func write<T: FixedWidthInteger>(integerAsString integer: T,
                                                   as: T.Type = T.self) -> Int
  {
    let bytesWritten = set(integerAsString: integer, at: self.writerIndex)
    moveWriterIndex(forwardBy: bytesWritten)
    return Int(bytesWritten)
  }

  @discardableResult
  public mutating func set<T: FixedWidthInteger>(integerAsString integer: T,
                                                 at index: Int,
                                                 as: T.Type = T.self) -> Int
  {
    var value = integer
    
    #if true // slow, fixme
      let len = String(value, radix: 10)
      return set(string: len, at: index)! // TBD: why is this returning an Opt?
    #else
      return Swift.withUnsafeBytes(of: &value) { ptr in
        // TODO: do the itoa thing to make it fast
      }
    #endif
  }
}
