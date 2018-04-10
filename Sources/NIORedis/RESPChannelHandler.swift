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

public enum RedisInboundError : Error {
  case UnexpectedStartByte(char: UInt8, buffer: ByteBuffer)
  case UnexpectedEndByte  (char: UInt8, buffer: ByteBuffer)
  case TransportError(Swift.Error)
  case ProtocolError
  case UnexpectedNegativeCount
  case InternalInconsistency
}

final class RedisChannelHandler : ChannelInboundHandler,
                                  ChannelOutboundHandler
{
  
  typealias InboundErr  = RedisInboundError
  
  typealias InboundIn   = ByteBuffer
  typealias InboundOut  = RESPValue

  typealias OutboundIn  = RESPEncodable
  typealias OutboundOut = ByteBuffer
  
  let nilStringBuffer = ConstantBuffers.nilStringBuffer
  let nilArrayBuffer  = ConstantBuffers.nilArrayBuffer
  
  
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

  @inline(__always)
  private func decoded(value: RESPValue, in ctx: ChannelHandlerContext) {
    if let arrayContext = arrayContext {
      _ = arrayContext.append(value: value)
      
      if arrayContext.isDone {
        let v = RESPValue.array(arrayContext.values)
        
        if let parent = arrayContext.parent {
          self.arrayContext = parent
        }
        else {
          self.arrayContext = nil
          if cachedParseContext == nil {
            cachedParseContext = arrayContext
            arrayContext.values = ContiguousArray()
            arrayContext.values.reserveCapacity(16)
          }
        }
        decoded(value: v, in: ctx)
      }
    }
    else {
      ctx.fireChannelRead(self.wrapInboundOut(value))
    }
  }

  public func channelRead(ctx: ChannelHandlerContext, data: NIOAny) {
    do {
      let buffer = self.unwrapInboundIn(data)
      
      try buffer.withUnsafeReadableBytes { bp in
        let count = bp.count
        var i     = 0
        
        @inline(__always)
        func doSkipNL() {
          if i >= count {
            overflowSkipNL = true
          }
          else {
            if bp[i] == 10 /* LF */ { i += 1 }
            overflowSkipNL = false
          }
        }
        
        if overflowSkipNL { doSkipNL() }
        
        while i < count {
          let c = bp[i]; i += 1
          
          switch state {
            
            case .protocolError:
              throw InboundErr.ProtocolError
            
            case .start:
              switch c {
                case 43 /* + */: state = .simpleString
                case 45 /* - */: state = .error
                case 58 /* : */: state = .integer
                case 36 /* $ */: state = .bulkStringLen
                case 42 /* * */: state = .arrayCount
                default:         state = .telnet
              }
              countValue = 0
              if state == .telnet || state == .simpleString || state == .error {
                overflowBuffer = ctx.channel.allocator.buffer(capacity: 80)
                overflowBuffer?.write(integer: c)
              }
              else {
                overflowBuffer = nil
              }
            
            case .telnet:
              assert(overflowBuffer != nil, "missing overflow buffer")
              if c == 13 || c == 10 {
                if c == 13 { doSkipNL() }
                let count = overflowBuffer?.readableBytes ?? 0
                if count > 0 {
                  // just a quick hack for telnet mode
                  guard let s = overflowBuffer?.readString(length: count) else {
                    throw InboundErr.ProtocolError
                  }
                  let vals = s.components(separatedBy: " ")
                              .lazy.map { RESPValue(bulkString: $0) }
                  decoded(value: .array(ContiguousArray(vals)), in: ctx)
                }
              }
              else {
                overflowBuffer?.write(integer: c)
              }
            
            case .arrayCount, .bulkStringLen, .integer:
              let c0 : UInt8 = 48, c9 : UInt8 = 57, cMinus : UInt8 = 45
              if c >= c0 && c <= c9 {
                let digit = c - c0
                countValue = (countValue * 10) + Int(digit)
              }
              else if !hadMinus && c == cMinus && countValue == 0 {
                hadMinus = true
              }
              else if c == 13 || c == 10 {
                let doNegate = hadMinus
                hadMinus = false
                if c == 13 { doSkipNL() }

                switch state {
                  
                  case .arrayCount:
                    if doNegate {
                      guard countValue == 1 else {
                        self.state = .protocolError
                        throw InboundErr.UnexpectedNegativeCount
                      }
                      decoded(value: .array(nil), in: ctx)
                    }
                    else {
                      if countValue > 0 {
                        arrayContext =
                          makeArrayParseContext(arrayContext, countValue)
                      }
                      else {
                        // push an empty array
                        decoded(value: .array([]), in: ctx)
                      }
                    }
                    state = .start
                  
                  case .bulkStringLen:
                    if doNegate {
                      state = .start
                      decoded(value: .bulkString(nil), in: ctx)
                    }
                    else {
                      if (count - i) >= (countValue + 2) { // include CRLF
                        let value = buffer.getSlice(at: buffer.readerIndex + i,
                                                    length: countValue)!
                        i += countValue
                        decoded(value: .bulkString(value), in: ctx)
                        
                        let ec = bp[i]
                        guard ec == 13 || ec == 10 else {
                          self.state = .protocolError
                          throw InboundErr.UnexpectedStartByte(char: bp[i],
                                                               buffer: buffer)
                        }
                        i += 1
                        if ec == 13 { doSkipNL() }
                        
                        state = .start
                      }
                      else {
                        state = .bulkStringValue
                        overflowBuffer =
                          ctx.channel.allocator.buffer(capacity: countValue + 1)
                      }
                    }
                  
                  case .integer:
                    let value = doNegate ? -countValue : countValue
                    countValue = 0 // reset
                    
                    decoded(value: .integer(value), in: ctx)
                    state = .start
                  
                  default:
                    assertionFailure("unexpected enum case \(state)")
                    state = .protocolError
                    throw InboundErr.InternalInconsistency
                }
              }
              else {
                self.state = .protocolError
                throw InboundErr.UnexpectedStartByte(char: c, buffer: buffer)
              }
            
            case .bulkStringValue:
              let pending = countValue - (overflowBuffer?.readableBytes ?? 0)
              
              if pending > 0 {
                overflowBuffer?.write(integer: c)
                let stillPending = pending - 1
                let avail = min(stillPending, (count - i))
                if avail > 0 {
                  overflowBuffer?.write(bytes: bp[i..<(i + avail)])
                  i += avail
                }
              }
              else if pending == 0 && (c == 13 || c == 10) {
                if c == 13 { doSkipNL() }
                
                let value = overflowBuffer
                overflowBuffer = nil
                
                decoded(value: .bulkString(value), in: ctx)
                state = .start
              }
              else {
                self.state = .protocolError
                throw InboundErr.UnexpectedEndByte(char: c, buffer: buffer)
              }
            
            case .simpleString, .error:
              assert(overflowBuffer != nil, "missing overflow buffer")
              if c == 13 || c == 10 {
                if c == 13 { doSkipNL() }
                
                if state == .simpleString {
                  if let v = overflowBuffer {
                    decoded(value: .simpleString(v), in: ctx)
                  }
                }
                else {
                  // TODO: make nice :-)
                  let avail = overflowBuffer?.readableBytes ?? 0
                  let value = overflowBuffer?.readBytes(length: avail) ?? []
                  let pair = value.split(separator: 32, maxSplits: 1)
                  let code = pair.count > 0 ? String.decode(utf8: pair[0]) ?? "" :""
                  let msg  = pair.count > 1 ? String.decode(utf8: pair[1]) ?? "" :""
                  let error = RESPError(code: code, message: msg)
                  decoded(value: .error(error), in: ctx)
                }
                overflowBuffer = nil
                
                state = .start
              }
              else {
                overflowBuffer?.write(integer: c)
              }
          }
        }
      }
    }
    catch {
      ctx.fireErrorCaught(error)
      ctx.close(promise: nil)
      return
    }
    

    // finish up.
    
    if let arrayContext = arrayContext {
      if arrayContext.isDone {
        let values = arrayContext.values
        self.arrayContext = nil
        ctx.fireChannelRead(self.wrapInboundOut(.array(values)))
      }
      else {
        // we leave the context around
      }
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
