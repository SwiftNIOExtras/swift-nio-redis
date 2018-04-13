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

import struct NIO.ByteBuffer
import struct NIO.ByteBufferAllocator

public enum RESPParserError : Error {
  case UnexpectedStartByte(char: UInt8, buffer: ByteBuffer)
  case UnexpectedEndByte  (char: UInt8, buffer: ByteBuffer)
  case TransportError(Swift.Error)
  case ProtocolError
  case UnexpectedNegativeCount
  case InternalInconsistency
}

public struct RESPParser {
  
  public typealias Yield = ( RESPValue ) -> Void
  
  private let allocator = ByteBufferAllocator()

  @inline(__always)
  private mutating func decoded(value: RESPValue, yield: Yield) {
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
        decoded(value: v, yield: yield)
      }
    }
    else {
      yield(value)
    }
  }

  public mutating func feed(_ buffer: ByteBuffer, yield: Yield) throws {
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
            throw RESPParserError.ProtocolError
          
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
              overflowBuffer = allocator.buffer(capacity: 80)
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
                  throw RESPParserError.ProtocolError
                }
                let vals = s.components(separatedBy: " ")
                            .lazy.map { RESPValue(bulkString: $0) }
                decoded(value: .array(ContiguousArray(vals)), yield: yield)
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
                      throw RESPParserError.UnexpectedNegativeCount
                    }
                    decoded(value: .array(nil), yield: yield)
                  }
                  else {
                    if countValue > 0 {
                      arrayContext =
                        makeArrayParseContext(arrayContext, countValue)
                    }
                    else {
                      // push an empty array
                      decoded(value: .array([]), yield: yield)
                    }
                  }
                  state = .start
                
                case .bulkStringLen:
                  if doNegate {
                    state = .start
                    decoded(value: .bulkString(nil), yield: yield)
                  }
                  else {
                    if (count - i) >= (countValue + 2) { // include CRLF
                      let value = buffer.getSlice(at: buffer.readerIndex + i,
                                                  length: countValue)!
                      i += countValue
                      decoded(value: .bulkString(value), yield: yield)
                      
                      let ec = bp[i]
                      guard ec == 13 || ec == 10 else {
                        self.state = .protocolError
                        throw RESPParserError.UnexpectedStartByte(char: bp[i],
                                                             buffer: buffer)
                      }
                      i += 1
                      if ec == 13 { doSkipNL() }
                      
                      state = .start
                    }
                    else {
                      state = .bulkStringValue
                      overflowBuffer = allocator.buffer(capacity:countValue + 1)
                    }
                  }
                
                case .integer:
                  let value = doNegate ? -countValue : countValue
                  countValue = 0 // reset
                  
                  decoded(value: .integer(value), yield: yield)
                  state = .start
                
                default:
                  assertionFailure("unexpected enum case \(state)")
                  state = .protocolError
                  throw RESPParserError.InternalInconsistency
              }
            }
            else {
              self.state = .protocolError
              throw RESPParserError.UnexpectedStartByte(char: c, buffer: buffer)
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
              
              decoded(value: .bulkString(value), yield: yield)
              state = .start
            }
            else {
              self.state = .protocolError
              throw RESPParserError.UnexpectedEndByte(char: c, buffer: buffer)
            }
          
          case .simpleString, .error:
            assert(overflowBuffer != nil, "missing overflow buffer")
            if c == 13 || c == 10 {
              if c == 13 { doSkipNL() }
              
              if state == .simpleString {
                if let v = overflowBuffer {
                  decoded(value: .simpleString(v), yield: yield)
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
                decoded(value: .error(error), yield: yield)
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

    // finish up.
    // TBD: I think this is not necessary anymore
    
    if let arrayContext = arrayContext {
      if arrayContext.isDone {
        let values = arrayContext.values
        self.arrayContext = nil
        yield(.array(values))
      }
      else {
        // we leave the context around
      }
    }
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
  private mutating func makeArrayParseContext(_ parent: ArrayParseContext? = nil,
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
  private var cachedParseContext : ArrayParseContext? = nil
  
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
    func append(value v: RESPValue) -> Bool {
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

}
