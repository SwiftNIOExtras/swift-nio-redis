//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-nio-redis open source project
//
// Copyright (c) 2018-2021 ZeeZide GmbH. and the swift-nio-redis project authors
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
  case TransportError     (Swift.Error)
  case ProtocolError
  case UnexpectedNegativeCount
  case InternalInconsistency
}

public struct RESPParser {
  
  public typealias Yield = ( RESPValue ) -> Void
  
  private let allocator = ByteBufferAllocator()

  public mutating func feed(_ buffer: ByteBuffer, yield: Yield) throws {
    try buffer.withUnsafeReadableBytes { bp in
      let count = bp.count
      var i     = 0
      
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
              overflowBuffer?.writeInteger(c)
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
              overflowBuffer?.writeInteger(c)
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
                      pushArrayContext(expectedCount: countValue)
                    }
                    else {
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
              overflowBuffer?.writeInteger(c)
              let stillPending = pending - 1
              let avail = min(stillPending, (count - i))
              if avail > 0 {
                overflowBuffer?.writeBytes(bp[i..<(i + avail)])
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
              overflowBuffer?.writeInteger(c)
            }
        }
      }
    }

    assert(ctxIndex < 0 || !arrayContextBuffer[ctxIndex].isDone,
           "array context on stack which is done? \(arrayContextBuffer)")
  }
  
  
  // MARK: - Parsing

  private mutating func pushArrayContext(expectedCount: Int) {
    if ctxIndex == ctxCapacity {
      for _ in 0..<4 {
        arrayContextBuffer.append(ArrayParseContext(expectedCount: -44))
      }
      ctxCapacity = arrayContextBuffer.count
    }
    assert(ctxIndex < ctxCapacity, "index overflow")
    ctxIndex += 1
    arrayContextBuffer[ctxIndex].expectedCount = expectedCount
    arrayContextBuffer[ctxIndex].values.reserveCapacity(expectedCount)
  }

  private mutating func decoded(value: RESPValue, yield: Yield) {
    if ctxIndex < 0 {
      return yield(value)
    }
    
    let idx    = ctxIndex
    let isDone = arrayContextBuffer[idx].append(value: value)
    if isDone {
      let value = RESPValue.array(arrayContextBuffer[idx].values)
      arrayContextBuffer[idx].values = emptyValueArray
      arrayContextBuffer[idx].expectedCount = -1337
      
      ctxIndex -= 1
      if ctxIndex < 0 {
        yield(value)
      }
      else {
        decoded(value: value, yield: yield)
      }
    }
  }
  
  let emptyValueArray = ContiguousArray<RESPValue>()
  
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
  
  private var ctxIndex    : Int
  private var ctxCapacity : Int
  private var arrayContextBuffer : ContiguousArray<ArrayParseContext>

  init() {
    ctxIndex    = -1
    ctxCapacity = 2
    arrayContextBuffer = ContiguousArray<ArrayParseContext>()
    arrayContextBuffer.reserveCapacity(8)
    for _ in 0..<ctxCapacity {
      arrayContextBuffer.append(ArrayParseContext(expectedCount: -42))
    }
  }
  
  private struct ArrayParseContext {
    
    var values        = ContiguousArray<RESPValue>()
    var expectedCount : Int
    
    init(expectedCount: Int) {
      self.expectedCount = expectedCount
      values.reserveCapacity(expectedCount + 1)
    }
    
    var isDone : Bool { return expectedCount <= values.count }
    
    mutating func append(value v: RESPValue) -> Bool {
      assert(!isDone, "attempt to add to a context which is not TL or done")
      values.append(v)
      return isDone
    }
  }
  
  private var state          = ParserState.start
  private var hadMinus       = false
  
  private var countValue     = 0
  private var overflowSkipNL = false
  private var overflowBuffer : ByteBuffer?
}
