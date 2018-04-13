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
import Foundation

public enum RESPValue {
  case simpleString(ByteBuffer)
  case bulkString  (ByteBuffer?)
  case integer     (Int)
  case array       (ContiguousArray<RESPValue>?)
  case error       (RESPError)
}

public struct RESPError : Error {
  
  public init(code: String = "ERR", message: String = "Generic Error") {
    _storage = _Storage(code: code, message: message)
  }
  
  public var code    : String { return _storage.code    }
  public var message : String { return _storage.message }
  
  private final class _Storage {
    let code    : String
    let message : String
    public init(code: String, message: String) {
      self.code    = code
      self.message = message
    }
  }
  private let _storage : _Storage
}

// MARK: - Initializers

fileprivate let sharedAllocator = ByteBufferAllocator()

public extension RESPValue {
  
  public init(_ v: Int) {
    self = .integer(v)
  }
  
  public init(bulkString s: String?) {
    if let s = s {
      let utf8   = s.utf8
      var buffer = sharedAllocator.buffer(capacity: utf8.count)
      buffer.write(bytes: utf8)
      self = .bulkString(buffer)
    }
    else {
      self = .bulkString(nil)
    }
  }
  public init(bulkString s: Data) {
    var buffer = sharedAllocator.buffer(capacity: s.count)
    buffer.write(bytes: s)
    self = .bulkString(buffer)
  }

  public init(bulkString s: Int) {
    let s      = String(s)
    let utf8   = s.utf8
    var buffer = sharedAllocator.buffer(capacity: utf8.count)
    buffer.write(bytes: utf8)
    self = .bulkString(buffer)
  }

  public init(simpleString s: String) {
    self = .simpleString(s.utf8.asByteBuffer)
  }

  public init(errorCode code: String, message: String? = nil) {
    self = .error(RESPError(code: code, message: message ?? "Failed: \(code)"))
  }
  
  public init<T: Sequence>(array: T) where T.Element == RESPValue {
    self = .array(ContiguousArray(array))
  }

  public var byteBuffer : ByteBuffer? {
    @inline(__always)
    get {
      switch self {
        case .simpleString(let cs), .bulkString(.some(let cs)): return cs
        default: return nil
      }
    }
  }

  public var stringValue : String? {
    @inline(__always)
    get {
      switch self {
        case .simpleString(let cs), .bulkString(.some(let cs)):
          return cs.getString(at: cs.readerIndex, length: cs.readableBytes)
        
        case .integer(let i):
          return String(i)
        
        default: return nil
      }
    }
  }
  
  public var dataValue : Data? {
    @inline(__always)
    get {
      switch self {
      case .simpleString(let cs), .bulkString(.some(let cs)):
        return cs.getData(at: cs.readerIndex, length: cs.readableBytes)
      default:
        return nil
      }
    }
  }
  
  public var keyValue : Data? {
    @inline(__always)
    get { return self.dataValue }
  }
  
  @inline(__always)
  func withKeyValue(_ cb: ( Data? ) throws -> Void) rethrows {
    // SR-7378
    switch self {
      case .simpleString(let cs), .bulkString(.some(let cs)):
        try cs.withVeryUnsafeBytes { ptr in
          let ip = ptr.baseAddress!.advanced(by: cs.readerIndex)
          let data = Data(bytesNoCopy: UnsafeMutableRawPointer(mutating: ip),
                          count: cs.readableBytes,
                          deallocator: .none)
          try cb(data)
        }
      
      default:
        try cb(nil)
    }
  }
  
  public var intValue : Int? {
    @inline(__always)
    get {
      switch self {
        case .integer(let i):
          return i
        
        case .simpleString(let cs), .bulkString(.some(let cs)):
          // PERF: inline atoi instead of constructing a string!
          guard let s = cs.getString(at: cs.readerIndex,
                                     length: cs.readableBytes) else {
            return nil
          }
          return Int(s)
        
        default:
          return nil
      }
    }
  }
}

public func ==(lhs: RESPValue, rhs: String) -> Bool {
  switch lhs {
    case .simpleString, .bulkString:
      guard let s = lhs.stringValue else { return false }
      return s == rhs
    
    default:
      return false
  }
}


// MARK: - Parse Literals

extension RESPValue : ExpressibleByIntegerLiteral {
  public init(integerLiteral value: IntegerLiteralType) {
    self = .integer(value)
  }
}

import NIOFoundationCompat

extension Data {
  var asByteBuffer : ByteBuffer {
    var bb = sharedAllocator.buffer(capacity: count)
    bb.write(bytes: self)
    return bb
  }
}

extension String.UTF8View {
  var asByteBuffer : ByteBuffer {
    var bb = sharedAllocator.buffer(capacity: count)
    bb.write(bytes: self)
    return bb
  }
}

extension RESPValue : ExpressibleByStringLiteral {
  
  public init(stringLiteral value: String) {
    self = .bulkString(value.utf8.asByteBuffer)
  }
  public init(extendedGraphemeClusterLiteral value: StringLiteralType) {
    self = .bulkString(value.utf8.asByteBuffer)
  }
  public init(unicodeScalarLiteral value: StringLiteralType) {
    self = .bulkString(value.utf8.asByteBuffer)
  }
  
}

extension RESPValue : CustomStringConvertible {
  
  public var description : String {
    switch self {
      case .simpleString(let cs):      return stringValue ?? "\(cs)"
      case .bulkString(.none):         return "<nil-str>"
      case .bulkString(.some(let cs)): return stringValue ?? "\(cs)"
      case .integer(let i):            return String(i)
      case .array(.none):              return "<nil-array>"
      case .array(.some(let members)): return members.description
      case .error(let e):              return "<Error: \(e)>"
    }
  }

}


// MARK: - String Decode

extension String {
  // FIXME: we can probably do this in the buffer
  
  static func decode<I: Collection>(utf8 ba: I) -> String?
                     where I.Iterator.Element == UInt8
  {
    return decode(units: ba, decoder: UTF8())
  }
  
  static func decode<Codec: UnicodeCodec, I: Collection>
                (units b: I, decoder d: Codec) -> String?
                     where I.Iterator.Element == Codec.CodeUnit
  {
    guard !b.isEmpty else { return "" }
    
    let minimumCapacity = 42 // what is a good tradeoff?
    var s = ""
    s.reserveCapacity(minimumCapacity)
    
    var decoder  = d
    var iterator = b.makeIterator()
    while true {
      switch decoder.decode(&iterator) {
        case .scalarValue(let scalar): s.append(String(scalar))
        case .emptyInput: return s
        case .error:      return nil
      }
    }
  }
  
}
