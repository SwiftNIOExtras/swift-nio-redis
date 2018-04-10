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

import struct Foundation.Data
import struct Foundation.TimeInterval
import enum   NIORedis.RESPValue

protocol RedisTypeTransformable {
  
  static func extractFromRESPValue(_ value: RESPValue) throws -> Self
  
}

enum RedisTypeTransformationError : Swift.Error {
  case unexpectedValueType (RESPValue)
  case unsupportedValueType(Any.Type)
  case unexpectedHashValue (RESPValue)
}

extension RESPValue: RedisTypeTransformable {

  static func extractFromRESPValue(_ value: RESPValue) throws -> RESPValue {
    return value
  }

}

extension TimeInterval: RedisTypeTransformable {
  
  static func extractFromRESPValue(_ value: RESPValue) throws -> TimeInterval {
    guard let s = value.intValue else {
      throw RedisTypeTransformationError.unexpectedValueType(value)
    }
    return TimeInterval(s) // full seconds granularity only
  }
  
}

extension Int: RedisTypeTransformable {
  
  static func extractFromRESPValue(_ value: RESPValue) throws -> Int {
    guard let s = value.intValue else {
      throw RedisTypeTransformationError.unexpectedValueType(value)
    }
    return s
  }
  
}

extension String: RedisTypeTransformable {
  
  static func extractFromRESPValue(_ value: RESPValue) throws -> String {
    guard let s = value.stringValue else {
      throw RedisTypeTransformationError.unexpectedValueType(value)
    }
    return s
  }
  
}

extension Bool: RedisTypeTransformable {
  
  static func extractFromRESPValue(_ value: RESPValue) throws -> Bool {
    guard let s = value.intValue else {
      throw RedisTypeTransformationError.unexpectedValueType(value)
    }
    return s != 0 ? true : false
  }
  
}

extension Data: RedisTypeTransformable {

  static func extractFromRESPValue(_ value: RESPValue) throws -> Data {
    guard let s = value.dataValue else {
      throw RedisTypeTransformationError.unexpectedValueType(value)
    }
    return s
  }

}

#if swift(>=4.1)

extension Optional : RedisTypeTransformable
                       where Wrapped : RedisTypeTransformable
{
  static func extractFromRESPValue(_ value: RESPValue) throws -> Wrapped? {
    switch value {
      case .bulkString(.none): return nil
      case .array     (.none): return nil
      default: return try Wrapped.extractFromRESPValue(value)
    }
  }
}

extension Array : RedisTypeTransformable
                       where Element : RedisTypeTransformable
{
  
  static func extractFromRESPValue(_ value: RESPValue) throws -> [ Element ] {
    guard case .array(let itemsOpts) = value else {
      throw RedisTypeTransformationError.unexpectedValueType(value)
    }
    guard let items = itemsOpts else { return [] } // Hm.
  
    return try items.map { try Element.extractFromRESPValue($0) }
  }
}

extension Dictionary : RedisTypeTransformable
            where Key == String, Value == String
{

  static func extractFromRESPValue(_ value: RESPValue)
                throws -> [ Key : Value ]
  {
    guard case .array(let itemsOpts) = value else {
      throw RedisTypeTransformationError.unexpectedValueType(value)
    }
    guard let items = itemsOpts, !items.isEmpty else { return [:] }

    guard items.count % 2 == 0 else {
      throw RedisTypeTransformationError.unexpectedHashValue(value)
    }

    var dict = [ Key : Value ]()
    dict.reserveCapacity(items.count / 2 + 1)
  
    for i in stride(from: items.startIndex, to: items.count, by: 2) {
      dict[try Key  .extractFromRESPValue(items[i])] =
           try Value.extractFromRESPValue(items[i + 1])
    }
    return dict
  }
}
#else

fileprivate
func _extractFromRESPValue<Wrapped>(_ value: RESPValue) throws -> Wrapped {
  let result : Wrapped
  if Wrapped.self == String.self {
    result = try String.extractFromRESPValue(value) as! Wrapped
  }
  else if Wrapped.self == Data.self {
    result = try Data.extractFromRESPValue(value) as! Wrapped
  }
  else if Wrapped.self == RESPValue.self {
    result = try RESPValue.extractFromRESPValue(value) as! Wrapped
  }
  else if Wrapped.self == Int.self {
    result = try Int.extractFromRESPValue(value) as! Wrapped
  }
  else {
    throw RedisTypeTransformationError.unsupportedValueType(Wrapped.self)
  }
  return result
}

extension Optional : RedisTypeTransformable {
  static func extractFromRESPValue(_ value: RESPValue) throws -> Wrapped? {
    switch value {
      case .bulkString(.none): return nil
      case .array     (.none): return nil
      default:                 return try _extractFromRESPValue(value)
    }
  }
}

extension Array : RedisTypeTransformable {
  
  static func extractFromRESPValue(_ value: RESPValue) throws -> [ Element ] {
    guard case .array(let itemsOpts) = value else {
      throw RedisTypeTransformationError.unexpectedValueType(value)
    }
    guard let items = itemsOpts else { return [] } // Hm.
    
    return try items.map { try _extractFromRESPValue($0) }
  }
}

extension Dictionary : RedisTypeTransformable {

  static func extractFromRESPValue(_ value: RESPValue)
                throws -> [ Key : Value ]
  {
    guard case .array(let itemsOpts) = value else {
      throw RedisTypeTransformationError.unexpectedValueType(value)
    }
    guard let items = itemsOpts, !items.isEmpty else { return [:] }

    guard items.count % 2 == 0 else {
      throw RedisTypeTransformationError.unexpectedHashValue(value)
    }

    var dict = [ Key : Value ]()
    dict.reserveCapacity(items.count / 2 + 1)
    
    for i in stride(from: items.startIndex, to: items.count, by: 2) {
      dict[try _extractFromRESPValue(items[i])] =
           try _extractFromRESPValue(items[i + 1])
    }
    return dict
  }
}
#endif // Swift <4.1
