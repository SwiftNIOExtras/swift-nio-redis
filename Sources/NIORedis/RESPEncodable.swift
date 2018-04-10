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
import struct NIO.ByteBuffer

public protocol RESPEncodable {
  
  func toRESPValue() -> RESPValue
  
}

extension RESPValue : RESPEncodable {

  public func toRESPValue() -> RESPValue {
    return self
  }

}

extension RESPError : RESPEncodable {
  
  public func toRESPValue() -> RESPValue {
    return .error(self)
  }

}

extension Int : RESPEncodable {
  
  public func toRESPValue() -> RESPValue {
    return .integer(self)
  }
  
}

extension Bool : RESPEncodable {

  public func toRESPValue() -> RESPValue {
    return .integer(self ? 1 : 0)
  }

}

extension String : RESPEncodable {
  
  public func toRESPValue() -> RESPValue {
    return .bulkString(self.utf8.asByteBuffer)
  }
  
}

extension Data : RESPEncodable {
  
  public func toRESPValue() -> RESPValue {
    return .bulkString(self.asByteBuffer)
  }
  
}

extension ByteBuffer : RESPEncodable {
  
  public func toRESPValue() -> RESPValue {
    return .bulkString(self)
  }
  
}


extension Array where Element: RESPEncodable {
  
  public func toRESPValue() -> RESPValue {
    let arrayOfRedisValues = self.map { $0.toRESPValue() }
    return .array(ContiguousArray(arrayOfRedisValues))
  }
  
}

extension Array: RESPEncodable {
  
  public func toRESPValue() -> RESPValue {
    let array : [ RESPValue ] = self.map { v in
      if let rv = (v as? RESPEncodable) {
        return rv.toRESPValue()
      }
      else { // hm, hm. ^ conditional conformance needed!
        return String(describing: v).toRESPValue()
      }
    }
    return .array(ContiguousArray(array))
  }
}
