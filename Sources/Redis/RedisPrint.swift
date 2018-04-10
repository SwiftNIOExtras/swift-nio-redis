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

import NIORedis

public func print(error err: Error?, value: RESPValue?) {
  assert(err != nil || value != nil, "Neither error nor value in Redis result?")
  
  if let error = err {
    print("ERROR: \(error)")
  }

  if let value = value {
    switch value {
      case .array(.some(let values)):
        print(error: nil, values: values)
      
      case .array(.none):
        print("Reply null[]")

      case .error(let error):
        print(error: error, value: nil)
      
      case .integer(let value):
        print("Reply \(value)")
      
      case .simpleString(let value), .bulkString(.some(let value)):
        // TODO: only attempt to decode up to some size
        if let s = value.getString(at: value.readerIndex,
                                   length: value.readableBytes)
        {
          print("Reply \(s)")
        }
        else {
          print("Reply \(value)")
        }

      case .bulkString(.none):
        print("Reply null")
    }
  }
}

public func print<T: Collection>(error err: Error?, values: T?)
              where T.Element == RESPValue
{
  assert(err != nil || values != nil, "Neither error nor vals in Redis result?")
  
  if let error = err {
    print("ERROR: \(error)")
  }
  
  guard let values = values else { return }
  
  print("Reply #\(values.count) values:")
  for i in values.indices {
    let prefix = "  [\(i)]: "
    switch values[i] {
      case .array(.some(let values)):
        print("\(prefix)\(values as Optional)")
      
      case .array(.none):
        print("\(prefix)  null[]")

      case .error(let error):
        print("\(prefix)ERROR \(error)")
      
      case .integer(let value):
        print("\(prefix)\(value)")
      
      case .simpleString(let value), .bulkString(.some(let value)):
        if let s = value.getString(at: value.readerIndex,
                                   length: value.readableBytes)
        {
          print("\(prefix)\(s)")
        }
        else {
          print("\(prefix)\(value)")
        }
      
      case .bulkString(.none):
        print("\(prefix)  null")
    }
  }
}
