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
import NIORedis

public final class RedisCommand : RESPEncodable {
  
  let values : ContiguousArray<RESPValue>
  
  init(_ values: ContiguousArray<RESPValue>) {
    self.values = values
  }
  
  convenience init<T: Collection>(_ values: T)
             where T.Element : RESPEncodable
  {
    self.init(ContiguousArray(values.map { $0.toRESPValue() }))
  }
  
  public func toRESPValue() -> RESPValue {
    return .array(values)
  }
  
  var isSubscribe : Bool {
    guard let cmd = values.first?.stringValue else { return false }
    return cmd.lowercased().contains("subscribe")
  }
}

public class RedisCommandCall {
  
  let command : RedisCommand
  let promise : EventLoopPromise<RESPValue>
  
  init(command: RedisCommand, promise: EventLoopPromise<RESPValue>) {
    self.command = command
    self.promise = promise
  }
  
  convenience init<T: Collection>(_ values: T, eventLoop: EventLoop)
                where T.Element : RESPEncodable
  {
    self.init(command: RedisCommand(values), promise: eventLoop.newPromise())
  }
}

extension RedisCommand : CustomStringConvertible {
  
  public var description : String {
    return "<RedisCmd: \(values)>"
  }
}
