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

fileprivate let onDemandSharedEventLoopGroup =
                  MultiThreadedEventLoopGroup(numberOfThreads: 1)

/// Configuration options for the socket connects
open class ConnectOptions : CustomStringConvertible {
  
  public var eventLoopGroup : EventLoopGroup
  public var hostname       : String?
  public var port           : Int
  
  public init(hostname: String? = "localhost", port: Int = 80,
              eventLoopGroup: EventLoopGroup? = nil)
  {
    self.hostname = hostname
    self.port     = port
    self.eventLoopGroup = eventLoopGroup
                       ?? MultiThreadedEventLoopGroup.currentEventLoop
                       ?? onDemandSharedEventLoopGroup
  }
  
  public var description: String {
    var ms = "<\(type(of: self)):"
    appendToDescription(&ms)
    ms += ">"
    return ms
  }
  
  open func appendToDescription(_ ms: inout String) {
    if let hostname = hostname {
      ms += " \(hostname):\(port)"
    }
    else {
      ms += " \(port)"
    }
  }
  
}

/// Configuration options for the Redis client object
public class RedisClientOptions : ConnectOptions {
  
  var password      : String?
  var database      : Int?
  var retryStrategy : RedisRetryStrategyCB?
  
  public init(port           : Int     = DefaultRedisPort,
              host           : String  = "127.0.0.1",
              password       : String? = nil,
              database       : Int?    = nil,
              eventLoopGroup : EventLoopGroup? = nil)
  {
    self.password      = password
    self.database      = database
    self.retryStrategy = nil
    
    super.init(hostname: host, port: port, eventLoopGroup: eventLoopGroup)
  }

  override open func appendToDescription(_ ms: inout String) {
    super.appendToDescription(&ms)
    if let database      = database      { ms += " #\(database)"     }
    if password != nil                   { ms += " pwd"              }
    if let retryStrategy = retryStrategy { ms += " \(retryStrategy)" }
  }
}
