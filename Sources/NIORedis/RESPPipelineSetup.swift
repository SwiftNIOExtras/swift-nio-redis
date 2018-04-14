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

import class NIO.ChannelPipeline
import class NIO.EventLoopFuture

public extension ChannelPipeline {
  
  public func configureRedisPipeline(first : Bool = false,
                                     name  : String = "de.zeezide.nio.RESP")
              -> EventLoopFuture<Void>
  {
    return self.add(name: name, handler: RESPChannelHandler(), first: first)
  }
  
}
