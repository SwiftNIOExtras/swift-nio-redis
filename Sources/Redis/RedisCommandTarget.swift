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

import protocol NIO.EventLoop

public protocol RedisCommandTarget {
  
  func enqueueCommandCall(_ call: RedisCommandCall)
  
  var eventLoop : EventLoop { get }
  
}

public enum RedisKeySetMode {
  case always, ifMissing, ifExisting
}

import struct Foundation.Data
import struct Foundation.TimeInterval
import struct Foundation.Date

public extension RedisCommandTarget { // Future based

  public func ping(_ message: String? = nil) -> EventLoopFuture<String> {
    return _enqueue([ RESPValue(bulkString: "PING"),
                      RESPValue(bulkString: message) ])
  }
  
  @discardableResult
  public func publish(_ channel: String, _ message: String)
                -> EventLoopFuture<Int>
  {
    return _enqueue([ "PUBLISH", channel, message ])
  }
  
  
  // MARK: - Basic KVS

  public func get(_ key: String) -> EventLoopFuture<String> {
    return _enqueue([ "GET", key ])
  }

  public func set(_ key: String, _ v: RESPValue,
                  expire to: TimeInterval? = nil,
                  mode: RedisKeySetMode = .always) -> EventLoopFuture<RESPValue>
  {
    var vals = [ RESPValue(bulkString: "SET"), RESPValue(bulkString: key), v ]
    if let to = to {
      vals.append("PX")
      vals.append(RESPValue.integer(Int(to * 1000.0)))
    }
    switch mode {
      case .ifExisting: vals.append("XX")
      case .ifMissing:  vals.append("NX")
      case .always:     break
    }
    
    return _enqueue(vals)
  }
  @discardableResult
  public func set(_ key: String, _ v: String,
                  expire to: TimeInterval? = nil,
                  mode: RedisKeySetMode = .always) -> EventLoopFuture<RESPValue>
  {
    return set(key, RESPValue(bulkString: v), expire: to, mode: mode)
  }
  
  @discardableResult
  public func set(_ key: String, _ v: Int) -> EventLoopFuture<RESPValue> {
    return set(key, RESPValue(bulkString: v))
  }
  
  @discardableResult
  public func set(_ key: String, _ v: Data,
                  expire to: TimeInterval? = nil,
                  mode: RedisKeySetMode = .always)
              -> EventLoopFuture<RESPValue>
  {
    return set(key, RESPValue(bulkString: v), expire: to, mode: mode)
  }
  
  public func keys(_ pattern: String = "*") -> EventLoopFuture<[String]> {
    return _enqueue([ "KEYS", pattern ])
  }
  
  @discardableResult
  public func del(keys: [ String ]) -> EventLoopFuture<RESPValue> {
    return _enqueue([ "DEL" ] + keys)
  }
  
  @discardableResult
  public func del(_ keys: String...) -> EventLoopFuture<RESPValue> {
    return del(keys: keys)
  }
  

  // MARK: - Integer Operations

  @discardableResult
  public func incr(_ key: String) -> EventLoopFuture<Int> {
    return _enqueue([ "INCR", key ])
  }
  @discardableResult
  public func decr(_ key: String) -> EventLoopFuture<Int> {
    return _enqueue([ "DECR", key ])
  }
  
  @discardableResult
  public func incr(_ key: String, by v: Int) -> EventLoopFuture<Int> {
    let vals = [ RESPValue(bulkString: "INCRBY"),
                 RESPValue(bulkString: key),
                 RESPValue(bulkString: v) ]
    return _enqueue(vals)
  }
  @discardableResult
  public func decr(_ key: String, by v: Int) -> EventLoopFuture<Int> {
    let vals = [ RESPValue(bulkString: "DECRBY"),
                 RESPValue(bulkString: key),
                 RESPValue(bulkString: v) ]
    return _enqueue(vals)
  }
  
  
  // MARK: - Hashes

  @discardableResult
  public func hset(_ key: String, _ field: String, _ value: String)
                -> EventLoopFuture<Bool>
  {
    return _enqueue([ "HSET", key, field, value ])
  }
  
  public func hkeys(_ key: String) -> EventLoopFuture<[String]> {
    return _enqueue([ "HKEYS", key ])
  }

  public func hgetall(_ key: String) -> EventLoopFuture<[ String : String ]> {
    return _enqueue([ "HGETALL", key ])
  }

  public func hmget(_ key: String, _ keys: [ String ])
                -> EventLoopFuture<[ String ]>
  {
    return _enqueue([ "HMGET", key ] + keys)
  }
  public func hmget(_ key: String, _ keys: String...)
                -> EventLoopFuture<[ String ]>
  {
    return hmget(key, keys)
  }
  
  @discardableResult
  public func hmset(_ key: String, _ hash: [ String : Any ])
                -> EventLoopFuture<RESPValue>
  {
    var vals = ContiguousArray<RESPValue>()
    vals.reserveCapacity(2 + hash.count)
    vals.append(RESPValue(bulkString: "HMSET"))
    vals.append(RESPValue(bulkString: key))
    for ( key, value ) in hash {
      vals.append(RESPValue(bulkString: key))
      vals.append(RESPValue(bulkString: "\(value)"))
    }
    return _enqueue(vals)
  }
  
  @discardableResult
  public func hmset(_ key: String, keyValues: [ String ])
                -> EventLoopFuture<RESPValue>
  {
    return _enqueue([ "HMSET", key ] + keyValues)
  }
  @discardableResult
  public func hmset(_ key: String, _ keyValues: String...)
    -> EventLoopFuture<RESPValue>
  {
    return _enqueue([ "HMSET", key ] + keyValues)
  }
  
  
  // MARK: - Expiration
  
  /// Expire the key in the specified seconds, in *full seconds granularity*.
  @discardableResult
  public func expire(_ key: String, in seconds: TimeInterval)
                -> EventLoopFuture<String>
  {
    return _enqueue([ RESPValue(bulkString: "EXPIRE"),
                      RESPValue(bulkString: key),
                      RESPValue.integer(Int(seconds))])
  }

  /// Expire the key in the specified seconds, in *full seconds granularity*.
  @discardableResult
  public func expire(_ key: String, at date: Date)
                -> EventLoopFuture<String>
  {
    let ts = Int(date.timeIntervalSince1970)
    return _enqueue([ RESPValue(bulkString: "EXPIREAT"),
                      RESPValue(bulkString: key),
                      RESPValue.integer(ts)])
  }

  /// Expire the key in the specified seconds, in *millisecond granularity*.
  @discardableResult
  public func pexpire(_ key: String, in seconds: TimeInterval)
                -> EventLoopFuture<String>
  {
    return _enqueue([ RESPValue(bulkString: "PEXPIRE"),
                      RESPValue(bulkString: key),
                      RESPValue.integer(Int(seconds * 1000.0)) ])
  }
  /// Expire the key in the specified seconds, in *full seconds granularity*.
  @discardableResult
  public func pexpire(_ key: String, at date: Date)
                -> EventLoopFuture<String>
  {
    let ts = Int(date.timeIntervalSince1970 * 1000.0)
    return _enqueue([ RESPValue(bulkString: "PEXPIREAT"),
                      RESPValue(bulkString: key),
                      RESPValue.integer(ts) ])
  }
  
  @discardableResult
  public func persist(_ key: String) -> EventLoopFuture<String> {
    return _enqueue([ "PERSIST", key ])
  }
  
  @discardableResult
  public func ttl(_ key: String) -> EventLoopFuture<TimeInterval> {
    return _enqueue([ "TTL", key ])
  }
}

public extension RedisCommandTarget { // Callback based

  public func ping(_ message: String? = nil,
                   _ cb: @escaping (Error?, String?) -> Void)
  {
    ping(message).whenCB(cb)
  }
  public func publish(_ channel: String, _ message: String,
                      _ cb: @escaping (Error?, Int?) -> Void)
  {
    publish(channel, message).whenCB(cb)
  }
  
  // MARK: - Basic KVS

  public func get(_ key: String, _ cb: @escaping (Error?, String?) -> Void) {
    get(key).whenCB(cb)
  }
  
  public func set(_ key: String, _ value: String,
                  expire to: TimeInterval? = nil,
                  mode: RedisKeySetMode = .always,
                  _ cb: @escaping ( Error?, RESPValue? ) -> Void)
  {
    set(key, value, expire: to, mode: mode).whenCB(cb)
  }
  public func set(_ key: String, _ value: Int,
                  _ cb: @escaping ( Error?, RESPValue? ) -> Void)
  {
    set(key, value).whenCB(cb)
  }

  public func keys(_ pattern: String = "*",
                   _ cb: @escaping (Error?, [String]?) -> Void)
  {
    keys(pattern).whenCB(cb)
  }

  public func del(keys: [ String ], _ cb: @escaping (Error?, Int?) -> Void) {
    del(keys: keys).whenCB(cb)
  }
  public func del(_ keys: String..., cb: @escaping (Error?, Int?) -> Void) {
    del(keys: keys).whenCB(cb)
  }

  
  // MARK: - Integer Operations

  public func incr(_ key: String, _ cb: @escaping (Error?, Int?) -> Void) {
    incr(key).whenCB(cb)
  }
  public func decr(_ key: String, _ cb: @escaping (Error?, Int?) -> Void) {
    decr(key).whenCB(cb)
  }
  public func incr(_ key: String, by v: Int,
                   _ cb: @escaping (Error?, Int?) -> Void)
  {
    incr(key).whenCB(cb)
  }
  public func decr(_ key: String, by v: Int,
                   _ cb: @escaping (Error?, Int?) -> Void)
  {
    decr(key).whenCB(cb)
  }
  
  
  // MARK: - Hashes

  public func hset(_ key: String, _ field: String, _ value: String,
                   _ cb: @escaping (Error?, Bool?) -> Void)
  {
    hset(key, field, value).whenCB(cb)
  }
  public func hkeys(_ key: String,
                    _ cb: @escaping (Error?, [ String ]?) -> Void)
  {
    hkeys(key).whenCB(cb)
  }
  public func hgetall(_ key: String,
                      _ cb: @escaping (Error?, [ String : String ]?) -> Void)
  {
    hgetall(key).whenCB(cb)
  }
  public func hmget(_ key: String, _ keys: [ String ],
                    _ cb: @escaping (Error?, [ String ]?) -> Void)
  {
    hmget(key, keys).whenCB(cb)
  }
  public func hmget(_ key: String, _ keys: String...,
                    cb: @escaping (Error?, [ String ]?) -> Void)
  {
    hmget(key, keys).whenCB(cb)
  }

  public func hmset(_ key: String, _ hash: [ String : Any ],
                    cb: @escaping (Error?, RESPValue?) -> Void)
  {
    hmset(key, hash).whenCB(cb)
  }
  public func hmset(_ key: String, _ keyValues: String...,
                    cb: @escaping (Error?, RESPValue?) -> Void)
  {
    hmset(key, keyValues: keyValues).whenCB(cb)
  }
}


// MARK: - Callback Helpers

import class    NIO.EventLoopFuture
import enum     NIORedis.RESPValue
import protocol NIORedis.RESPEncodable

fileprivate extension EventLoopFuture {
  
  func whenCB(file: StaticString = #file, line: UInt = #line,
              _ cb: @escaping ( Swift.Error?, T? ) -> Void) -> Void
  {
    self.map(file: file, line: line) { cb(nil, $0) }
        .whenFailure { cb($0, nil) }
  }
}

fileprivate extension EventLoopFuture where T == RESPValue {
  
  func whenCB<U: RedisTypeTransformable>(file: StaticString = #file, line: UInt = #line,
              _ cb: @escaping ( Swift.Error?, U? ) -> Void) -> Void
  {
    self.map(file: file, line: line) {
          do { cb(nil, try U.extractFromRESPValue($0)) }
          catch { cb(error, nil) }
        }
        .whenFailure { cb($0, nil) }
  }
}

public extension RedisCommandTarget {
  
  internal
  func _enqueue<T: Collection, U: RedisTypeTransformable>(_ values: T)
       -> EventLoopFuture<U>
          where T.Element : RESPEncodable
  {
    let call   = RedisCommandCall(values, eventLoop: eventLoop)
    let future = call.promise.futureResult.thenThrowing {
      try U.extractFromRESPValue($0)
    }
    enqueueCommandCall(call)
    return future
  }
  
  fileprivate
  func _enqueue<T: Collection, U: RedisTypeTransformable>(_ values: T,
                               _ cb: @escaping ( Error?, U? ) -> Void)
          where T.Element : RESPEncodable
  {
    let call    = RedisCommandCall(values, eventLoop: eventLoop)
    call.promise.futureResult.whenCB(cb)
    enqueueCommandCall(call)
  }
}

