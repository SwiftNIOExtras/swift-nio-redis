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

public let DefaultRedisPort = 6379

/// Create a Redis client object
public func createClient(port     : Int     = DefaultRedisPort,
                         host     : String  = "127.0.0.1",
                         password : String? = nil,
                         db       : Int?    = nil,
                         eventLoop : EventLoop? = nil)
  -> RedisClient
{
  let options = RedisClientOptions(port: port, host: host,
                                   password: password, database: db)
  if let eventLoop = eventLoop {
    options.eventLoop = eventLoop
  }
  return RedisClient(options: options)
}

extension ChannelOptions {
  
  static let reuseAddr =
    ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET),
                          SO_REUSEADDR)
  
}

/**
 * A connection to a Redis server.
 *
 * The connection is tied to a specific NIO.EventLoop which is setup in the
 * `init` and used as the synchronization point.
 *
 * Simple callback example:
 *
 *     import Redis
 *
 *     let client = Redis.createClient()
 *     client.set("hello", "world")
 *     client.get("hello") { err, reply in
 *       print("reply:", reply")
 *     }
 *
 * Using promises:
 *
 *     _ = client.get("mykey").then { value in
 *       print("mykey is:", value)
 *     }
 *
 * Pub/Sub example:
 *
 *     import Redis
 *
 *     let sub = Redis.createClient(), pub = Redis.createClient()
 *     var msg_count = 0
 *
 *     pub.onSubscribe { channel, count in
 *       pub.publish("a nice channel", "I am sending a message.")
 *       pub.publish("a nice channel", "I am sending a second message.")
 *       pub.publish("a nice channel", "I am sending my last message.")
 *     }
 *
 *     sub.onMessage { channel, message in
 *       print("sub channel \(channel):", message)
 *       msg_count += 1
 *       if msg_count == 3 {
 *         sub.unsubscribe()
 *         sub.quit()
 *         pub.quit()
 *       }
 *     }
 *
 *     sub.subscribe("a nice channel")
 *
 */
open class RedisClient : RedisCommandTarget {
  
  public  let options   : RedisClientOptions
  public  let eventLoop : EventLoop
  
  public enum Error : Swift.Error {
    case writeError(Swift.Error)
    case stopped
    case notImplemented
    case internalInconsistency
    case unexpectedInput
    case channelError(Swift.Error)
  }
  
  enum State {
    case disconnected
    case connecting
    case authenticating(channel: Channel)
    case connected     (channel: Channel)
    case error         (Error)
    case requestedQuit
    case quit
    
    var isConnected : Bool {
      switch self {
        case .connected: return true
        default: return false
      }
    }
    
    var channel : Channel? {
      @inline(__always) get {
        switch self {
          case .authenticating(let channel): return channel
          case .connected     (let channel): return channel
          default: return nil
        }
      }
    }
    
    var canStartConnection : Bool {
      switch self {
        case .disconnected, .error:
          return true
        case .connecting:
          return false
        case .authenticating:
          return false
        case .connected:
          return false
        case .requestedQuit, .quit:
          return false
      }
    }
  }
  
  private var state : State = .disconnected {
    didSet {
      #if false
        print("change state:\n  from: \(oldValue)\n  to:    \(state)")
      #endif
    }
  }
  private let bootstrap : ClientBootstrap
  
  public init(options: RedisClientOptions) {
    self.options = options
    
    self.eventLoop = options.eventLoop
    
    bootstrap = ClientBootstrap(group: self.eventLoop)
    
    _ = bootstrap.channelOption(ChannelOptions.reuseAddr, value: 1)
    
    _ = bootstrap.channelInitializer { [weak self] channel in
      channel.pipeline
        .configureRedisPipeline()
        .thenThrowing { [weak self] in
          guard let me = self else {
            //assert(self != nil, "bootstrap running, but client gone?!")
            throw Error.internalInconsistency
          }
          
          let handler = Handler(client: me)
          _ = channel.pipeline.add(name: "de.zeezide.nio.redis.client",
                                   handler: handler)
        }
    }
  }
  #if false
    deinit {
      print("DEINIT client")
    }
  #endif
  
  // MARK: - Subscriptions
  
  var subscribedChannels = Set<String>()
  var subscribedPatterns = Set<String>()
  
  private var subscribeListeners = EventLoopEventListenerSet<(String, Int)>()
  private var messageListeners = EventLoopEventListenerSet<(String, String)>()
  
  open func subscribe(_ channels: String...) {
    _subscribe(channels: channels)
  }
  open func psubscribe(_ patterns: String...) {
    _subscribe(patterns: patterns)
  }
  
  open func unsubscribe(_ channels: String...) {
    _unsubscribe(channels: channels.isEmpty ? nil : channels)
  }
  open func punsubscribe(_ patterns: String...) {
    _unsubscribe(patterns: patterns.isEmpty ? nil : patterns)
  }
  
  private func _unsubscribe(channels: [ String ]? = [],
                            patterns: [ String ]? = [])
  {
    guard eventLoop.inEventLoop else {
      return eventLoop.execute {
        self._unsubscribe(channels: channels, patterns: patterns)
      }
    }
    
    let remChannels = channels != nil
                    ? subscribedChannels.union(channels!)
                    : subscribedChannels
    if !remChannels.isEmpty {
      subscribedChannels.subtract(remChannels)
      
      if state.isConnected {
        let call = RedisCommandCall([ "UNSUBSCRIBE" ] + remChannels,
                                    eventLoop: eventLoop)
        _enqueueCommandCall(call)
      }
    }
    
    let remPatterns = patterns != nil
                    ? subscribedPatterns.union(patterns!)
                    : subscribedPatterns
    if !remPatterns.isEmpty {
      subscribedPatterns.subtract(remPatterns)
      
      if state.isConnected {
        let call = RedisCommandCall([ "PUNSUBSCRIBE" ] + remPatterns,
                                    eventLoop: eventLoop)
        _enqueueCommandCall(call)
      }
    }
    
    _processQueue()
  }
  private func _subscribe(channels: [ String ] = [],
                          patterns: [ String ] = [])
  {
    guard eventLoop.inEventLoop else {
      return eventLoop.execute {
        self._subscribe(channels: channels, patterns: patterns)
      }
    }
    
    let newChannels = Set(channels).subtracting(subscribedChannels)
    if !newChannels.isEmpty {
      subscribedChannels.formUnion(newChannels)
     
      if state.isConnected {
        let call = RedisCommandCall([ "SUBSCRIBE" ] + newChannels,
                                    eventLoop: eventLoop)
        _enqueueCommandCall(call)
      }
    }
    
    let newPatterns = Set(patterns).subtracting(subscribedPatterns)
    if !newPatterns.isEmpty {
      subscribedPatterns.formUnion(newChannels)
      
      if state.isConnected {
        let call = RedisCommandCall([ "PSUBSCRIBE" ] + newPatterns,
                                    eventLoop: eventLoop)
        _enqueueCommandCall(call)
      }
    }
    
    _processQueue()
  }
  
  @discardableResult
  open func onSubscribe(_ cb: @escaping ( String, Int ) -> Void) -> Self {
    if eventLoop.inEventLoop {
      subscribeListeners.append(cb)
    }
    else {
      eventLoop.execute {
        self.subscribeListeners.append(cb)
      }
    }
    return self
  }
  
  @discardableResult
  open func onMessage(_ cb: @escaping ( String, String ) -> Void) -> Self {
    if eventLoop.inEventLoop {
      messageListeners.append(cb)
    }
    else {
      eventLoop.execute {
        self.messageListeners.append(cb)
      }
    }
    return self
  }

  
  // MARK: - Commands
  
  var callQueue    = CircularBuffer<RedisCommandCall>(initialRingCapacity: 16)
  var pendingCalls = CircularBuffer<RedisCommandCall>(initialRingCapacity: 16)
  
  public func enqueueCommandCall(_ call: RedisCommandCall) { // Q: any
    guard eventLoop.inEventLoop else {
      return eventLoop.execute { self.enqueueCommandCall(call) }
    }
    _enqueueCommandCall(call)
    _processQueue()
  }
  
  @discardableResult
  public func _enqueueCommandCall(_ call: RedisCommandCall) // Q: own
                -> EventLoopFuture<RESPValue>
  {
    callQueue.append(call)
    return call.promise.futureResult
  }

  private func _processQueue() { // Q: own
    switch state {
      case .disconnected:
        _ = _connect(host: options.hostname ?? "localhost", port: options.port)
      
      case .requestedQuit, .quit:
        callQueue.forEach { $0.promise.fail(error: Error.stopped) }
      
      case .error(let error):
        callQueue.forEach { $0.promise.fail(error: error) }

      case .connecting, .authenticating: break
      
      case .connected(let channel):
        _sendQueuedCommands(to: channel)
    }
  }
  
  func _sendQueuedCommands(to channel: Channel) {
    guard !callQueue.isEmpty else { return }

    while !callQueue.isEmpty {
      let call = callQueue.removeFirst()
      
      channel.write(call.command)
        .map         { self.pendingCalls.append(call) }
        .whenFailure { call.promise.fail(error: Error.writeError($0)) }
    }
    channel.flush()
  }
  
  func _handleReply(_ value: RESPValue) { // Q: own
    //Redis.print(error: nil, value: value)
    
    if !pendingCalls.isEmpty {
      let call = pendingCalls.removeFirst()
      
      if !call.command.isSubscribe {
        call.promise.succeed(result: value)
        return
      }
      call.promise.succeed(result: .bulkString(nil)) // TBD
    }
    
    // PubSub handling
    
    if case .array(.some(let items)) = value, items.count > 2,
       let cmd = items[0].stringValue?.lowercased()
    {
      switch ( cmd, items[1], items[2] ) {
        case ( "message", let channel, let message ):
          if let channel = channel.stringValue,
             let message = message.stringValue
          {
            return messageListeners.emit( ( channel, message ) )
          }
        case ( "subscribe", let channel, let count ):
          if let channel = channel.stringValue, let count = count.intValue {
            return subscribeListeners.emit( (channel, count ) )
          }
        case ( "unsubscribe", _, _ ): return
        default: break
      }
    }
    
    Redis.print(error: nil, value: value)
    assertionFailure("reply, but no pending calls?")
    return _closeOnUnexpectedError(Error.unexpectedInput)
  }
  
  
  // MARK: - Connect

  var retryInfo = RedisRetryInfo()
  var channel : Channel? { @inline(__always) get { return state.channel } }
  
  public func quit() {
    _enqueueCommandCall(RedisCommandCall(["QUIT"], eventLoop: eventLoop))
      .whenComplete {
        self.state = .quit
        self.subscribeListeners.removeAll()
        self.messageListeners.removeAll()
      }
    _processQueue()
  }
  
  private func _connect(host: String, port: Int) -> EventLoopFuture<Channel> {
    assert(state.canStartConnection, "cannot start connection!")
    
    state = .connecting
    retryInfo.attempt += 1
    return bootstrap.connect(host: host, port: port)
      .map { channel in
        self.retryInfo.registerSuccessfulConnect()
        
        guard case .connecting = self.state else {
          assertionFailure("called \(#function) but we are not connecting?")
          return channel
        }
        
        self.state = .authenticating(channel: channel)
        self._authenticate()
        return channel
      }
  }
  
  func _closeOnUnexpectedError(_ error: Swift.Error? = nil) {
    if let error = error {
      self.retryInfo.lastSocketError = error
    }
    _ = channel?.close(mode: .all)
  }
  
  private func _authenticate() {
    guard case .authenticating(let channel) = state else {
      assertionFailure("called \(#function) but we are not connecting?")
      return
    }
    
    // TODO: OK, auth not supported :-) Trivial to add!
    
    // now we are connected
    state = .connected(channel: channel)
    
    // when this is done, we want to re-subscribe channels
    _resubscribe()
  }
  
  
  // MARK: - Subscriptions
  
  private func _resubscribe() {
    if !subscribedChannels.isEmpty {
      let call = RedisCommandCall([ "SUBSCRIBE" ] + subscribedChannels,
                                  eventLoop: eventLoop)
      callQueue.append(call)
    }
    
    if !subscribedPatterns.isEmpty {
      let call = RedisCommandCall([ "PSUBSCRIBE" ] + subscribedPatterns,
                                  eventLoop: eventLoop)
      callQueue.append(call)
    }

    _processQueue()
  }
  
  
  // MARK: - Retry
  
  #if false // TODO: finish Noze port
  private func retryConnectAfterFailure() {
    let retryHow : RedisRetryResult
    
    if let cb = options.retryStrategy {
      retryHow = cb(retryInfo)
    }
    else {
      if retryInfo.attempt < 10 {
        retryHow = .retryAfter(TimeInterval(retryInfo.attempt) * 0.200)
      }
      else {
        retryHow = .stop
      }
    }
    
    switch retryHow {
      case .retryAfter(let timeout):
        // TBD: special Retry status?
        if state != .connecting {
          state = .connecting
          eventLoop.scheduleTask(in: .milliseconds(timeout * 1000.0)) {
            self.state = .disconnected
            self.connect()
          }
        }
      
      case .error(let error):
        stop(error: error)
      
      case .stop:
        stop(error: RedisClientError.ConnectionQuit)
    }
  }
  #endif
  
  
  // MARK: - Handler Delegate
  
  func handlerDidDisconnect(_ ctx: ChannelHandlerContext) { // Q: own
    switch state {
      case .error, .quit: break // already handled
      default: state = .disconnected
    }
  }
  
  func handlerHandleResult(_ value: RESPValue) { // Q: own
    
    switch state {
      case .authenticating:
        // TODO: process auth result
        Redis.print(error: nil, value: value)
        assertionFailure("not implemented: AUTH")
        return _closeOnUnexpectedError(Error.notImplemented)

      case .connected:
        _handleReply(value)
      
      default:
        Redis.print(error: nil, value: value)
        assertionFailure("unexpected receive: \(state)")
        print("ERROR: unexpected receive:", value)
        // TODO: emit client error
        return _closeOnUnexpectedError(Error.unexpectedInput)
    }
  }
  
  func handlerCaughtError(_ error: Swift.Error,
                          in ctx: ChannelHandlerContext) // Q: own
  {
    retryInfo.lastSocketError = error
    state = .error(.channelError(error))
    
    print("RedisClient error:", error)
    Redis.print(error: error, value: nil)
  }
  
  
  // MARK: - Handler
  
  final class Handler : ChannelInboundHandler {
    
    typealias InboundIn = RESPValue
    
    let client  : RedisClient
    
    init(client: RedisClient) {
      self.client = client
    }
    
    func channelActive(ctx: ChannelHandlerContext) {
    }
    func channelInactive(ctx: ChannelHandlerContext) {
      client.handlerDidDisconnect(ctx)
    }
    
    func channelRead(ctx: ChannelHandlerContext, data: NIOAny) {
      let value = unwrapInboundIn(data)
      client.handlerHandleResult(value)
    }
    
    public func errorCaught(ctx: ChannelHandlerContext, error: Error) {
      self.client.handlerCaughtError(error, in: ctx)
      _ = ctx.close(promise: nil)
    }
  }
  
}

fileprivate class EventLoopEventListenerSet<T> {
  
  var listeners = Array<( T ) -> Void>()
  
  func append(_ cb: @escaping ( T ) -> Void) {
    listeners.append(cb)
  }
  
  func emit(_ value: T) {
    for listener in listeners {
      listener(value)
    }
  }
  
  func removeAll() {
    listeners.removeAll()
  }
}
