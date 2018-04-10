# SwiftNIO Redis

SwiftNIO Redis is a Swift package that contains a high performance 
[Redis protocol](https://redis.io/topics/protocol)
implementation for
[SwiftNIO](https://github.com/apple/swift-nio).
This is a **standalone project** and has no other dependencies but
[SwiftNIO](https://github.com/apple/swift-nio).

Apart from the protocol implementation which can encode and decode
[RESP](https://redis.io/topics/protocol) (REdis Serialization Protocol),
we also provide a [Redis client module](Sources/Redis/README.md)
build on top.

What is Redis?
[Redis](https://redis.io/) is a highly scalable in-memory data structure store,
used as a database, cache and message broker.
For example it can be used to implement a session store backing a web backend
using its "expiring keys" feature,
or it can be used as a relay to implement a chat server using its builtin
[PubSub](https://redis.io/topics/pubsub)
features.

## Performance

This implementation is focused on performance.
It tries to reuse NIO `ByteBuffer`s as much as possible to avoid copies.

The parser is based on a state machine, not on a buffering
`ByteToMessageDecoder`/Encoder.
That doesn't make it nice, but efficient ;-)

## Importing the module using Swift Package Manager

An example `Package.swift `importing the necessary modules:

```swift
// swift-tools-version:4.0

import PackageDescription

let package = Package(
    name: "RedisTests",
    dependencies: [
        .package(url: "https://github.com/NozeIO/swift-nio-redis.git", 
                 from: "0.8.0")
    ],
    targets: [
        .target(name: "MyProtocolTool",
                dependencies: [ "NIORedis" ]),
        .target(name: "MyClientTool",
                dependencies: [ "Redis" ])
    ]
)
```


## Using the SwiftNIO Redis protocol handler

The RESP protocol handler just implemented as a regular
`ChannelHandler`, similar to `NIOHTTP1`.
It takes incoming `ByteBuffer` data, parses that, and emits `RESPValue`
items.
Same the other way around, the user writes `RESPValue` (or `RESPEncodable`)
objects, and the handler renders such into `ByteBuffer`s.

The [NIORedis module](Sources/NIORedis/README.md) has a litte more
information.

To add the RESP handler to a NIO Channel pipeline, the `configureRedisPipeline`
method is called, e.g.:

```swift
bootstrap.channelInitializer { channel in
  channel.pipeline
    .configureRedisPipeline()
    .then { ... }
}
```


## Using the Redis client module

The
[Redis](Sources/Redis/README.md)
client module is modeled after the Node.js
[node_redis](https://github.com/NodeRedis/node_redis)
module,
but it also supports NIO like Promise/Future based methods in addition
to the Node.js `(err,result)` style callbacks. Choose your poison.

### Simple KVS use example:

```swift
import Redis

let client = Redis.createClient()

client.set ("counter", 0, expire: 2)
client.incr("counter", by: 10)
client.get ("counter") { err, value in
    print("Reply:", value)
}
client.keys("*") { err, reply in
    guard let keys = reply else { return print("got no keys!") }
    print("all keys in store:", keys.joined(separator: ","))
}
```

Using NIO Promises:

```swift
client
  .set ("counter", 0, expire: 2)
  .then {
    client.incr("counter", by: 10)
  }
  .then {
    client.get("counter")
  }
  .map {
    print("counter is:", $0)
  }
```


## Status

The
[protocol implementation](Sources/NIORedis/)
is considered complete. There are a few open ends
in the `telnet` variant, yet the regular binary protocol is considered done.

The
[Redis client module](Sources/Redis/)
has a few more open ends, but seems to work fine.


### Who

Brought to you by
[ZeeZide](http://zeezide.de).
We like
[feedback](https://twitter.com/ar_institute),
GitHub stars,
cool [contract work](http://zeezide.com/en/services/services.html),
presumably any form of praise you can think of.
