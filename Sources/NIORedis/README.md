# SwiftNIO Redis - Protocol Implementation

swift-nio-redis is a port of the
[Noze.io redis module](https://github.com/NozeIO/Noze.io/tree/master/Sources/redis).

The NIO implementation has been optimized for performance.

This [Noze.io](http://noze.io/) `RedisParser` stream:

```swift
let parser = RedisParser()

stream! | parser | Writable { values, done in
  handle(replies: values)
  done(nil)
}
```

This is essentially replaced by the
[RESPChannelHandler](RESPChannelHandler.swift).
Instead of piping via `|`, it can be injected into the
Swift NIO channel pipleline like so:

```swift
_ = bootstrap.channelInitializer { channel in
  channel.pipeline
    .configureRedisPipeline()
    .then {
      channel.pipeline.add(YourRedisHandler())
    }
}
```

Your handler will then receive
[RESPValue](RESPValue.swift)
enums as the "readable input",
and it can emit
[RESPEncodable](RESPEncodable.swift)
values are the "writable output".

A `RESPValue` is just an enum with the on-the-write datatypes supported
by RESP:

- `simpleString` (a `ByteBuffer`)
- `bulkString`   (a `ByteBuffer` or `nil`)
- `integer`
- `array`        (a `ContiguousArray` of `RESPValue`s)
- `error`        (an error)

The primary `RESPEncodable` is again a `RESPValue`, but
`Int`'s, `String`'s, `Data`'s etc can also be directly written
w/o having to wrap them in a `RESPValue`.

## Example

For a full example on how to use the protocol implementation,
a [Redis client module](../Redis/) is provided as part of this package.

## Telnet Mode

Besides the binary variant, the Redis protocol also supports a "Telnet mode".
A basic implementation of that is included,
the major piece lacking is quoted strings.
