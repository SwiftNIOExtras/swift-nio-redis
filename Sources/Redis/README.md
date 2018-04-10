# SwiftNIO Redis - Client

Designed after [node_redis](https://github.com/NodeRedis/node_redis).

### Sample

```swift
import Redis

let client = Redis.createClient()

client.set("foo_rand00000", "OK")

client.get("foo_rand00000") { err, reply in
    print("Reply:", reply)
}
```

### PubSub Sample

```swift
import Redis

let sub = Redis.createClient(), pub = Redis.createClient()
var msg_count = 0

sub.onSubscribe { channel, count in
  pub.publish("a nice channel", "I am sending a message.")
  pub.publish("a nice channel", "I am sending a second message.")
  pub.publish("a nice channel", "I am sending my last message.")
}

sub.onMessage { channel, message in
  print("sub channel \(channel):", message)
  msg_count += 1
  if msg_count == 3 {
    sub.unsubscribe()
    sub.quit()
    pub.quit()
  }
}

sub.subscribe("a nice channel")
```

## Limitations:

- no auth
- reconnect strategy needs to be implemented
- only basic commands
- no `client.multi` or `client.batch`
- no URL

