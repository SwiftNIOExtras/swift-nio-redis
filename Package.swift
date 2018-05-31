// swift-tools-version:4.0

import PackageDescription

let package = Package(
    name: "swift-nio-redis",
    products: [
        .library   (name: "NIORedis", targets: [ "NIORedis" ]),
        .library   (name: "Redis",    targets: [ "Redis"    ]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", 
                 from: "1.8.0"),
    ],
    targets: [
        .target(name: "NIORedis", dependencies: [ "NIO", "NIOFoundationCompat" ]),
        .target(name: "Redis",    dependencies: [ "NIORedis"    ])
    ]
)
