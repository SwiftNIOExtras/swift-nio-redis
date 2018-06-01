import XCTest
@testable import Redis
import NIO

final class RedisClientTests: XCTestCase {
    func testCreateClientWithDefaultArguments() throws {
        let redisClient = Redis.createClient()

        XCTAssert(redisClient.options.hostname! == "127.0.0.1")
        XCTAssert(redisClient.options.port == 6379)
        XCTAssert(redisClient.options.password == nil)
        XCTAssert(redisClient.options.database == nil)
    }

    func testCreateClientWithSpecifiedArguments() throws {
        let eventLoop = MultiThreadedEventLoopGroup(numberOfThreads: 1).next()

        let redisClient = Redis.createClient(port: 6380, host: "localhost", password: "password", db: 1, eventLoop: eventLoop)

        XCTAssert(redisClient.options.hostname! == "localhost")
        XCTAssert(redisClient.options.port == 6380)
        XCTAssert(redisClient.options.password == "password")
        XCTAssert(redisClient.options.database == 1)
        XCTAssert(redisClient.options.eventLoop === eventLoop)
        XCTAssert(redisClient.eventLoop === eventLoop)
    }

    static var allTests = [
        ("testCreateClientWithDefaultArguments", testCreateClientWithDefaultArguments),
        ("testCreateClientWithSpecifiedArguments", testCreateClientWithSpecifiedArguments),
    ]
}
