import XCTest

import NIORedisTests
import RedisTests

var tests = [XCTestCaseEntry]()
tests += NIORedisTests.allTests()
tests += RedisTests.allTests()
XCTMain(tests)
