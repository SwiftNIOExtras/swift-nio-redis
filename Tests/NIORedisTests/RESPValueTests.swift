import XCTest
@testable import NIORedis

final class RESPValueTests: XCTestCase {
    func testDescription() throws {
        XCTAssert(String(describing: RESPValue(1)) == "1")
    }

    static var allTests = [
        ("testDescription", testDescription)
    ]
}
