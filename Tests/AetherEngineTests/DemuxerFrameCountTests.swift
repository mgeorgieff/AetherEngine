import XCTest
@testable import AetherEngine

// mkvmerge writes each track's block count as a `NUMBER_OF_FRAMES` statistics tag; hosts use it to tell a
// forced-only subtitle track from a full one. resolveFrameCount is the tag parse, covered without a fixture.
final class DemuxerFrameCountTests: XCTestCase {
    func test_parsesPositiveCount() {
        XCTAssertEqual(Demuxer.resolveFrameCount("38"), 38)
        XCTAssertEqual(Demuxer.resolveFrameCount(" 1450 "), 1450)
    }

    func test_returnsZeroWhenMissingOrInvalid() {
        XCTAssertEqual(Demuxer.resolveFrameCount(nil), 0)
        XCTAssertEqual(Demuxer.resolveFrameCount(""), 0)
        XCTAssertEqual(Demuxer.resolveFrameCount("x"), 0)
        XCTAssertEqual(Demuxer.resolveFrameCount("0"), 0)
        XCTAssertEqual(Demuxer.resolveFrameCount("-3"), 0)
    }
}
