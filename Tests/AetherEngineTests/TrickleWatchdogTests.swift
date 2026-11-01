import Testing
import Foundation
@testable import AetherEngine

/// A connection that TRICKLES while a read waits on it.
///
/// The field case (4K HDR over a debrid origin, held connection): a fresh generation delivered about
/// 256 KB every 40 to 100 s for three minutes. Every drip reset the delivery-gap watchdog's clock
/// and signalled the read loop's forward wait, so neither detector ever fired, the consumer sat in
/// one slow read after another and AVPlayer stalled. The trickle watchdog judges volume over the
/// last stall period instead, and only while a read is actually blocked on forward data.
///
/// The origin here paces each response individually, which `ThrottledOriginServer` cannot (its
/// throttle is per server, and the open must stay fast): the first request at `dripOffset` gets the
/// scripted pace, every other request is served at loopback speed.
@Suite("Trickle watchdog", .serialized, .offCooperativePool)
struct TrickleWatchdogTests {

    private static let totalSize: Int64 = 256 * 1024 * 1024
    /// The open's bounded range. Its completion puts the next request at `dripOffset`.
    private static let firstRange: Int64 = 2 * 1024 * 1024
    private static var dripOffset: Int64 { firstRange }
    private static let stallTimeout: TimeInterval = 0.5

    /// How one response body is written.
    enum Pace: Sendable {
        case fast
        /// `bytes` per write, then `everyUs` microseconds of sleep.
        case drip(bytes: Int, everyUs: useconds_t)
    }

    /// Loopback 206 origin with a per-request pace. Keep-alive, Range-aware, no other features.
    final class PacedOrigin: @unchecked Sendable {
        let port: UInt16
        private let listener: LoopbackListener
        private let totalSize: Int64
        private let pace: @Sendable (_ start: Int64, _ attemptAtStart: Int) -> Pace
        private let lock = NSLock()
        private var _stopped = false
        private var _fds: [Int32] = []
        private var _requests: [(start: Int64, at: Date)] = []

        init?(totalSize: Int64, pace: @escaping @Sendable (_ start: Int64, _ attemptAtStart: Int) -> Pace) {
            guard let listener = LoopbackListener() else { return nil }
            self.listener = listener
            self.port = listener.port
            self.totalSize = totalSize
            self.pace = pace
            listener.start { [self] fd in admit(fd) }
        }

        var requests: [(start: Int64, at: Date)] {
            lock.lock(); defer { lock.unlock() }
            return _requests
        }

        private var stopped: Bool {
            lock.lock(); defer { lock.unlock() }
            return _stopped
        }

        func stop() {
            lock.lock()
            let already = _stopped
            _stopped = true
            let fds = already ? [] : _fds
            for fd in fds { shutdown(fd, SHUT_RDWR) }
            lock.unlock()
            if !already { listener.stop() }
        }

        private func admit(_ fd: Int32) -> Bool {
            lock.lock()
            if _stopped {
                lock.unlock()
                shutdown(fd, SHUT_RDWR)
                close(fd)
                return false
            }
            _fds.append(fd)
            lock.unlock()
            Thread.detachNewThread { [self] in
                while !stopped, serveOne(fd) {}
                lock.lock()
                _fds.removeAll { $0 == fd }
                close(fd)
                lock.unlock()
            }
            return true
        }

        private func serveOne(_ fd: Int32) -> Bool {
            var buf = [UInt8](repeating: 0, count: 16 * 1024)
            var collected = Data()
            while collected.range(of: Data("\r\n\r\n".utf8)) == nil {
                let n = recv(fd, &buf, buf.count, 0)
                guard n > 0 else { return false }
                collected.append(contentsOf: buf[0..<n])
            }
            let request = String(decoding: collected, as: UTF8.self)
            var start: Int64 = 0
            var last = totalSize - 1
            if let line = request.components(separatedBy: "\r\n")
                .first(where: { $0.lowercased().hasPrefix("range:") }),
               let eq = line.range(of: "bytes=") {
                let spec = line[eq.upperBound...].trimmingCharacters(in: .whitespaces)
                let parts = spec.split(separator: "-", omittingEmptySubsequences: false).map(String.init)
                if parts.count == 2 {
                    if parts[0].isEmpty, let suffix = Int64(parts[1]) {
                        start = max(0, totalSize - suffix)
                    } else {
                        start = Int64(parts[0]) ?? 0
                        if let end = Int64(parts[1]) { last = min(end, totalSize - 1) }
                    }
                }
            }
            lock.lock()
            let attempt = _requests.filter { $0.start == start }.count + 1
            _requests.append((start, Date()))
            lock.unlock()

            let length = last - start + 1
            let header = "HTTP/1.1 206 Partial Content\r\n"
                + "Content-Range: bytes \(start)-\(last)/\(totalSize)\r\n"
                + "Content-Length: \(length)\r\n"
                + "Accept-Ranges: bytes\r\n"
                + "Connection: keep-alive\r\n\r\n"
            guard writeAll(fd, Array(header.utf8)) else { return false }

            let (piece, sleepUs): (Int, useconds_t) = {
                switch pace(start, attempt) {
                case .fast: return (256 * 1024, 0)
                case .drip(let bytes, let everyUs): return (bytes, everyUs)
                }
            }()
            let body = [UInt8](repeating: 0x55, count: piece)
            var sent: Int64 = 0
            while sent < length {
                if stopped { return false }
                let n = Int(min(Int64(piece), length - sent))
                guard writeAll(fd, Array(body[0..<n])) else { return false }
                sent += Int64(n)
                if sleepUs > 0 { usleep(sleepUs) }
            }
            return true
        }

        private func writeAll(_ fd: Int32, _ bytes: [UInt8]) -> Bool {
            var off = 0
            while off < bytes.count {
                let n = bytes[off...].withUnsafeBytes { write(fd, $0.baseAddress, $0.count) }
                guard n > 0 else { return false }
                off += n
            }
            return true
        }
    }

    /// Reads on a detached thread so the blocking read never runs on the test's own thread.
    private static func readInBackground(_ reader: AVIOReader, bytes target: Int,
                                         sliceCap: Int) async -> Int {
        await withCheckedContinuation { continuation in
            Thread.detachNewThread {
                let buf = UnsafeMutablePointer<UInt8>.allocate(capacity: sliceCap)
                defer { buf.deallocate() }
                var got = 0
                let stopAt = Date().addingTimeInterval(60)
                while got < target && Date() < stopAt {
                    let n = reader.read(into: buf, size: Int32(min(sliceCap, target - got)))
                    if n <= 0 { break }
                    got += Int(n)
                }
                continuation.resume(returning: got)
            }
        }
    }

    private static func makeReader(_ origin: PacedOrigin, held: Bool = false) -> AVIOReader {
        AVIOReader(url: URL(string: "http://127.0.0.1:\(origin.port)/movie.bin")!,
                   boundedInitialFetch: held ? nil : firstRange,
                   connStallTimeout: stallTimeout, heldConnection: held, backoffScale: 0.02)
    }

    // MARK: - Detection

    @Test("a connection trickling far below the floor while a read waits is ended and replaced",
          .timeLimit(.minutes(1)))
    func tricklingConnectionIsReplaced() async throws {
        let dripOffset = Self.dripOffset
        // 4 KB every 0.2 s is 10 KB per 0.5 s period. The scaled floor at 0.5 s is 2 MiB / 40,
        // about 52 KB, so this is a fifth of it, and every drip lands well inside the gap timeout.
        let originMaybe = PacedOrigin(totalSize: Self.totalSize, pace: { start, attempt in
            start == dripOffset && attempt == 1 ? .drip(bytes: 4 * 1024, everyUs: 200_000) : .fast
        })
        let origin = try #require(originMaybe)
        defer { origin.stop() }
        let reader = Self.makeReader(origin)
        defer { reader.markClosed(); reader.close() }
        try reader.open()
        try await waitFor { !reader.hasLiveConnectionForTesting }

        // Past the first range: this read drains the window, puts the refill at `dripOffset` on the
        // wire, and then blocks on it.
        let target = Int(Self.firstRange) + 1024 * 1024
        async let got = Self.readInBackground(reader, bytes: target, sliceCap: 256 * 1024)
        try await waitFor { origin.requests.contains { $0.start == dripOffset } }
        let dripRequest = origin.requests.first { $0.start == dripOffset }
        let dripStartedAt = try #require(dripRequest?.at)

        try await waitFor { reader.trickleEndCountForTesting > 0 }
        let detectedAfter = Date().timeIntervalSince(dripStartedAt)
        #expect(detectedAfter < 3 * Self.stallTimeout,
                "the trickle was ended \(detectedAfter)s after it started, over 3x the \(Self.stallTimeout)s threshold")

        // The replacement is asked for where the drip left off and served at full rate.
        let gotBytes = await got
        #expect(gotBytes == target, "the read did not complete after the trickle was replaced")
        let replacements = origin.requests.filter { $0.start > dripOffset && $0.start < dripOffset + 1024 * 1024 }
        #expect(!replacements.isEmpty, "no replacement request after the trickle end: \(origin.requests.map(\.start))")
    }

    // MARK: - No false positives

    @Test("a healthy stream a long read waits on is never ended as a trickle",
          .timeLimit(.minutes(1)))
    func healthyStreamIsLeftAlone() async throws {
        let dripOffset = Self.dripOffset
        // 64 KB every 20 ms, about 3 MB/s: well above the floor, and slow enough that one 4 MB read
        // waits several stall periods, which is the state the verdict is evaluated in.
        let originMaybe = PacedOrigin(totalSize: Self.totalSize, pace: { start, _ in
            start == dripOffset ? .drip(bytes: 64 * 1024, everyUs: 20_000) : .fast
        })
        let origin = try #require(originMaybe)
        defer { origin.stop() }
        let reader = Self.makeReader(origin)
        defer { reader.markClosed(); reader.close() }
        try reader.open()
        try await waitFor { !reader.hasLiveConnectionForTesting }

        let target = Int(Self.firstRange) + 6 * 1024 * 1024
        let got = await Self.readInBackground(reader, bytes: target, sliceCap: 4 * 1024 * 1024)
        #expect(got == target)
        #expect(reader.trickleEndCountForTesting == 0, "a healthy stream was ended as a trickle")
        #expect(origin.requests.filter { $0.start == dripOffset }.count == 1)
        #expect(!origin.requests.contains { $0.start > dripOffset && $0.start < dripOffset + 6 * 1024 * 1024 },
                "the healthy stream was replaced: \(origin.requests.map(\.start))")
    }

    @Test("a trickle with no read outstanding is not judged", .timeLimit(.minutes(1)))
    func trickleWithoutAWaitingReadIsLeftAlone() async throws {
        let dripOffset = Self.dripOffset
        let originMaybe = PacedOrigin(totalSize: Self.totalSize, pace: { start, _ in
            start == dripOffset ? .drip(bytes: 4 * 1024, everyUs: 200_000) : .fast
        })
        let origin = try #require(originMaybe)
        defer { origin.stop() }
        let reader = Self.makeReader(origin)
        defer { reader.markClosed(); reader.close() }
        try reader.open()
        try await waitFor { !reader.hasLiveConnectionForTesting }

        // Draw just enough to put the refill on the wire, then park the consumer: a paused player.
        // The window still holds read-ahead, so no read ever blocks on the drip.
        let drawn = await Self.readInBackground(reader, bytes: 64 * 1024, sliceCap: 64 * 1024)
        #expect(drawn == 64 * 1024)
        try await waitFor { origin.requests.contains { $0.start == dripOffset } }

        let ended = try await waitFor(upTo: .seconds(8 * Self.stallTimeout)) {
            reader.trickleEndCountForTesting > 0 || !reader.hasLiveConnectionForTesting
        }
        #expect(!ended, "a drip nobody was waiting on was ended")
        #expect(reader.trickleEndCountForTesting == 0)
    }

    @Test("a held connection parked on a full window with zero delivery is not judged",
          .timeLimit(.minutes(1)))
    func heldFullWindowIsLeftAlone() async throws {
        let originMaybe = PacedOrigin(totalSize: Self.totalSize, pace: { _, _ in .fast })
        let origin = try #require(originMaybe)
        defer { origin.stop() }
        let reader = Self.makeReader(origin, held: true)
        defer { reader.markClosed(); reader.close() }
        reader.playIntentProvider = { true }
        try reader.open()
        // The window fills at loopback speed; from then on the pump is refused a budget and nothing
        // is delivered at all, and no read is outstanding.
        try await waitFor { reader.windowDiagnostics.aheadBytes >= 15 * 1024 * 1024 }

        let ended = try await waitFor(upTo: .seconds(8 * Self.stallTimeout)) {
            reader.trickleEndCountForTesting > 0 || !reader.hasLiveConnectionForTesting
        }
        #expect(!ended, "a parked held connection was ended")
        #expect(reader.trickleEndCountForTesting == 0)
        #expect(origin.requests.filter { $0.start == 0 }.count == 1,
                "the park re-requested: \(origin.requests.map(\.start))")
    }
}
