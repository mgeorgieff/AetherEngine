import Foundation

/// The outputs every `AetherEngine` in a process shares, and who is still using them (Sodalite#175).
///
/// The audio session, its channel preference and the panel's display criteria are process-wide, and
/// each engine used to act on them as if it were alone. Measured on an Apple TV with two live tiles:
/// every stop that released the session paused the OTHER engine within 10 ms, picture and all, 4 of 4.
/// Engines join here when a load starts and leave on a final teardown; the decisions below are the
/// ones that need to know whether anybody is left.
@MainActor
final class SharedOutputCoordinator {
    static let shared = SharedOutputCoordinator()

    struct Member {
        var role: SharedOutputRole
        var tag: String?
        var sourceChannels: Int?
        /// Weak, so an engine released without a final `stop()` does not stay a member forever.
        weak var owner: AnyObject?
        let tracksOwner: Bool
    }

    enum LeaveOutcome: Equatable, Sendable {
        case notMember
        case othersRemain(Int)
        case lastOut(releaseSession: Bool)
    }

    private var members: [ObjectIdentifier: Member] = [:]
    /// Set by any leaver that asked for the session to be released, paid by the last one out. Without
    /// it, the engine that opted in could leave first and the one that did not would close the round.
    private var releaseOwed = false
    /// Criteria writers that stopped while others still played. Keyed by engine, so one that loads
    /// again as `.primary` drops its own reset instead of having it wipe the criteria it has just written.
    /// The owner is held weakly beside it: a host that builds an engine per item releases the old one,
    /// and the next engine can be allocated at the same address, so the identifier alone made a NEW
    /// engine's join drop the reset of the one before it.
    private struct DeferredReset {
        weak var owner: AnyObject?
        let tracksOwner: Bool
        let label: String
        let run: @MainActor () -> Void
    }
    private var deferredCriteriaResets: [ObjectIdentifier: DeferredReset] = [:]
    private var lastTransition: Task<Void, Never>?
    private var categoryConfiguration: Task<Bool, Never>?
    private var categoryConfigured = false

    init() {}

    var isEmpty: Bool {
        pruneReleasedOwners()
        return members.isEmpty
    }

    var preferredSourceChannels: Int? {
        pruneReleasedOwners()
        return members.values.compactMap(\.sourceChannels).max()
    }

    /// Drops members whose engine was deallocated without a `stop()`, and leaves the owed release to the
    /// next `leave`. Deferred criteria resets run here once nobody is left: they were waiting for the last
    /// engine out, and a released engine never calls `leave`, so without this they waited for an engine
    /// that no longer existed (until some later round's last-out ran them over that round's criteria).
    private func pruneReleasedOwners() {
        let gone = members.filter { $0.value.tracksOwner && $0.value.owner == nil }
        guard !gone.isEmpty else { return }
        for (id, member) in gone {
            members[id] = nil
            if let dropped = deferredCriteriaResets.removeValue(forKey: id) {
                EngineLog.emit("[SharedOutput] deferred criteria reset of \(dropped.label) dropped: its engine was released", category: .engine)
            }
            EngineLog.emit("[SharedOutput] \(member.tag ?? "engine-\(UInt(bitPattern: id.hashValue) % 10_000)") was released without stop(), dropped", category: .engine)
        }
        if members.isEmpty { runDeferredCriteriaResets(reason: "the last other engine was released without stop()") }
    }

    private func runDeferredCriteriaResets(reason: String) {
        guard !deferredCriteriaResets.isEmpty else { return }
        let resets = deferredCriteriaResets.values
        deferredCriteriaResets.removeAll()
        EngineLog.emit(
            "[SharedOutput] running \(resets.count) deferred criteria reset(s) (\(resets.map(\.label).sorted().joined(separator: ", "))): \(reason)",
            category: .engine)
        resets.forEach { $0.run() }
    }

    func join(_ id: ObjectIdentifier, role: SharedOutputRole, tag: String?, owner: AnyObject? = nil) {
        let channels = members[id]?.sourceChannels
        members[id] = Member(role: role, tag: tag, sourceChannels: channels, owner: owner, tracksOwner: owner != nil)
        // A secondary never writes criteria the pending reset could wipe, so it keeps the reset owed. Only
        // the engine that deferred it may drop it; another one at a reused address keeps it owed.
        if role == .primary, let pending = deferredCriteriaResets[id],
           !pending.tracksOwner || (owner != nil && pending.owner === owner) {
            deferredCriteriaResets[id] = nil
            EngineLog.emit("[SharedOutput] deferred criteria reset of \(pending.label) dropped: it loads again as primary", category: .engine)
        }
    }

    func noteSourceChannels(_ channels: Int?, for id: ObjectIdentifier) {
        guard members[id] != nil else { return }
        members[id]?.sourceChannels = channels
    }

    func leave(_ id: ObjectIdentifier, releasesSession: Bool) -> LeaveOutcome {
        pruneReleasedOwners()
        guard members.removeValue(forKey: id) != nil else { return .notMember }
        if releasesSession { releaseOwed = true }
        guard members.isEmpty else { return .othersRemain(members.count) }
        let release = releaseOwed
        releaseOwed = false
        runDeferredCriteriaResets(reason: "last engine out")
        return .lastOut(releaseSession: release)
    }

    func othersActive(besides id: ObjectIdentifier) -> Bool {
        !otherMemberLabels(besides: id).isEmpty
    }

    /// The members other than `id`, by log label, so a deferral can say who it waits for.
    func otherMemberLabels(besides id: ObjectIdentifier) -> [String] {
        pruneReleasedOwners()
        return members.filter { $0.key != id }.map { label(for: $0.key) }.sorted()
    }

    /// Defers a criteria reset to run when the last engine leaves, unless nobody else is playing now.
    /// If `id` is alone or not a member, the reset runs immediately.
    func deferCriteriaReset(for id: ObjectIdentifier, owner: AnyObject? = nil, label: String? = nil,
                            _ body: @escaping @MainActor () -> Void) {
        guard othersActive(besides: id) else { body(); return }
        deferredCriteriaResets[id] = DeferredReset(
            owner: owner, tracksOwner: owner != nil,
            label: label ?? "engine-\(UInt(bitPattern: id.hashValue) % 10_000)", run: body)
    }

    /// One queue for every engine's activation and release. AE#538 ordered them per engine; with two
    /// engines the release of one and the activation of the other had no order between them.
    func enqueueTransition(_ body: @escaping @Sendable () async -> Void) -> Task<Void, Never> {
        let previous = lastTransition
        let transition = BlockingWork.detached(priority: .userInitiated) {
            await previous?.value
            await body()
        }
        lastTransition = transition
        return transition
    }

    /// The category belongs to the process-wide session: concurrent loads share the in-flight
    /// declaration, a successful one is never repeated, a failed one is retried by the next load.
    /// It runs on the activation/release queue, so it is ordered with both.
    func configureAudioSessionCategory(_ body: @escaping @Sendable () async -> Bool) async {
        if categoryConfigured { return }
        if let categoryConfiguration {
            _ = await categoryConfiguration.value
            return
        }
        let previous = lastTransition
        let configuration = BlockingWork.detached(priority: .userInitiated) { () async -> Bool in
            await previous?.value
            return await body()
        }
        lastTransition = BlockingWork.detached(priority: .userInitiated) { _ = await configuration.value }
        categoryConfiguration = configuration
        let succeeded = await configuration.value
        if succeeded { categoryConfigured = true }
        categoryConfiguration = nil
    }

    /// Log label for an engine: its host-given tag, or the identifier's hash when there is none.
    func label(for id: ObjectIdentifier) -> String {
        members[id]?.tag ?? "engine-\(UInt(bitPattern: id.hashValue) % 10_000)"
    }
}
