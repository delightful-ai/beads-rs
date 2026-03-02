# bd-ooe2 Implementation Plan

**Goal:** Make `catch_up_index` persist replay rows, segment offsets, and frontier state atomically so crash boundaries cannot leave divergence.
**Architecture:** Replace split catch-up writes with a typed atomic replay-commit path in `wal/replay.rs`. All catch-up row writes, `segments` upserts, and frontier updates flow through one typed commit object and one transaction.
**Tech Stack:** Rust, `beads-daemon-core` WAL replay, `WalIndex` transactions, SQLite/memory index backends.

## Locked Decisions

- Hard cutover for catch-up replay only. No feature flag, no dual path, no compatibility mode.
- Remove per-segment commits from catch-up.
- Keep public `catch_up_index` and `rebuild_index` signatures unchanged.
- Keep `WalIndex` trait surface unchanged unless implementation proves impossible.
- Keep typed atomic commit design for catch-up (`ReplayAtomicCatchUpCommit` + typed frontier update payload).
- Add deterministic crash injection in replay tests to validate atomic rollback and retry behavior.
- Acceptance bar: impossible to persist rows, segment offsets, or frontier independently.

## Exact File List

- Modify: `crates/beads-daemon-core/src/wal/replay.rs`

## API and Type Changes

- Public API changes: none.
- Add internal type `ReplayFrontierUpdate { namespace, origin, watermarks: WatermarkPair, next_seq: Seq1 }`.
- Add internal typed wrapper `ReplayAtomicCatchUpCommit` around `Box<dyn WalIndexTxn>`.
- Add test-only enum `ReplayAtomicCommitStage` with `AfterRowsBeforeFrontier` and `AfterFrontierBeforeCommit`.
- Change internal `index_record(...)` to accept the typed commit wrapper instead of raw `&mut dyn WalIndexTxn`.
- Add helper(s) to build/apply typed frontier updates from `ReplayTracker`.

## Crash-Test Snapshot Contract (Explicit)

Every crash test must capture a pre-state snapshot before invoking `catch_up_index`, then compare post-crash state against it.

Snapshot fields:
- Replay rows visible from `iter_from` for the target `(namespace, origin)`.
- Frontier row from `load_watermarks` for the target `(namespace, origin)`.
- Next sequence probe via `begin_txn -> next_origin_seq -> rollback`.
- `segments` offsets map for namespace: `segment_id -> last_indexed_offset` from `list_segments`.

Required crash equality assertion:
- Post-crash snapshot must equal pre-state snapshot for all fields above, including exact equality of every `segments.last_indexed_offset` entry.

Required success advancement assertion:
- On successful catch-up, replay rows/frontier advance and the touched segment’s `last_indexed_offset` advances beyond pre-state.

## Transaction Boundaries (Current vs Target)

### Current catch-up boundary (remove)

1. Per segment: begin txn -> write event/client/HLC rows + segment row -> commit.
2. End of replay: begin txn -> write watermarks + next-origin-seq -> commit.
3. Crash between boundaries can leave rows/segment offsets durable while frontier is stale.

### Target catch-up boundary (hard cutover)

1. Begin one transaction before catch-up segment scanning starts.
2. During scanning, write all replay rows and segment rows via `ReplayAtomicCatchUpCommit`.
3. Build typed `ReplayFrontierUpdate` values from tracker state and apply them in the same transaction.
4. Commit once at catch-up end.
5. Any error/panic before commit drops txn and rolls back rows, segment offsets, and frontier together.

## Implementation Tasks

### Task 1: Add failing atomicity tests first (with snapshot helpers)

**Files:**
- Modify: `crates/beads-daemon-core/src/wal/replay.rs` (test module)

**Steps:**
1. Add fixture helper that seeds seq `1..2` in index, appends seq `3` to WAL, and records target namespace/origin.
2. Add snapshot helper that captures:
   - `iter_from` rows,
   - `load_watermarks` row,
   - `next_origin_seq` rollback probe,
   - `list_segments` `segment_id -> last_indexed_offset`.
3. Add `catch_up_atomic_crash_after_rows_before_frontier_rolls_back_all`.
4. Add `catch_up_atomic_crash_after_frontier_before_commit_rolls_back_all`.
5. Add `catch_up_atomic_success_commits_rows_segments_and_frontier_together`.
6. Run targeted tests and confirm they fail before refactor.

### Task 2: Enforce crash test protocol including retry

**Files:**
- Modify: `crates/beads-daemon-core/src/wal/replay.rs` (test module)

**Steps (for each crash-injection test):**
1. Capture pre-state snapshot.
2. Enable injection stage (`AfterRowsBeforeFrontier` or `AfterFrontierBeforeCommit`).
3. Run `catch_up_index` and assert injected failure.
4. Capture post-crash snapshot.
5. Assert strict pre/post equality for rows, watermark, next-seq probe, and `segments.last_indexed_offset` map.
6. Disable injection.
7. Rerun `catch_up_index` (post-crash retry).
8. Assert seq `3` now appears.
9. Assert frontier advanced via `load_watermarks`.
10. Assert `next_origin_seq` probe advanced correctly (returns seq `4` after retry success).
11. Assert touched segment `last_indexed_offset` advanced from pre-state.

### Task 3: Introduce typed atomic catch-up commit path

**Files:**
- Modify: `crates/beads-daemon-core/src/wal/replay.rs`

**Steps:**
1. Add `ReplayAtomicCatchUpCommit` methods for replay row writes, segment upserts, frontier application, and single commit.
2. Add test-only crash stage hook calls after rows and after frontier, before commit.
3. Keep rebuild path unchanged unless reuse is mechanical and low risk.

### Task 4: Refactor catch-up flow to one commit boundary

**Files:**
- Modify: `crates/beads-daemon-core/src/wal/replay.rs`

**Steps:**
1. In `ReplayMode::CatchUp`, open one typed commit wrapper before segment loop.
2. Route scan callback writes and segment upserts through the wrapper.
3. Compute/apply typed frontier updates in the same transaction.
4. Remove old catch-up per-segment `txn.commit()` calls.
5. Remove old standalone frontier-only txn for catch-up.
6. Keep replay stats behavior unchanged.

### Task 5: Cleanup and hard-cutover validation

**Files:**
- Modify: `crates/beads-daemon-core/src/wal/replay.rs`

**Steps:**
1. Delete obsolete split-commit catch-up code and dead helpers.
2. Re-run targeted tests and full gate.
3. Confirm no compatibility path remains.

## Mandatory Crash-Injection / Atomicity Tests

1. `catch_up_atomic_crash_after_rows_before_frontier_rolls_back_all`
- Inject at `AfterRowsBeforeFrontier`.
- Assert pre/post crash snapshot equality including exact `segments.last_indexed_offset` equality.
- Disable injection, retry catch-up, assert seq `3` appears and frontier/next-seq/segment offset advance.

2. `catch_up_atomic_crash_after_frontier_before_commit_rolls_back_all`
- Inject at `AfterFrontierBeforeCommit`.
- Assert pre/post crash snapshot equality including exact `segments.last_indexed_offset` equality.
- Disable injection, retry catch-up, assert seq `3` appears and frontier/next-seq/segment offset advance.

3. `catch_up_atomic_success_commits_rows_segments_and_frontier_together`
- No injection.
- Assert rows advance (seq `3` present), frontier advances (`load_watermarks`), next-seq advances (`next_origin_seq` probe returns `4`), and touched segment `last_indexed_offset` advances with them.

## Verification Commands

```bash
# Targeted replay atomicity tests
cargo test -p beads-daemon-core wal::replay::tests::catch_up_atomic_crash_after_rows_before_frontier_rolls_back_all
cargo test -p beads-daemon-core wal::replay::tests::catch_up_atomic_crash_after_frontier_before_commit_rolls_back_all
cargo test -p beads-daemon-core wal::replay::tests::catch_up_atomic_success_commits_rows_segments_and_frontier_together

# Replay module sanity
cargo test -p beads-daemon-core wal::replay::tests

# Full required gate
cargo fmt --all
just dylint
cargo clippy --all-features -- -D warnings
cargo test
```

## Hard-Cutover Assumptions

- Catch-up replay runs during store open before normal write traffic.
- `WalIndexTxn` rollback-on-drop remains valid in SQLite and memory backends.
- No compatibility behavior is required for old catch-up commit layout.
- This bead changes commit atomicity only; no data model migration is required.

## Risks and Mitigations

- Risk: single catch-up txn can be larger and hold write lock longer.
- Mitigation: scope change to catch-up only; monitor startup latency in tests.
- Risk: crash hook leakage into non-test builds.
- Mitigation: gate crash injection with `#[cfg(test)]` only.
- Risk: retry assertions become flaky if fixture is not deterministic.
- Mitigation: fixed namespace/origin and deterministic WAL seed with explicit pre/post snapshots.

## Done Checklist

- [ ] Catch-up replay writes rows + segment rows + frontier in one txn.
- [ ] Old catch-up split transaction path is removed.
- [ ] Typed atomic commit path in `replay.rs` is the only catch-up write path.
- [ ] Crash tests assert exact rollback of `segments.last_indexed_offset` (pre-state snapshot equals post-crash snapshot).
- [ ] Crash tests include post-crash retry (disable injection, rerun catch-up, seq `3` appears, `load_watermarks` + `next_origin_seq` advance correctly).
- [ ] Success test asserts rows/frontier advancement and segment offset advancement together.
- [ ] Replay test module passes.
- [ ] `cargo fmt --all`, `just dylint`, `cargo clippy --all-features -- -D warnings`, and `cargo test` pass.
