# bd-azyx Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make WAL event index rows and watermark durability persist in one atomic commit unit in both mutation and repl-ingest paths, with hard cutover only.

**Architecture:** Introduce one daemon-local typed commit wrapper that owns the `WalIndexTxn` commit boundary and requires a `WatermarkPair` at commit time. Route mutation and repl-ingest indexing through this wrapper, delete split watermark transactions, wire pause hooks so crash tests can actually hang at the atomic-commit boundary, and prove rollback/recovery with failpoint + crash-window tests.

**Tech Stack:** Rust, `beads-daemon` runtime, `beads-daemon-core` WAL index traits, SQLite WAL index, slow integration crash tests.

---

## Locked Decisions

- Hard cutover only. No feature flags, no dual path, no compatibility branch.
- `WalIndexTxn` trait surface in `beads-daemon-core` stays unchanged.
- Enforce atomicity by type in `beads-daemon` with a wrapper that hides `commit()` and only exposes `commit_with_watermarks(...)`.
- Mutation and repl-ingest both use the same atomic commit abstraction.
- Remove old split transaction callsites completely (`txn.commit()` then `watermark_txn.commit()` pattern).
- Keep commit timing relative to WAL append as-is (append/fsync first, then index+watermark atomic commit), then in-memory apply/broadcast flow.
- Crash pause hook must be active in integration harness builds: `maybe_pause` is enabled under `feature = "slow-tests"` **or** `feature = "test-harness"` in `beads-daemon`.

---

## Exact File List

- Create: `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/wal_atomic_commit.rs`
- Modify: `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/mod.rs`
- Modify: `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/executor.rs`
- Modify: `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/repl_ingest.rs`
- Modify: `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/executor/helpers.rs` (imports/signature touch only if needed)
- Modify: `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/test_hooks.rs`
- Modify tests: `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/executor.rs` `#[cfg(test)]` block
- Modify tests: `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/mod.rs` `#[cfg(test)]` block
- Modify slow crash test: `/Users/darin/src/personal/beads-rs/crates/beads-rs/tests/integration/daemon/crash_recovery.rs`

---

## 1) Single Durable Commit Unit (Types/Signatures)

Add new internal type in `wal_atomic_commit.rs`:

```rust
pub(crate) enum AtomicWalCommitPath {
    Mutation,
    ReplIngest,
}

pub(crate) struct AtomicWalDurabilityTxn {
    txn: Box<dyn WalIndexTxn>,
    namespace: NamespaceId,
    origin: ReplicaId,
    path: AtomicWalCommitPath,
}

impl AtomicWalDurabilityTxn {
    pub(crate) fn begin(
        provider: &dyn WalIndexTxnProvider,
        namespace: NamespaceId,
        origin: ReplicaId,
        path: AtomicWalCommitPath,
    ) -> Result<Self, WalIndexError>;

    pub(crate) fn index_mut(&mut self) -> &mut dyn WalIndexTxn;

    pub(crate) fn commit_with_watermarks(
        self,
        watermarks: WatermarkPair,
    ) -> Result<(), WalIndexError>;
}

pub(crate) fn tip_watermark_pair(
    seq: Seq1,
    sha: [u8; 32],
) -> Result<WatermarkPair, WalIndexError>;
```

Implementation rules inside `commit_with_watermarks(...)`:

- Always call `txn.update_watermark(&namespace, &origin, watermarks)` before commit.
- Use per-path pause hook stage names:
- `wal_mutation_before_atomic_commit`
- `wal_repl_ingest_before_atomic_commit`
- Then call `txn.commit()`.
- No alternate commit path exposed.

`runtime/mod.rs` change:

- Register `mod wal_atomic_commit;` (internal module, no public export needed).

---

## 2) Callsite Changes (Mutation + Repl Ingest)

### Mutation path

Touch `Daemon::apply_mutation_request_with_inner` in `executor.rs`:

- Replace raw `let mut txn = wal_index.begin_wal_txn()?` with atomic wrapper begin:
- `AtomicWalDurabilityTxn::begin(..., AtomicWalCommitPath::Mutation)`.
- Route all existing WAL index row writes (`upsert_segment`, `record_event`, `upsert_client_request`, `update_hlc`) through `atomic_txn.index_mut()`.
- Derive commit watermark once from the event tip (`origin_seq`, `sha_bytes`) via `tip_watermark_pair(...)`.
- Remove split boundary entirely:
- Remove `maybe_pause("wal_before_index_commit")`.
- Remove first `txn.commit()`.
- Remove second `watermark_txn` open/update/commit block.
- Add one `atomic_txn.commit_with_watermarks(commit_watermarks)` call.
- Keep post-commit in-memory apply + receipt path intact.

### Repl ingest path

Touch `Daemon::ingest_remote_batch` in `core/repl_ingest.rs`:

- Replace raw `begin_wal_txn()` with `AtomicWalDurabilityTxn::begin(..., AtomicWalCommitPath::ReplIngest)`.
- Keep existing per-event `upsert_segment`, `record_event`, `update_hlc` writes, but call through `atomic_txn.index_mut()`.
- Track batch tip (`last_seq`, `last_canonical_sha`) while iterating.
- Build one `WatermarkPair` from tip via `tip_watermark_pair(...)`.
- Remove split boundary entirely:
- Remove current `txn.commit()`.
- Remove later `watermark_txn` open/update/commit block.
- Add one `atomic_txn.commit_with_watermarks(commit_watermarks)` call.
- Keep current apply/broadcast/watermark-in-memory advancement flow after commit.

---

## 3) Pause-Hook Feature Wiring For Crash Tests

Touch `runtime/test_hooks.rs`:

- Change active hook cfg from `#[cfg(feature = "slow-tests")]` to:
- `#[cfg(any(feature = "slow-tests", feature = "test-harness"))]`
- Change no-op cfg from `#[cfg(not(feature = "slow-tests"))]` to:
- `#[cfg(not(any(feature = "slow-tests", feature = "test-harness")))]`
- Keep env-variable gate behavior and timeout logic unchanged.
- Do not add compatibility branches; this is direct cutover for test hook activation semantics.

Reason: `beads-rs` integration tests build `beads-daemon` with `test-harness`, and crash tests rely on `maybe_pause` to emit marker files before SIGKILL. Without this wiring, the hang stage is silently skipped.

---

## 4) Failpoint/Crash-Window Tests

### Test A: Mutation failpoint rollback (unit)

Add in `executor.rs` test module:

- New test: `mutation_atomic_commit_failpoint_rolls_back_event_and_watermark`.
- Inject failpoint just before atomic commit.
- Execute one create mutation.
- Assert mutation fails.
- Assert index snapshot unchanged for local `(namespace, origin)`:
- no new event row
- no watermark advancement
- no `next_origin_seq` advancement
- Retry without failpoint and assert success with both event row and watermark advanced together.

### Test B: Repl-ingest failpoint rollback (unit)

Add in `core/mod.rs` test module:

- New test: `repl_ingest_atomic_commit_failpoint_rolls_back_event_and_watermark`.
- Build one-event contiguous batch and call `ingest_remote_batch`.
- Inject failpoint just before atomic commit.
- Assert ingest returns error.
- Assert WAL index snapshot unchanged for source `(namespace, origin)`:
- no event row
- no watermark advancement
- no `next_origin_seq` advancement
- Retry same batch without failpoint and assert event + watermark advance together.

### Test C: Real crash-window regression (slow integration)

Update `crash_recovery.rs` mutation crash test:

- Switch hang stage from `wal_before_index_commit` to `wal_mutation_before_atomic_commit`.
- While daemon is killed at pause window (before commit), inspect SQLite index and assert no partial persistence:
- event row absent
- watermark row absent or unchanged baseline
- Restart and assert replay recovery succeeds (`bd show` returns created issue).
- After recovery, assert event row and watermark row are both present and aligned at same seq/head.

---

## 5) Verification Commands

```bash
# Fast compile + focused unit tests
cargo check
cargo test -p beads-daemon mutation_atomic_commit_failpoint_rolls_back_event_and_watermark
cargo test -p beads-daemon repl_ingest_atomic_commit_failpoint_rolls_back_event_and_watermark

# Confirm the crash test target path exists in the integration test binary
cargo test -p beads-rs --features "slow-tests,beads-daemon/slow-tests" --test integration -- --list \
  | rg "daemon::crash_recovery::crash_recovery_rebuilds_index_after_fsync_before_commit"

# Slow crash-window proof (valid cargo target + full integration path)
cargo test -p beads-rs --features "slow-tests,beads-daemon/slow-tests" --test integration \
  daemon::crash_recovery::crash_recovery_rebuilds_index_after_fsync_before_commit -- --exact

# Optional companion crash test
cargo test -p beads-rs --features "slow-tests,beads-daemon/slow-tests" --test integration \
  daemon::crash_recovery::crash_recovery_truncates_tail_and_resets_origin_seq -- --exact
```

```bash
# Hard-cutover checks: fail on any legacy split-transaction symbols in callsites
FILES=(
  /Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/executor.rs
  /Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/repl_ingest.rs
)

for f in "${FILES[@]}"; do
  if rg -n "wal_before_index_commit|watermark_txn" "$f"; then
    echo "legacy split-transaction symbol remains in $f" >&2
    exit 1
  fi
  if rg -n "begin_wal_txn\\(|update_watermark\\(" "$f"; then
    echo "direct wal tx/update_watermark call remains in $f" >&2
    exit 1
  fi
done

# Multiline-safe detector: catches split path even when begin/commit/update are separated by many lines
if rg -nU "begin_wal_txn\\([\\s\\S]{0,4000}update_watermark\\(" "${FILES[@]}"; then
  echo "multiline split transaction path still present" >&2
  exit 1
fi

# Ensure new pause stages are present in atomic wrapper
rg -n "wal_mutation_before_atomic_commit|wal_repl_ingest_before_atomic_commit" \
  /Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/wal_atomic_commit.rs
```

```bash
# Required gate
cargo fmt --all
just dylint
cargo clippy --all-features -- -D warnings
cargo test
cargo test --features slow-tests
```

---

## Risk Notes

- Risk: Watermark derivation from event tip could diverge from in-memory post-apply state if invariants regress.
- Mitigation: Derive from canonical tip deterministically; assert unchanged snapshot on failpoint; retry tests verify convergence.

- Risk: Test-only pause hook logic leaks into production behavior.
- Mitigation: Keep `maybe_pause` env-gated and feature-gated to `slow-tests || test-harness`; production default path remains no-op.

- Risk: Slow crash test timing flakiness around marker wait/kill.
- Mitigation: Reuse existing marker polling/backoff pattern and generous timeout.

- Risk: Missing hard-cutover cleanup leaves alternate split path reachable through multiline code.
- Mitigation: Enforce both symbol-level and multiline `rg -U` checks in verification.

---

## Done Checklist

- [ ] New `AtomicWalDurabilityTxn` exists and is the only commit boundary in mutation/repl-ingest WAL indexing.
- [ ] Mutation path has exactly one index+watermark commit call.
- [ ] Repl-ingest path has exactly one index+watermark commit call.
- [ ] Old split transaction code (`watermark_txn`) removed from both paths.
- [ ] Old stage name `wal_before_index_commit` removed from runtime callsites.
- [ ] `runtime/test_hooks.rs` enables `maybe_pause` under `slow-tests || test-harness`.
- [ ] Mutation failpoint rollback test passes.
- [ ] Repl-ingest failpoint rollback test passes.
- [ ] Slow mutation crash-window test passes via valid target: `--test integration daemon::crash_recovery::crash_recovery_rebuilds_index_after_fsync_before_commit`.
- [ ] Multiline-safe hard-cutover verification passes (no hidden split transaction flow).
- [ ] `cargo fmt --all`, `just dylint`, `cargo clippy --all-features -- -D warnings`, `cargo test`, and `cargo test --features slow-tests` pass.
