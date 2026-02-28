# bd-jzxt Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make WAL/meta durability side effects explicit in mutation paths so no fsync-capable seam appears pure.

**Architecture:** Hard cutover from “pure-looking” seam returns (`AppendOutcome`, `Dot`) to explicit pending/acknowledged durability-effect boundary types. Local mutation and repl-ingest callsites must explicitly acknowledge durability effects before they can access append/planning results.

**Tech Stack:** Rust, `beads-daemon-core` WAL seams, `beads-daemon` runtime mutation engine/executor/repl ingest, compile-fail doctests, existing runtime/integration durability tests.

---

## Hard-Cutover Decisions

1. No backward compatibility layer. Old seam signatures are removed in one change.
2. `WalAppend` no longer returns `AppendOutcome` directly.
3. `DotAllocator` no longer returns `Dot` directly.
4. `MutationEngine::plan` no longer returns `EventDraft` directly.
5. Compile-time enforcement is done via private fields + explicit `acknowledge_*` transition methods on pending effectful types.
6. Mandatory acceptance scenario is compile-time tested: mutation callsites cannot use pure-looking paths that bypass durability acknowledgement.

## 1) Exact File/Function/Type Signature Changes

1. In [seams.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon-core/src/wal/seams.rs), replace pure append seam with explicit durability boundary types.

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalAppendDurabilityEffect {
    SyncBoundaryCrossed,
}

#[must_use = "must acknowledge WAL durability effects before using append outcome"]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingWalAppend {
    append: AppendOutcome,
    durability: WalAppendDurabilityEffect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcknowledgedWalAppend {
    pub append: AppendOutcome,
    pub durability: WalAppendDurabilityEffect,
}

impl PendingWalAppend {
    pub fn acknowledge_durability(self) -> AcknowledgedWalAppend;
}

pub trait WalAppend {
    fn wal_append(
        &mut self,
        namespace: &NamespaceId,
        record: &VerifiedRecord,
        now_ms: u64,
    ) -> EventWalResult<PendingWalAppend>;
}
```

2. In [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon-core/src/wal/mod.rs), export the new seam types.
   Replace current seam re-export with:
```rust
pub use seams::{
    AcknowledgedWalAppend,
    PendingWalAppend,
    WalAppend,
    WalAppendDurabilityEffect,
    WalIndexTxnProvider,
    WalReadRange,
};
```

3. In [mutation_engine.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/mutation_engine.rs), make planning/dot allocation effectful.

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DotDurabilityEffect {
    StoreMetaSyncBoundary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DotAllocation {
    pub dot: Dot,
    pub durability: DotDurabilityEffect,
}

pub trait DotAllocator {
    fn next_dot(&mut self) -> Result<DotAllocation, OpError>;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PendingPlanningDurabilityEffects {
    pub dot_meta_sync_boundaries: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcknowledgedPlanningDurabilityEffects {
    pub dot_meta_sync_boundaries: u32,
}

#[must_use = "planned mutation has unacknowledged durability effects"]
pub struct PlannedMutation {
    draft: EventDraft,
    durability: PendingPlanningDurabilityEffects,
}

impl PlannedMutation {
    pub fn acknowledge_durability(
        self,
    ) -> (EventDraft, AcknowledgedPlanningDurabilityEffects);
}

pub fn plan(...) -> Result<PlannedMutation, OpError>;
```

4. In [helpers.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/executor/helpers.rs), update `RuntimeDotAllocator` implementation:
   Change `next_dot` return from `Dot` to `DotAllocation`, mapping runtime counter allocation to `DotDurabilityEffect::StoreMetaSyncBoundary`.

5. In [executor.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/executor.rs), update mutation flow callsites:
   Replace direct use of `engine.plan(...) -> EventDraft` with explicit durability acknowledgement:
```rust
let planned = engine.plan(...)?;
let (draft, planning_effects) = planned.acknowledge_durability();
```
   Replace direct use of `wal_append(...) -> AppendOutcome` with explicit acknowledgement:
```rust
let pending = store_runtime.event_wal.wal_append(...)?;
let acknowledged = pending.acknowledge_durability();
let wal_effect = acknowledged.durability;
let append = acknowledged.append;
```
   Keep `planning_effects` and `wal_effect` explicit at the mutation callsite and include them in trace/debug fields.

6. In [repl_ingest.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/repl_ingest.rs), update WAL append usage to the same explicit acknowledgement pattern as executor.

7. In [executor.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/executor.rs) tests and [mutation_engine.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/mutation_engine.rs) tests, update all `DotAllocator` impls and `plan()` callsites to new effectful signatures.

## 2) Explicit Durability Effect Type Propagation at Callsites

Local mutation path propagation:

```text
RuntimeDotAllocator::next_dot()
  -> DotAllocation { dot, durability: DotDurabilityEffect }

MutationEngine::plan(...)
  -> PlannedMutation { draft, PendingPlanningDurabilityEffects }

Executor callsite:
  planned.acknowledge_durability()
  -> (EventDraft, AcknowledgedPlanningDurabilityEffects)

EventWal.wal_append(...)
  -> PendingWalAppend

Executor callsite:
  pending.acknowledge_durability()
  -> AcknowledgedWalAppend { append, WalAppendDurabilityEffect }
```

Repl ingest path propagation:

```text
EventWal.wal_append(...)
  -> PendingWalAppend

repl_ingest callsite:
  pending.acknowledge_durability()
  -> AcknowledgedWalAppend { append, WalAppendDurabilityEffect }
```

## 3) Compile-Time Guarantees

1. Old pure return types are removed (`WalAppend -> AppendOutcome`, `DotAllocator -> Dot`, `plan -> EventDraft`).
2. Pending effectful structs use private fields; append/planning payloads are inaccessible without explicit acknowledge transition.
3. Callsites must invoke `acknowledge_durability()` to obtain usable payloads.
4. `#[must_use]` on pending types prevents accidental drop of unacknowledged effects under lint gates.
5. Compile-fail doctests enforce “no pure-looking fsync-hidden path” at type-check time.

## 4) Tests (including mandatory scenario)

### Mandatory compile-time scenario

1. Add compile-fail doctest in [seams.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon-core/src/wal/seams.rs):
   Attempt to use `PendingWalAppend` payload directly without `acknowledge_durability()` must fail to compile.

2. Add compile-fail doctest in [mutation_engine.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/mutation_engine.rs):
   Attempt to use `PlannedMutation` draft directly without `acknowledge_durability()` must fail to compile.

### Runtime/unit regression updates

1. Update `DotAllocator` test impls in [mutation_engine.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/mutation_engine.rs) and [executor.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/executor.rs) to return `DotAllocation`.
2. Update all `engine.plan(...)` test callsites to acknowledge durability before using draft.
3. Add new unit assertions in [mutation_engine.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/mutation_engine.rs):
   `dot_meta_sync_boundaries > 0` for a dot-allocating request (for example `add_labels`), and `== 0` for a non-dot-allocating request.
4. Keep/update existing mutation + ingest behavior tests:
   `idempotent_retry_reuses_wal_mapping`, `ingest_rotation_seals_previous_segment`, `ingest_uses_canonical_sha_for_index_and_watermarks`, `ingest_accepts_orphan_note_after_append`.
5. Keep integration durability behavior coverage:
   `repl_daemon_replicated_fsync_receipt`, `repl_daemon_replicated_fsync_timeout_receipt`, `crash_recovery_rebuilds_index_after_fsync_before_commit`.

## 5) Verification Commands

```bash
cargo check -p beads-daemon-core --all-features
cargo check -p beads-daemon --all-features

cargo test -p beads-daemon-core --all-features --doc
cargo test -p beads-daemon --all-features --doc

cargo test -p beads-daemon runtime::executor::tests::idempotent_retry_reuses_wal_mapping
cargo test -p beads-daemon runtime::core::tests::ingest_rotation_seals_previous_segment
cargo test -p beads-daemon runtime::core::tests::ingest_uses_canonical_sha_for_index_and_watermarks
cargo test -p beads-daemon runtime::core::tests::ingest_accepts_orphan_note_after_append

cargo test -p beads-rs --features slow-tests repl_daemon_replicated_fsync_receipt
cargo test -p beads-rs --features slow-tests repl_daemon_replicated_fsync_timeout_receipt
cargo test -p beads-rs --features slow-tests crash_recovery_rebuilds_index_after_fsync_before_commit

cargo check
cargo fmt --all
just dylint
cargo clippy --all-features -- -D warnings
cargo test
cargo test --features slow-tests
```

## Risks and Mitigations

1. Risk: Signature cutover ripples through many mutation-engine tests.
   Mitigation: Apply type changes first, then mechanical callsite/test updates in one commit group.

2. Risk: Developers may acknowledge effects but ignore them semantically.
   Mitigation: Require explicit effect binding at callsites and include effect fields in tracing at mutation/repl-ingest boundaries.

3. Risk: Compile-fail doctests can be brittle with visibility/feature gating.
   Mitigation: Keep doctest snippets minimal and colocated with public effect types.

4. Risk: Future optimization bead (`bd-9phx`) could conflict with effect semantics.
   Mitigation: Keep effect enums semantic (“sync boundary crossed”), not implementation-detail-specific (`sync_all` vs `sync_data`).
