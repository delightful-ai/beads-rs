# bd-ub8m Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to execute this plan task-by-task.

**Goal:** Replace raw `last_indexed_offset` replay starts with a validated WAL cursor proof so catch-up replay rejects header-before and out-of-bounds persisted offsets before scan entry.

**Architecture:** Introduce a two-layer cursor model. `WalCursorOffset` replaces raw `u64` for persisted index offsets (`SegmentRow`), and a replay-only `ReplayStartCursor` proof is required to call `scan_segment`. The proof constructor validates persisted cursor bounds against the concrete segment header/file bounds and fails fast on invalid persisted state.

**Tech Stack:** Rust, `beads-daemon-core` WAL domain/replay/index, SQLite + memory WAL index backends, existing replay/fsck/admin/integration tests.

---

## Locked Decisions

- Hard cutover only. No compatibility branch, no feature flag, no dual raw-offset path.
- `scan_segment` will no longer accept raw `u64`; only validated `ReplayStartCursor`.
- Persisted segment offsets remain stored as `INTEGER` in SQLite schema; type-safety is in Rust domain model (`WalCursorOffset`), not schema migration.
- Keep `catch_up_index` and `rebuild_index` public function signatures unchanged.
- Preserve `WalReplayError::IndexOffsetInvalid` as the rejection surface, but include enough context to distinguish header-before vs beyond-end failures.
- Reject invalid persisted offsets before any scan I/O starts.

## Exact File List

- Modify: `crates/beads-daemon-core/src/wal/mod.rs`
- Modify: `crates/beads-daemon-core/src/wal/replay.rs`
- Modify: `crates/beads-daemon-core/src/wal/index.rs`
- Modify: `crates/beads-daemon-core/src/wal/memory_index.rs`
- Modify: `crates/beads-daemon-core/src/wal/contract.rs`
- Modify: `crates/beads-daemon-core/src/wal/fsck.rs`
- Modify: `crates/beads-daemon/src/runtime/core/repl_ingest.rs`
- Modify: `crates/beads-daemon/src/runtime/executor.rs`
- Modify: `crates/beads-daemon/src/runtime/admin/reporting.rs`
- Modify: `crates/beads-rs/tests/integration/wal/index.rs`
- Modify: `crates/beads-rs/tests/integration/wal/fsck.rs`

## Exact API / Type Changes

### 1) WAL domain type hard cutover (`wal/mod.rs`)

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WalCursorOffset(u64);

impl WalCursorOffset {
    pub const fn new(offset: u64) -> Self;
    pub const fn get(self) -> u64;
}
```

```rust
pub enum SegmentRow {
    Open {
        namespace: NamespaceId,
        segment_id: SegmentId,
        segment_path: PathBuf,
        created_at_ms: u64,
        last_indexed_offset: WalCursorOffset,
    },
    Sealed {
        namespace: NamespaceId,
        segment_id: SegmentId,
        segment_path: PathBuf,
        created_at_ms: u64,
        last_indexed_offset: WalCursorOffset,
        final_len: u64,
    },
}

impl SegmentRow {
    pub fn open(
        namespace: NamespaceId,
        segment_id: SegmentId,
        segment_path: PathBuf,
        created_at_ms: u64,
        last_indexed_offset: WalCursorOffset,
    ) -> Self;

    pub fn sealed(
        namespace: NamespaceId,
        segment_id: SegmentId,
        segment_path: PathBuf,
        created_at_ms: u64,
        last_indexed_offset: WalCursorOffset,
        final_len: u64,
    ) -> Self;

    pub fn last_indexed_offset(&self) -> WalCursorOffset;
}
```

### 2) Replay proof type and scan boundary hard cutover (`wal/replay.rs`)

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReplayStartCursor {
    offset: u64,
}

impl ReplayStartCursor {
    fn for_rebuild(segment: &SegmentDescriptor<Verified>) -> Self;

    fn for_catch_up(
        segment: &SegmentDescriptor<Verified>,
        persisted: WalCursorOffset,
    ) -> Result<Self, WalReplayError>;

    fn offset(self) -> u64;
}
```

```rust
fn scan_segment<F>(
    segment: &SegmentDescriptor<Verified>,
    start: ReplayStartCursor,
    max_record_bytes: usize,
    limits: &Limits,
    repair_tail: bool,
    on_record: F,
) -> Result<SegmentScanOutcome, WalReplayError>
where
    F: FnMut(u64, &VerifiedRecord, u32) -> Result<(), WalReplayError>;
```

```rust
struct SegmentScanOutcome {
    last_indexed_offset: WalCursorOffset,
    records: usize,
    truncated: bool,
    truncated_from_offset: Option<u64>,
}
```

### 3) Replay offset error payload shape

```rust
WalReplayError::IndexOffsetInvalid {
    path: PathBuf,
    offset: u64,
    header_len: u64,
    len: u64,
}
```

- `offset < header_len` means persisted cursor is header-before.
- `offset > len` means persisted cursor is out-of-bounds.

### 4) Cross-boundary updates required by `WalCursorOffset`

- `wal/index.rs`: decode `last_indexed_offset` from SQLite as `WalCursorOffset::new(...)`; encode with `.get()`.
- `wal/memory_index.rs`: unchanged storage semantics; type updates only.
- Runtime writers (`executor.rs`, `repl_ingest.rs`): wrap computed offsets with `WalCursorOffset::new(...)`.
- Admin/fsck/reporting/tests: read offset via `.last_indexed_offset().get()`.

## Replay Flow Cutover

### Before

- Catch-up computes `start_offset: u64` from raw persisted row.
- Replay performs inline raw checks.
- `scan_segment` accepts raw `u64`.

### After

- Catch-up computes `ReplayStartCursor`:
  - Rebuild: `ReplayStartCursor::for_rebuild(&segment)`.
  - Catch-up with row: `ReplayStartCursor::for_catch_up(&segment, row.last_indexed_offset())?`.
  - Catch-up without row: rebuild constructor.
- Bounds validation happens in `ReplayStartCursor::for_catch_up`.
- `scan_segment` can only be called with proof type.
- Inline raw `start_offset` checks are removed.

## Implementation Tasks

### Task 1: Introduce typed persisted cursor in WAL domain

1. Add `WalCursorOffset` in `wal/mod.rs`.
2. Update `SegmentRow` fields/constructors/accessors to use `WalCursorOffset`.
3. Compile-fix all direct `SegmentRow` constructors/pattern matches.

### Task 2: Refactor replay entry to proof-based cursor validation

1. Add `ReplayStartCursor` in `wal/replay.rs`.
2. Implement `for_rebuild` and `for_catch_up` constructors with strict bounds checks.
3. Change replay loop to build proof type before scan call.
4. Change `scan_segment` signature to accept proof type and remove raw check block.
5. Return `WalCursorOffset` from scan outcome and thread into `build_segment_row`.

### Task 3: Update index backends and callsites for new cursor type

1. SQLite reader/writer path: map `INTEGER` <-> `WalCursorOffset`.
2. Memory index and contract tests: type updates only.
3. Runtime/admin/fsck paths: convert reads/writes to `.get()` / `WalCursorOffset::new(...)`.

### Task 4: Add mandatory replay rejection tests proving no scan entry

1. Add test-only scan-entry counter hook in replay module.
2. Add `catch_up_rejects_persisted_cursor_before_header_without_scan_entry`.
3. Add `catch_up_rejects_persisted_cursor_beyond_segment_len_without_scan_entry`.
4. In both tests:
   - Seed valid WAL segment + index row.
   - Corrupt persisted `last_indexed_offset` in index row.
   - Run `catch_up_index` and assert `WalReplayError::IndexOffsetInvalid`.
   - Assert scan-entry counter remains `0`.

### Task 5: Hard-cutover cleanup

1. Remove any remaining raw replay-start `u64` path into scanner.
2. Ensure there is exactly one path to scanner: validated `ReplayStartCursor`.
3. Keep no compatibility shims.

## Mandatory Tests

- `wal::replay::tests::catch_up_rejects_persisted_cursor_before_header_without_scan_entry`
  - Persist `last_indexed_offset` lower than `segment.header_len`.
  - Expect `WalReplayError::IndexOffsetInvalid`.
  - Assert scan was never entered (`scan_entry_count == 0`).

- `wal::replay::tests::catch_up_rejects_persisted_cursor_beyond_segment_len_without_scan_entry`
  - Persist `last_indexed_offset` greater than `segment.file_len`.
  - Expect `WalReplayError::IndexOffsetInvalid`.
  - Assert scan was never entered (`scan_entry_count == 0`).

- Regression coverage updates required after type cutover:
  - WAL contract tests compile and pass with `WalCursorOffset`.
  - Integration WAL index/fsck tests compile and pass with `WalCursorOffset`.

## Verification Commands

```bash
# Targeted cursor-proof replay tests
cargo test -p beads-daemon-core wal::replay::tests::catch_up_rejects_persisted_cursor_before_header_without_scan_entry
cargo test -p beads-daemon-core wal::replay::tests::catch_up_rejects_persisted_cursor_beyond_segment_len_without_scan_entry
cargo test -p beads-daemon-core wal::replay::tests

# Full required gate
cargo check
cargo fmt --all
just dylint
cargo clippy --all-features -- -D warnings
cargo test
cargo test --features slow-tests
```

## Risks and Mitigations

- Risk: Type cutover touches many `SegmentRow` callsites and tests.
- Mitigation: Keep semantic behavior unchanged outside replay start validation; rely on compile-driven refactor.

- Risk: Replay error plumbing changes could alter user-facing diagnostics.
- Mitigation: Keep `IndexOffsetInvalid` variant and mapping to `IndexCorrupt` behavior, only adding explicit bounds context.

- Risk: Test-only scan-entry hook leaks into non-test builds.
- Mitigation: Gate hook and counter under `#[cfg(test)]` only.

## Done Checklist

- [ ] `WalCursorOffset` replaces raw `u64` in `SegmentRow`.
- [ ] Replay scan entry requires `ReplayStartCursor` proof (no raw `u64` scanner entry).
- [ ] Header-before persisted offsets are rejected before scan entry.
- [ ] Out-of-bounds persisted offsets are rejected before scan entry.
- [ ] Two mandatory replay tests assert rejection plus zero scan-entry count.
- [ ] SQLite and memory index backends compile/pass with cursor type.
- [ ] Runtime/admin/fsck/integration callsites updated for cursor type.
- [ ] Hard cutover complete; no compatibility layer remains.
- [ ] Full verification gate passes.
