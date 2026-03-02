# bd-a3hl Implementation Plan

**Goal:** Reconcile WAL segment index rows as a total set during catch-up so stale rows are deleted and index topology exactly matches filesystem topology after commit.
**Architecture:** Add explicit transaction-level segment set replacement and drive catch-up through that single path. Catch-up will reconcile each namespace to the observed filesystem set (including empty), then commit rows + segment set + frontier atomically.
**Tech Stack:** Rust, `beads-daemon-core`, WAL replay, `WalIndex` traits, SQLite + memory index backends.

## Locked Decisions

- Hard cutover only for catch-up reconciliation. No feature flag, no dual path, no compatibility mode.
- Catch-up segment writes move from per-segment upsert semantics to namespace total-set replacement semantics.
- Rebuild behavior remains unchanged for this bead.
- Reconciliation scope is the union of filesystem namespaces and indexed namespaces, so index-only namespaces are actively cleared.
- Public CLI/API surfaces remain unchanged.

## Exact File List

- Modify: [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon-core/src/wal/mod.rs)
- Modify: [index.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon-core/src/wal/index.rs)
- Modify: [memory_index.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon-core/src/wal/memory_index.rs)
- Modify: [replay.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon-core/src/wal/replay.rs)
- Modify: [contract.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon-core/src/wal/contract.rs)

## Exact API / Interface Changes (Hard Cutover)

```rust
pub trait WalIndexTxn {
    // existing methods...
    fn replace_namespace_segments(
        &mut self,
        ns: &NamespaceId,
        segments: &[SegmentRow],
    ) -> Result<(), WalIndexError>;
}

pub trait WalIndexReader {
    // existing methods...
    fn list_segment_namespaces(&self) -> Result<Vec<NamespaceId>, WalIndexError>;
}
```

### Semantics

- `replace_namespace_segments` is authoritative total-set reconciliation for one namespace in one txn:
  - Delete all existing `segments` rows for `ns`.
  - Insert exactly `segments`.
  - Validate every row in `segments` belongs to `ns`; mismatch is an error.
- `list_segment_namespaces` returns sorted unique namespaces currently present in index `segments`.

### Backend implementation requirements

- SQLite:
  - `list_segment_namespaces`: `SELECT DISTINCT namespace FROM segments ORDER BY namespace`.
  - `replace_namespace_segments`: `DELETE FROM segments WHERE namespace=?`, then insert rows with existing row validation rules.
- Memory index:
  - `list_segment_namespaces`: derive from `state.segments` keys, sorted/deduped.
  - `replace_namespace_segments`: remove all keys for namespace, then insert provided rows.

### Replay call-flow change

- In catch-up mode:
  - Compute namespace set as union of `list_namespaces(wal_dir)` and `reader.list_segment_namespaces()`.
  - For each namespace:
    - Scan filesystem segments and build final `Vec<SegmentRow>` for that namespace.
    - Keep existing offset/sealed checks against preloaded index rows for start offsets.
    - Call `ReplayAtomicCatchUpCommit::replace_namespace_segments(namespace, rows)` once per namespace.
  - Apply frontier updates in the same txn.
  - Commit once.
- Remove catch-up reliance on segment upsert as reconciliation behavior.

## Implementation Tasks

### Task 1: Add trait methods and backend implementations

1. Add `replace_namespace_segments` to `WalIndexTxn` and `list_segment_namespaces` to `WalIndexReader`.
2. Implement both methods in SQLite index backend.
3. Implement both methods in memory index backend.
4. Add replay wrapper method on `ReplayAtomicCatchUpCommit` for namespace replacement.

### Task 2: Refactor catch-up to total-set reconciliation

1. In `replay_index` catch-up path, compute namespace union (filesystem + index).
2. For each namespace, build observed segment rows from filesystem scan results.
3. Replace namespace segment rows via one call to `replace_namespace_segments`.
4. Keep frontier update and commit atomic with reconciliation.
5. Remove remaining catch-up assumptions that upsert-only is sufficient.

### Task 3: Mandatory replay regressions (bug proof)

1. Add test `catch_up_reconcile_deletes_stale_segment_rows_and_matches_filesystem_set`.
2. Add test `catch_up_reconcile_clears_rows_for_index_only_namespace`.
3. Reuse deterministic fixture strategy from existing catch-up atomic tests.

### Task 4: Add index contract coverage for new interface

1. Add `test_segment_replacement_is_total_set` in `wal/contract.rs`.
2. Ensure laws run against both memory and SQLite implementations.
3. Assert replaced set exactly equals provided set and stale rows disappear.

### Task 5: Hard-cutover cleanup and validation

1. Ensure catch-up has only total-set reconciliation path for `segments`.
2. Confirm no compatibility branch remains for upsert-only catch-up reconciliation.
3. Run targeted tests then full gate.

## Mandatory Tests

- `wal::replay::tests::catch_up_reconcile_deletes_stale_segment_rows_and_matches_filesystem_set`
  - Seed stale index segment row not present on disk.
  - Run `catch_up_index`.
  - Assert stale row removed.
  - Assert indexed segment ID set equals filesystem segment ID set for namespace.

- `wal::replay::tests::catch_up_reconcile_clears_rows_for_index_only_namespace`
  - Seed segment rows for a namespace with no filesystem directory.
  - Run `catch_up_index`.
  - Assert namespace has zero segment rows after catch-up.

- `wal::contract::test_segment_replacement_is_total_set`
  - Seed two segments, replace with a different set, assert exact set equality and stale removal.

## Verification Commands

```bash
# Targeted new coverage
cargo test -p beads-daemon-core wal::contract::test_segment_replacement_is_total_set
cargo test -p beads-daemon-core wal::replay::tests::catch_up_reconcile_deletes_stale_segment_rows_and_matches_filesystem_set
cargo test -p beads-daemon-core wal::replay::tests::catch_up_reconcile_clears_rows_for_index_only_namespace
cargo test -p beads-daemon-core wal::replay::tests

# Full gate
cargo check
cargo fmt --all
just dylint
cargo clippy --all-features -- -D warnings
cargo test
cargo test --features slow-tests
```

## Risks and Mitigations

- Risk: Replace-all semantics could drop valid rows if row-collection logic is wrong.
  Mitigation: Mandatory set-equality replay tests and namespace-empty test.
- Risk: Namespace union adds extra index read and catch-up complexity.
  Mitigation: Keep logic local to replay catch-up and add focused tests.
- Risk: Larger txn scope in catch-up.
  Mitigation: Reuse existing atomic catch-up structure and keep rebuild unchanged.

## Done Checklist

- [ ] `WalIndexTxn` supports namespace total-set segment replacement.
- [ ] `WalIndexReader` can list segment namespaces from index.
- [ ] SQLite and memory backends implement new methods with matching semantics.
- [ ] Catch-up reconciles segment rows as total set (not upsert-only).
- [ ] Stale rows are deleted on catch-up.
- [ ] Indexed segment set equals filesystem segment set after catch-up.
- [ ] Index-only namespaces are cleared during catch-up.
- [ ] Contract and replay tests for replacement/reconciliation pass.
- [ ] Full verification gate passes.
