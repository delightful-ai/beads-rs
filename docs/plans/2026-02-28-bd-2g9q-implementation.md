# bd-2g9q Implementation Plan (Revised)

## Goal
Ship the `WatermarkPair` hard cutover and make index-schema bumps safe by ensuring runtime can always reach legacy index rebuild when only `store_meta.index_schema_version` is stale.

## Locked Decisions
- Hard cutover only. No compatibility layer, no dual-read path, no in-place legacy index migration.
- Bump `StoreMetaVersions::INDEX_SCHEMA_VERSION` from `1` to `2`.
- Runtime accepts exactly one stale `store_meta` case: non-index versions match current and `index_schema_version` is older.
- Runtime rejects all other `store_meta` version mismatches, including newer index schema values.
- Legacy index replacement remains delete-and-rebuild from WAL (`wal.sqlite`, `wal.sqlite-wal`, `wal.sqlite-shm` removed, then rebuild).

## Explicit Index-Only `store_meta` Cutover Flow (Must Match Runtime Order)
1. Read `store_meta.json`.
2. Compute `expected = StoreMetaVersions::current()`.
3. If `store_meta` is missing, create new meta with `expected` and mark `write_meta = true`.
4. If `store_meta.store_id` mismatches requested store, fail with `MetaMismatch`.
5. If versions exactly match `expected`, use as-is and `write_meta = false`.
6. If only `index_schema_version` differs and `got.index_schema_version < expected.index_schema_version`, clone meta, set only `index_schema_version = expected.index_schema_version`, and set `write_meta = true`.
7. Otherwise fail with `UnsupportedStoreMetaVersion`.
8. Acquire store lock.
9. If `write_meta`, persist updated `store_meta.json` before opening WAL index.
10. Open SQLite WAL index with upgraded meta.
11. If `SchemaVersionMismatch`, delete index sqlite files, reopen fresh index, and force `needs_rebuild = true`.
12. Run `rebuild_index` when `needs_rebuild` is true.
13. Never attempt to read legacy index rows through current schema code.

Crash-safety expectation:
- If crash occurs after step 9 but before rebuild, next boot sees upgraded `store_meta` + legacy index and still rebuilds via step 11.

## API and Type Changes
- Add `WatermarkPair` and `WatermarkPairError` in `crates/beads-core/src/watermark.rs`.
- `WatermarkPair::new(applied, durable)` enforces:
  - both watermarks individually valid (`seq/head` genesis rules),
  - `durable.seq <= applied.seq`,
  - if seqs equal, heads must match.
- Export `WatermarkPair` and `WatermarkPairError` from `crates/beads-core/src/lib.rs`.
- Change WAL boundary API:
  - `WalIndexTxn::update_watermark(..., watermarks: WatermarkPair)`.
  - `WatermarkRow` stores one `WatermarkPair` plus accessors for `applied()` and `durable()` (or equivalent).
- No CLI/API wire-format changes.

## SQLite Schema Changes
Apply in `crates/beads-daemon-core/src/wal/index.rs` `initialize_schema` for `watermarks`:
```sql
CHECK (applied_seq >= 0),
CHECK (durable_seq >= 0),
CHECK (
  (applied_seq = 0 AND applied_head_sha IS NULL) OR
  (applied_seq > 0 AND applied_head_sha IS NOT NULL AND length(applied_head_sha) = 32)
),
CHECK (
  (durable_seq = 0 AND durable_head_sha IS NULL) OR
  (durable_seq > 0 AND durable_head_sha IS NOT NULL AND length(durable_head_sha) = 32)
),
CHECK (durable_seq <= applied_seq),
CHECK (
  durable_seq != applied_seq OR
  durable_head_sha IS applied_head_sha
)
```

## File-by-File Implementation
### 1) Version constants and version comparison
- Modify `crates/beads-core/src/store_meta.rs`.
- Bump `INDEX_SCHEMA_VERSION` to `2`.
- Keep `current()` consistency tests updated.
- Add/adjust helper logic if needed for “match except index schema” comparison (runtime can keep this logic private if preferred).

### 2) Runtime open-path cutover
- Modify `crates/beads-daemon/src/runtime/store/runtime.rs`.
- Replace strict `got_versions != expected_versions` rejection with index-only cutover classifier.
- Write upgraded `store_meta` before WAL index open when index-only stale.
- Keep `open_wal_index_with_layout` schema-mismatch branch as authoritative rebuild trigger.
- Preserve hard failure for non-index mismatches and newer index schema versions.
- Remove/replace tests that relied on invalid watermark rows being accepted then rejected at decode time.

### 3) Watermark pair boundary hardening
- Modify `crates/beads-core/src/watermark.rs`.
- Modify `crates/beads-core/src/lib.rs`.
- Modify `crates/beads-daemon-core/src/wal/mod.rs`.
- Modify `crates/beads-daemon-core/src/wal/index.rs`.
- Modify `crates/beads-daemon-core/src/wal/memory_index.rs`.
- Modify `crates/beads-daemon-core/src/wal/contract.rs`.
- Modify `crates/beads-daemon-core/src/wal/replay.rs`.
- Modify callsites:
  - `crates/beads-daemon/src/runtime/executor.rs`
  - `crates/beads-daemon/src/runtime/core/repl_ingest.rs`
  - `crates/beads-daemon/src/runtime/core/helpers.rs`
  - `crates/beads-daemon/src/runtime/repl/runtime.rs`

### 4) Test fixture version hygiene for schema bump
- Replace hardcoded index schema `1` with `StoreMetaVersions::INDEX_SCHEMA_VERSION` where indices are opened/rebuilt:
  - `crates/beads-rs/tests/integration/fixtures/identity.rs`
  - `crates/beads-rs/tests/integration/fixtures/wal.rs`
  - `crates/beads-rs/tests/integration/repl/ack.rs`
  - `crates/beads-daemon/src/runtime/executor.rs` (test setup)
  - any other direct `StoreMetaVersions::new(..., ..., ..., ..., 1)` used with WAL index.

## Regression Tests (Required)
### Runtime cutover regressions (`crates/beads-daemon/src/runtime/store/runtime.rs`)
- Add test: stale `store_meta` index version (`1`) + legacy sqlite index opens successfully and rebuild path runs.
- Test must assert post-open `store_meta.index_schema_version == StoreMetaVersions::INDEX_SCHEMA_VERSION`.
- Test must assert legacy sentinel table/file state is gone after open (proves delete/recreate happened).
- Test must assert invalid watermark insert now fails immediately (CHECK constraint), not on load.
- Add test: `store_meta` with newer index schema than runtime is rejected with `UnsupportedStoreMetaVersion`.
- Keep/update test: non-index version mismatch still rejected.

### SQLite watermark invariant regressions (`crates/beads-daemon-core/src/wal/index.rs`)
- Replace decode-time invalid-row test with insert-time constraint tests.
- Cover each invariant:
  - applied nonzero with null head rejected,
  - applied zero with non-null head rejected,
  - durable nonzero with null head rejected,
  - durable zero with non-null head rejected,
  - `durable_seq > applied_seq` rejected,
  - equal seq with head mismatch rejected.
- Keep valid-row roundtrip test to prove accepted shape.

### Watermark pair unit tests (`crates/beads-core/src/watermark.rs`)
- Reject `durable > applied`.
- Reject equal seq with differing heads.
- Accept valid unequal seq.
- Accept valid equal seq with identical heads.

### Integration/test-harness updates
- Update `crates/beads-rs/tests/integration/wal/index.rs` and `crates/beads-rs/src/test_harness/mod.rs` for new watermark API shape.

## Verification
- `cargo test -p beads-core watermark`
- `cargo test -p beads-daemon-core wal::index::tests`
- `cargo test -p beads-daemon runtime::store::runtime::tests`
- `cargo test -p beads-rs --test integration wal::index`
- `cargo fmt --all`
- `just dylint`
- `cargo clippy --all-features -- -D warnings`
- `cargo test`

## Done Checklist
- [ ] `INDEX_SCHEMA_VERSION` bumped to `2`.
- [ ] Runtime implements explicit index-only `store_meta` cutover before WAL index open.
- [ ] Non-index version mismatches and newer index schema versions still fail hard.
- [ ] WAL index schema mismatch still triggers delete-and-rebuild flow.
- [ ] `WatermarkPair` is the only WAL watermark write boundary type.
- [ ] SQLite `watermarks` invariants enforced at insert/update time with CHECK constraints.
- [ ] Regression test proves stale `store_meta` no longer blocks rebuild path.
- [ ] Regression test proves invalid watermark rows fail at SQL write time.
- [ ] All fixture/version hardcodes that would break on schema bump are updated.
- [ ] Full verification gate passes.
