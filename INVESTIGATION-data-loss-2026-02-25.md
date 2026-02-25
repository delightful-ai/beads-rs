# Investigation: Beads Data Loss — DSRs Repo (2026-02-25)

## Summary

An agent session created ~18 beads (epic bd-i5t + 17 subtasks) in the DSRs repo. After `bd sync` failed with `"invalid field value: meta.json missing checksum fields"`, all beads except one post-failure bug report (bd-yy3) disappeared from `.beads/issues.jsonl`.

**No data was deleted from git.** The git ref `refs/heads/beads/store` still contains 227 beads. The "loss" is an **export view corruption** caused by the daemon operating on an empty in-memory state after a failed git load, then overwriting the export file with that empty state.

The bd-i5t beads were never synced to git because the sync failure prevented it. They exist only in the local WAL (45 segments at `~/.local/share/beads-rs/stores/94b6bb93-.../wal/core/`).

## Root Cause

**Three bugs combined to produce the data loss:**

### Bug 1 (P0): No rollback of partial store initialization after failed git load

**File:** `crates/beads-daemon/src/runtime/core/repo_load.rs`

In both `ensure_repo_loaded()` and `ensure_repo_loaded_strict()`:

1. A `StoreRuntime` is inserted into `self.stores` and a `GitLaneState` into `self.git_lanes` **before** the git load completes (lines ~21-24)
2. If the git load fails (returns `Err`), the function returns the error **without removing** the inserted store
3. On the next command, `self.stores.contains_key(&store_id)` returns `true`, so the daemon skips git loading entirely and operates on the empty `StoreState::new()`
4. The `else if` branch also calls `self.export_go_compat()`, which exports the empty state to `issues.jsonl`

```rust
// Line ~20: Store inserted BEFORE load
self.stores.insert(store_id, runtime);
self.git_lanes.insert(store_id, GitLaneState::new());

// Line ~47: If load fails, error returned but store NOT removed
Ok(Err(e)) => return Err(OpError::from(e)),

// Line ~88: Next call finds store already present, skips loading
} else if let Some(store) = self.stores.get_mut(&store_id) {
    // ... exports empty state
    self.export_go_compat(store_id, &remote);
}
```

### Bug 2 (P1): Over-strict meta.json checksum validation

**File:** `crates/beads-git/src/wire.rs`, `SupportedStoreMeta::parse()` (line ~405)

The parser requires ALL checksum fields (`state_sha256`, `tombstones_sha256`, `deps_sha256`) to be present. If any are missing, it returns a hard error. The DSRs repo's `meta.json` has `format_version: 1` but **no checksum fields** — likely written by an older version of beads or a different writer.

**Actual content of `refs/heads/beads/store:meta.json`:**
```json
{
  "format_version": 1,
  "root_slug": "dsrs",
  "last_write_stamp": [1769815077826, 1]
}
```

This should be treated as "legacy/no-checksums" rather than "corrupt."

### Bug 3 (P1): Export not gated on successful load

**File:** `crates/beads-daemon/src/runtime/core/housekeeping.rs`, `export_go_compat()`

The export scheduler only checks `self.stores.contains_key(&store_id)` — it doesn't verify the store was successfully loaded from git. Combined with Bug 1, this allows exporting empty/default state.

## Exact Failure Sequence

1. Agent runs `bd sync` (or any command) against DSRs repo
2. Daemon enters `ensure_repo_loaded()` → store not loaded → inserts `StoreRuntime` + `GitLaneState`
3. Sends `GitOp::LoadLocal` → git thread calls `read_state_at_oid()` → `wire::parse_supported_meta()` → sees `format_version: 1` but no checksums → **returns `WireError::InvalidValue("meta.json missing checksum fields")`**
4. Error propagates back to `ensure_repo_loaded()` → `return Err(OpError::from(e))` → **store remains in `self.stores` with empty state**
5. Command fails, user sees error
6. Agent runs `bd create "bd sync/meta checksum migration issue"` to report the bug
7. Daemon enters `ensure_repo_loaded_strict()` → `self.stores.contains_key(&store_id)` is `true` → enters `else if` branch
8. **Calls `self.export_go_compat()` → exports empty state → `issues.jsonl` is overwritten to empty**
9. Returns `Ok(loaded)` → `bd create` succeeds, adding bd-yy3 to the empty in-memory state
10. After debounce, export fires again → `issues.jsonl` now has exactly 1 record (bd-yy3)

## Additional Impact (not reported but present)

- **False "synced" response:** `SyncWait` can falsely report "synced" because a fresh `GitLaneState::new()` has `dirty: false` and `sync_in_progress: false`
- **WAL data orphaned:** The bd-i5t beads likely exist in the WAL (45 segments present) but can't be replayed because the daemon never loads the base git state to replay against

## Data Recovery

### What's recoverable:
- **Git data (227 beads with dsrs- prefix):** Fully intact in `refs/heads/beads/store:state.jsonl`. Fix the meta.json parsing and these load normally.
- **WAL data (bd-i5t beads):** Potentially recoverable from 45 WAL segments at `~/.local/share/beads-rs/stores/94b6bb93-.../wal/core/`. Once git state loads successfully, WAL replay should reconstruct these.

### What's NOT recoverable:
- If the daemon was restarted and the WAL was truncated, the bd-i5t beads may be gone permanently.
- The export file has already been overwritten (no `.bak` since it was a symlink).

### Immediate recovery steps:
1. Fix `meta.json` parsing (Bug 2) to allow missing checksums
2. Restart daemon to clear the poisoned in-memory state
3. Run `bd list` to trigger re-load from git + WAL replay
4. If bd-i5t beads appear: success
5. If not: recreate from juvu's command history (all 14 `bd create` commands are recorded)

## Recommendations

### Fix 1 (P0): Rollback on failed load
In `ensure_repo_loaded()` and `ensure_repo_loaded_strict()`, either:
- **Option A:** Defer `self.stores.insert()` until after `apply_loaded_repo_state()` succeeds
- **Option B:** Add a rollback guard that removes the store on any error after insertion

### Fix 2 (P1): Tolerate missing checksums in meta.json
In `SupportedStoreMeta::parse()`, treat `format_version: 1` with missing checksums as `V1 { checksums: None }` instead of returning an error. Log a warning. The verification step already handles `checksums: None` gracefully.

### Fix 3 (P1): Gate export on load success
Add a `loaded_ok: bool` field to `GitLaneState` (or equivalent), set only by `apply_loaded_repo_state()`. Require it before enqueueing `ExportJob`.

### Fix 4 (P2): Observability
- Log `store_id`, `remote_url`, `export_path`, `live_count` during export
- Log `remote_url`, `old_target`, `new_target` during symlink updates
- Use structured `OpError::LoadTimeout` consistently for both `LoadLocal` and `Load` timeouts

## Preventive Measures

1. **Invariant:** A store in `self.stores` must always have valid state. Either it's been loaded from git, or it doesn't exist in the map.
2. **Invariant:** Export should never run against a store that hasn't been successfully loaded.
3. **Test:** Add an integration test that simulates meta.json parse failure and verifies:
   - The store is NOT left in `self.stores`
   - A subsequent command retries the load instead of using empty state
   - The export file is NOT overwritten
4. **Backwards compatibility:** meta.json parsing should always be forwards-compatible with older writers that may not include newer fields.

## Investigation Log

### Phase 1: Symptom identification
- `.beads/issues.jsonl` reduced from ~18 records to 1
- Only surviving record is bd-yy3 (the sync bug report itself)
- `bd sync` failed with `"meta.json missing checksum fields"`

### Phase 2: Code analysis via context_builder
- Traced `bd sync` → `SyncWait` → `ensure_loaded_and_maybe_start_sync` → `ensure_repo_loaded`
- Identified the partial initialization + no-rollback pattern
- Confirmed `export_jsonl` does full rewrite from in-memory state

### Phase 3: Direct code verification
- Read `repo_load.rs`: confirmed store inserted before load, not removed on error
- Read `wire.rs`: confirmed `SupportedStoreMeta::parse()` hard-fails on missing checksums
- Read `export.rs`: confirmed full rewrite semantics
- Read `housekeeping.rs`: confirmed export only checks `stores.contains_key()`

### Phase 4: Evidence from live system
- `git show refs/heads/beads/store:meta.json` → no checksum fields (trigger condition confirmed)
- `git show refs/heads/beads/store:state.jsonl` → 227 beads with `dsrs-` prefix, none with `bd-` prefix (data never synced to git)
- WAL directory has 45 segments → potential recovery source
- Export file has 1 record → confirmed overwrite

### Oracle review: Confirmed analysis, added false-positive SyncWait risk
