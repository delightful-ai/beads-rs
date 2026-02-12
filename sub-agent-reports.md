# Sub-Agent Reports

Generated: 2026-02-12

## 1) `019c541f-e02e-7363-859c-a5a3a3aad35e`

**Scope:** `bd show` non-JSON latency root cause and single-RPC design

**Findings:**
- Human `bd show` currently makes 4 IPC round trips: `Request::Show`, then `Request::Deps`, `Request::Notes`, and `Request::List`.
- This multiplies daemon dispatch/IPC/read-gate costs and introduces avoidable latency.

**Proposed fix:**
- Add an aggregated request/response path (e.g. `Request::ShowDetails` + `QueryResult::ShowDetails`) that returns issue + deps + notes + summaries in one query.
- Switch human `bd show` path to single request; keep JSON compatibility.

## 2) `019c541f-e03d-7ad3-952b-93d0019a04d7`

**Scope:** repeated store identity resolution overhead

**Findings:**
- Request handling repeatedly enters store resolution paths (`ensure_repo_loaded*` -> `StoreCaches::resolve_store`).
- In `resolve_store_id`, even when cached resolution exists, code still re-opens repo and re-runs verification path, causing repeated `store identity resolved` overhead.

**Proposed fix:**
- Short-circuit cached verified resolutions without re-opening/re-verifying on every request.
- Re-verify only when needed (unverified state, invalidation, remote change).

## 3) `019c541f-e052-7921-80ea-bc5f0ae10756`

**Scope:** backup ref lock contention / stale lock behavior

**Findings:**
- Backup refs are created/pruned in sync path (`ensure_backup_ref`, `prune_backup_refs`).
- Stale `.git/refs/beads/backup/*.lock` files can persist after interrupted/failed operations.
- Current code surfaces `Locked` errors but does not clean stale lockfiles/retry, so failures can repeat indefinitely.

**Proposed fix:**
- On `Locked`, remove stale lockfile and retry once for create/delete operations in backup ref maintenance.
- Add regression tests for stale lock recovery.

## 4) `019c541f-e06e-76c2-803c-9ea7c8d476ac`

**Scope:** checkpoint decode warning (`WireLabelStateV1` invalid length)

**Findings:**
- Root cause was legacy checkpoint compatibility (`labels: []` array form) hitting strict struct deserialization for `WireLabelStateV1`.
- Compatibility logic has already been added in local changes (`crates/beads-core/src/wire_bead.rs`, `crates/beads-rs/src/git/wire.rs`) with regression test:
  - `parse_state_accepts_legacy_labels_array`

**Validation reported by sub-agent:**
- `cargo fmt --all`
- `cargo test parse_state_accepts_legacy_labels_array --package beads-rs`

## 5) `019c541f-e098-73e1-b09d-28bf8e405137`

**Scope:** instrumentation gaps in `bd admin metrics`

**Findings:**
- Current metrics capture WAL/checkpoint/apply/etc but not per-IPC-request latency by request type.
- `bd admin metrics` only reflects what metrics subsystem receives, so request histograms are absent.

**Proposed fix:**
- Add IPC request latency histogram emission in daemon request dispatch (`process_request_message`), labeled by `request_type`.
- Surface automatically via existing admin metrics snapshot/serialization.
- Add integration tests to ensure request-latency histograms appear.

---

## Prioritized Action Order (derived from reports)

1. Single-RPC `bd show` read path (highest UX impact).
2. Per-request latency instrumentation (long-term benchmarkability).
3. Store identity verified-cache short-circuit (reduce repeated read overhead).
4. Backup lock stale-lock cleanup/retry (stability under sync contention).
5. Keep legacy labels compatibility patch + tests (already addressed in working tree).
