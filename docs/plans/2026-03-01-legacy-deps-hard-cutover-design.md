# Legacy Deps Hard Cutover to OR-Set V1

## Goal and constraints

Real repositories exist where `refs/heads/beads/store` still contains a legacy `deps.jsonl` encoded as JSONL “one edge per line”, not the current single-object OR-Set snapshot (`WireDepStoreV1 { cc, entries, stamp }`).

Current runtime load path is strict:

- `SyncProcess::fetch` -> `read_state_at_oid` -> `wire::parse_legacy_state` -> `parse_deps` -> `serde_json` decode into `WireDepStoreV1`
- Legacy deps fails with `missing field cc`, which bricks commands that need store load.

Hard cutover policy:

- No runtime backward-compat parsing for canonical store loads.
- Provide explicit migration that rewrites `refs/heads/beads/store` to strict OR-Set deps shape and required v1 invariants (including `notes.jsonl` + checksums in `meta.json`).

Scope includes mixed-format sweep across:

- sync-ref load/write path (git store commits)
- checkpoint import/export path
- CLI migrate surface
- safe ref update and multi-repo rollout/backfill

## Target end state

After migration, each store commit on `refs/heads/beads/store` is readable by strict runtime:

- Tree contains: `state.jsonl`, `tombstones.jsonl`, `deps.jsonl`, `notes.jsonl`, `meta.json` (SPEC §5.2).
- `deps.jsonl` is a single JSON object matching `beads_core::WireDepStoreV1`:
  - has `cc` and `entries` (optional `stamp`)
  - entries sorted by `DepKey (from,to,kind)`, dots sorted
  - validates via `SnapshotCodec::validate_dep_store`
- `meta.json` includes complete checksums including deps and notes.
- Migration writes a new commit and pushes (unless `--no-push`).

## Locked design decisions

1. Runtime strictness stays intact.
- Do not add fallback parsing to `read_state_at_oid` or normal sync/daemon hydration.

2. Legacy parsing is migration-only.
- Only `bd migrate detect` and `bd migrate to 1` may parse legacy deps.

3. Checkpoints are cache.
- For incompatible checkpoint payloads, classify and rebuild.
- Do not add legacy checkpoint parsing.

4. Default safety posture.
- Refuse unrelated-history migration unless `--force`.
- Always create backup ref before local store-ref update.

## Precision contracts for implementors

This section locks behaviors that were previously implicit, so separate implementors produce the same behavior.

### A) Local/remote source and parent selection matrix

Migration executor must use this matrix after fetch/ref resolution:

| Case | local `refs/heads/beads/store` | remote `refs/remotes/origin/beads/store` | Default behavior | Parent for new commit |
|---|---|---|---|---|
| 1 | missing | missing | fail: nothing to migrate | n/a |
| 2 | present | missing | migrate from local only | `local_oid` |
| 3 | missing | present | migrate from remote only (create local migrated commit) | `remote_oid` |
| 4 | present | present (equal) | migrate that single state | `remote_oid` |
| 5 | present | present, related ancestry | load both and `join` | `remote_oid` |
| 6 | present | present, unrelated ancestry | refuse unless `--force`; with `--force`, load both and `join` | `remote_oid` |

Notes:

- This intentionally mirrors sync’s linear-history posture (remote parent preferred when remote exists).
- `Case 6` is stricter than current runtime sync behavior; this is migration safety policy, not sync policy.

### B) `effective_format_version` and `needs_migration` truth table

`effective_format_version` must reflect strict readability + v1 invariants, not only `meta.format_version`.

| `meta_format_version` | `deps_format` | `notes_present` | `checksums_present` | `effective_format_version` | `needs_migration` |
|---|---|---|---|---|---|
| `Some(1)` | `orset_v1` | `true` | `true` | `1` | `false` |
| `Some(1)` | `legacy_edges` | any | any | `0` | `true` |
| `Some(1)` | `orset_v1` | `false` | any | `0` | `true` |
| `Some(1)` | `orset_v1` | `true` | `false` | `0` | `true` |
| `Some(0)` or `None` | any | any | any | `0` | `true` |
| any | `missing` or `invalid` | any | any | `0` | `true` |

### C) `checksums_present` exact rule

Set `checksums_present = true` only if all are true:

1. `meta.json` parses as supported meta.
2. `meta.checksums()` exists.
3. checksum values exist for `state`, `tombstones`, `deps`, and `notes`.

Rationale:

- `parse_supported_meta` currently allows `notes_sha256` omission in v1 meta.
- Migration target requires notes checksum for strict target invariants, so detect must still flag omission as needing migration.

### D) Legacy deps parser row-handling contract

Legacy parser behavior is deterministic and warning-driven:

- blank line: ignore, no warning
- malformed JSON: warning, skip row
- missing required key fields (`from/to/kind` after shape normalization): warning, skip row
- unknown kind alias: warning, skip row
- invalid bead IDs / invalid `DepKey` (including self-edge if rejected by core): warning, skip row
- duplicate logical edge rows: coalesce (same deterministic dot), do not duplicate entries

Warnings must include row context (line number) and a stable machine-greppable code prefix.

Recommended warning prefixes:

- `LEGACY_DEPS_JSON(line=...)`
- `LEGACY_DEPS_SHAPE(line=...)`
- `LEGACY_DEPS_KIND(line=...)`
- `LEGACY_DEPS_KEY(line=...)`

### E) Deterministic dot derivation contract

Given normalized `DepKey`, derive migration dot exactly:

1. `input = key.collision_bytes() || b"legacy-deps-v0"`
2. `digest = sha256(input)` (32 bytes)
3. `replica = ReplicaId::from(Uuid::from_bytes(digest[0..16]))`
4. `counter = u64::from_le_bytes(digest[16..24])`
5. `Dot { replica, counter }`

Use little-endian counter conversion to match existing legacy-dot convention used in go import code (`legacy_dot_from_bytes`).

### F) Idempotency and no-op contract

`bd migrate to 1` is a no-op (`changed=false`, `commit_oid=null`) when all are true:

- strict detect result reports no migration needed
- computed merged canonical bytes match would-be parent tree bytes for all store blobs
- no metadata normalization changes are needed

`--force` does not require creating an empty “marker commit”; preserve no-op behavior if no content change.

### G) Push retry contract

Migration push retry must match sync retry semantics:

- total attempts = `max_retries + 1`
- on retryable push rejection, refetch and recompute from refs before retry
- fail with retry-limit error after attempts exhausted

Retryable push rejection classifier (case-insensitive substring match), aligned with current sync/checkpoint logic:

- `non-fast-forward`
- `fetch first`
- `cannot lock ref`
- `failed to update ref`
- `failed to lock file`

Migration executor should set `MIGRATE_MAX_RETRIES = 3` unless an explicit configuration knob is introduced.

### H) Backup ref naming and retention contract

Use existing backup namespace conventions from sync:

- prefix: `refs/beads/backup/`
- full name: `refs/beads/backup/<oid>`
- retention cap: `64` refs

Create backup ref before local store-ref update attempt, then prune to retention cap using the same selection rule as sync helpers (keep newest, protect current target oid).

### I) Checkpoint incompatibility classification and rebuild contract

Add a typed import classification for deps-format incompatibility (for example `CheckpointImportError::IncompatibleDepsFormat { ... }`).

Daemon load behavior on this classification:

1. do not apply checkpoint state from that checkpoint
2. do not apply checkpoint watermarks from that checkpoint
3. log explicit incompatibility reason
4. schedule/force rebuild once per group for the load cycle

Rebuild scheduling should use existing checkpoint scheduler hooks (for example `force_checkpoint_for_namespace`) and avoid hot-loop rescheduling on repeated failures in the same cycle.

### J) Concrete output examples (locked)

Detect (meta says 1 but deps still legacy):

```json
{
  "meta_format_version": 1,
  "latest_format_version": 1,
  "effective_format_version": 0,
  "deps_format": "legacy_edges",
  "notes_present": true,
  "checksums_present": true,
  "needs_migration": true,
  "reasons": [
    "deps.jsonl is legacy line-per-edge (missing cc)"
  ]
}
```

Migrate dry-run with warnings:

```json
{
  "dry_run": true,
  "from_effective_version": 0,
  "to_version": 1,
  "deps_format_before": "legacy_edges",
  "converted_deps": true,
  "added_notes_file": true,
  "wrote_checksums": true,
  "commit_oid": null,
  "push": "skipped_no_push",
  "warnings": [
    "LEGACY_DEPS_KIND(line=42): unknown dep kind `blocked-by`",
    "LEGACY_DEPS_KEY(line=87): invalid dep key (self-edge)"
  ]
}
```

## CLI user experience

### 1) Detection

`bd migrate detect` must classify structural status even when `meta.format_version` is already `1`.

Example output:

```json
{
  "meta_format_version": 1,
  "latest_format_version": 1,
  "effective_format_version": 0,
  "deps_format": "legacy_edges",
  "notes_present": false,
  "checksums_present": false,
  "needs_migration": true,
  "reasons": [
    "deps.jsonl is legacy line-per-edge (missing cc)",
    "notes.jsonl missing",
    "meta checksums missing"
  ]
}
```

Detect behavior requirements:

- If local store ref is missing but remote exists, detect still classifies (no hard error).
- Missing `meta.json` is treated as legacy/meta-unknown, not fatal.
- Missing `notes.jsonl` is surfaced as a reason.
- `deps_format` values: `orset_v1 | legacy_edges | missing | invalid`.

### 2) Migration execution

Implement `bd migrate to 1` (currently stubbed) as explicit hard-cutover migration.

- `bd migrate to 1 --dry-run`
  - prints what would change (classification, counts, warnings)
  - performs no commit/ref update/push/daemon refresh
- `bd migrate to 1`
  - rewrites store ref to strict canonical v1
  - pushes unless `--no-push`
- `--force`
  - allows proceeding when default safety checks would refuse (for example unrelated history)

Migration outcome should distinguish push disposition explicitly:

- `pushed`
- `skipped_no_push`
- `skipped_no_remote`

Optional ergonomics (recommended): when strict runtime load fails with the specific `missing field cc` signature, surface actionable hint: `Run bd migrate to 1`.

## Architecture boundary: where legacy parsing lives

To honor explicit migration flow (not runtime compatibility), legacy deps parsing must be reachable only from migrate/detect paths.

Concretely:

- Keep `crates/beads-git/src/sync.rs::read_state_at_oid` strict.
- Add migration-only loader function(s) that can parse legacy deps for detect/migrate.
- `bd migrate to 1` uses migration loader and rewrites to strict canonical snapshot.

Guardrail:

- Migration parser must not be used by runtime sync load path.

## Data conversion: legacy deps JSONL -> OR-Set snapshot

### Legacy input assumptions

Legacy `deps.jsonl` is JSONL with one edge per line. Field names vary across historical versions.

### OR-Set output requirements

`WireDepStoreV1` requires:

- `entries: Vec<WireDepEntryV1 { key: DepKey, dots: Vec<Dot> }>`
- `cc: Dvv` consistent with entries and acceptable to OR-Set validation
- optional `stamp: Option<WireFieldStamp>`

### Dot strategy (critical)

Migration should not inflate duplicate dots under join when both sides migrate the same legacy edges.

Recommended deterministic dot derivation:

- input bytes: `b"legacy-deps-v0" + DepKey::collision_bytes()`
- SHA-256 digest
- first 16 bytes -> `ReplicaId`
- next 8 bytes -> `counter` (`u64`)

This provides stable per-key dots across replicas and keeps post-migration joins clean.

### Causal context (`cc`) strategy

Use import normalization path (robust, already aligned with existing patterns):

1. Build raw entries map: `BTreeMap<DepKey, BTreeSet<Dot>>`.
2. Start with `Dvv::default()`.
3. Normalize via `OrSet::normalize_for_import(entries, cc)`.
4. Emit normalized `WireDepStoreV1`.

### Legacy delete semantics

- If legacy rows encode delete intent, represent removal by including removed dot in context but not in entries.
- If legacy format lacks delete records, import visible active edges only and document limitation.

### Legacy shapes and kind normalization

Support at least:

1. Canonical-ish rows:
- `{"from":"bd-a","to":"bd-b","kind":"blocks", ...}`

2. Historical beads-go-ish rows:
- `{"issue_id":"bd-a","depends_on_id":"bd-b","type":"blocks", ...}`

3. Kind aliases:
- `blocks | block` -> `blocks`
- `related | relates` -> `related`
- `parent-child | parent` -> `parent-child`
- `discovered-from | discovered_from` -> `discovered-from`

Unknown/invalid rows should produce warnings and be skipped.

## Migration flow: end-to-end

### Phase 0: Safety pre-checks

- Ensure repo context and store refs are discoverable.
- Fetch `refs/heads/beads/store` into `refs/remotes/origin/beads/store` when remote exists.
- Resolve `local_oid` and `remote_oid` when present.
- Divergence policy:
  - if one is descendant of the other: proceed
  - if unrelated: refuse unless `--force`
- Create backup ref before local ref update.

### Phase 1: Migration-aware load

Load local/remote states via migration loader:

- read `state.jsonl`
- read `tombstones.jsonl`
- read `deps.jsonl` (strict parse first, fallback to legacy conversion)
- `notes.jsonl` optional (missing -> empty)
- `meta.json` optional

Return:

- `CanonicalState`
- `SupportedStoreMeta`
- classification flags (`deps_format`, `notes_present`, `checksums_present`)
- warnings from legacy parse/conversion

### Phase 2: Merge state

Mirror sync semantics:

- if remote exists, merge local into remote via `CanonicalState::join`
- keep migration deterministic and idempotent

### Phase 3: Commit rewritten canonical snapshot

Write blobs using canonical writers:

- `wire::serialize_state(&state)`
- `wire::serialize_tombstones(&state)`
- `wire::serialize_deps(&state)`
- `wire::serialize_notes(&state)`

Compute checksums and serialize meta:

- `wire::StoreChecksums::from_bytes(...)`
- `wire::serialize_meta(root_slug, last_write_stamp, checksums)`

Metadata policy:

- preserve `root_slug` if present
- else infer from existing IDs, fallback `"bd"`
- preserve `last_write_stamp` if present; else `None`

Parenting policy:

- remote head as parent when present
- else local head
- else root commit for uninitialized state

Commit message:

- `migrate: deps to OR-Set v1` (+ optional summary counts)

### Phase 4: Push with retry loop

- push `refs/heads/beads/store:refs/heads/beads/store`
- on retryable failures (non-fast-forward / lock races), refetch and retry full migrate cycle (idempotent)

### Phase 5: Daemon refresh

- best-effort `notify_migrate_refresh` after successful non-dry-run migration

## Concrete code changes by area

### A) CLI migrate surface

File: `crates/beads-cli/src/commands/migrate.rs`

1. Implement `MigrateCmd::To`

- validate `args.to == FormatVersion::CURRENT.get()` (currently `1`)
- call new backend `run_migrate_to(MigrateToRequest { ... })`
- return structured JSON outcome

2. Upgrade `Detect` output

- replace bare version comparison output with rich structural detect payload

### B) Backend trait contracts

File: `crates/beads-cli/src/backend.rs`

1. Replace detect return type

From:

```rust
fn run_migrate_detect(&self, request: MigrateDetectRequest) -> Result<u32, Self::Error>;
```

To:

```rust
pub enum DepsFormat { OrSetV1, LegacyEdges, Missing, Invalid }

pub struct MigrateDetectOutcome {
    pub meta_format_version: Option<u32>,
    pub effective_format_version: u32,
    pub latest_format_version: u32,
    pub deps_format: DepsFormat,
    pub notes_present: bool,
    pub checksums_present: bool,
    pub needs_migration: bool,
    pub reasons: Vec<String>,
}
```

2. Add migrate-to contracts

```rust
pub struct MigrateToRequest {
    pub repo: PathBuf,
    pub to: u32,
    pub dry_run: bool,
    pub force: bool,
    pub no_push: bool,
}

pub enum PushDisposition {
    Pushed,
    SkippedNoPush,
    SkippedNoRemote,
}

pub struct MigrateToOutcome {
    pub dry_run: bool,
    pub from_effective_version: u32,
    pub to_version: u32,
    pub deps_format_before: DepsFormat,
    pub converted_deps: bool,
    pub added_notes_file: bool,
    pub wrote_checksums: bool,
    pub commit_oid: Option<String>,
    pub push: PushDisposition,
    pub warnings: Vec<String>,
}

fn run_migrate_to(&self, request: MigrateToRequest) -> Result<MigrateToOutcome, Self::Error>;
```

3. Keep `run_migrate_apply_import` and `notify_migrate_refresh` unchanged.

### C) Host backend implementation

File: `crates/beads-rs/src/cli/backend.rs`

1. Implement rich `run_migrate_detect`

- classify deps format by strict parse probe + legacy fallback detector
- detect `notes.jsonl` and checksum presence/completeness
- tolerate missing `meta.json`/local ref for classification
- compute `effective_format_version` and `needs_migration`

2. Implement `run_migrate_to`

- execute full migration flow described above
- do not depend on daemon to load legacy deps

3. Harden version detection behavior

- missing `meta.json` should not hard-fail detect path

### D) Git wire changes (migration-only parsing)

File: `crates/beads-git/src/wire.rs`

Add migration-only helpers without altering runtime strict entrypoint semantics:

1. Expose strict deps parse probe

```rust
pub fn parse_deps_wire(bytes: &[u8]) -> Result<WireDepStoreV1, WireError>;
```

2. Add legacy deps parser (migration-only)

```rust
pub fn parse_legacy_deps_edges(bytes: &[u8]) -> Result<(WireDepStoreV1, Vec<String>), WireError>;
```

3. Add migration-capable state parser

```rust
pub fn parse_state_allow_legacy_deps(
    state_bytes: &[u8],
    tombstones_bytes: &[u8],
    deps_bytes: &[u8],
    notes_bytes: &[u8],
) -> Result<(CanonicalState, DepsFormat, Vec<String>), WireError>;
```

Guardrail:

- Do not route `read_state_at_oid` through this function.

### E) Sync layer: migration loader and executor

File: `crates/beads-git/src/sync.rs`

1. Add migration loader by OID

```rust
pub fn read_state_at_oid_for_migration(
    repo: &Repository,
    oid: Oid,
) -> Result<LoadedStoreMigration, SyncError>;
```

`LoadedStoreMigration` includes:

- `state: CanonicalState`
- `meta: wire::SupportedStoreMeta`
- `deps_format: DepsFormat`
- `warnings: Vec<String>`

Behavior:

- reads blobs similarly to `read_state_at_oid`
- treats missing notes as empty
- treats missing meta as legacy defaults
- parses via `wire::parse_state_allow_legacy_deps`

2. Add migrate executor for store ref

- fetch refs
- load local+remote through migration loader
- merge with `CanonicalState::join`
- commit canonical snapshot
- push with retry loop

3. Optional refactor for shared commit logic

Extract reusable helper from sync commit path and reuse in migrate executor:

```rust
fn commit_store_snapshot(
    repo: &Repository,
    parent_oid: Oid,
    state: &CanonicalState,
    root_slug: Option<&BeadSlug>,
    parent_meta_stamp: Option<&WriteStamp>,
    message: &str,
) -> Result<Oid, SyncError>
```

## Checkpoint mixed-format strategy

Chosen strategy: treat incompatible checkpoints as invalid cache and rebuild.

Files:

- `crates/beads-git/src/checkpoint/import.rs`
- `crates/beads-daemon/src/runtime/core/checkpoint_import.rs`
- `crates/beads-daemon/src/runtime/core/repo_load.rs`
- `crates/beads-daemon/src/runtime/git_worker.rs`
- (optional) `crates/beads-core/src/store_meta.rs`

Required behavior:

1. Import classification

- If checkpoint deps parse fails due to legacy/incompatible shape, emit typed incompatible-checkpoint error classification.
- Avoid brittle string matching where possible.

2. Daemon startup/load behavior

- log incompatibility explicitly
- continue boot
- schedule or force checkpoint rebuild from canonical store

3. Export/publish hardening

- validate `previous` checkpoint payload before reuse
- on invalid previous payload, drop it and export full (`previous: None`)
- avoid caching invalid exports as reusable state before successful publish

Optional format bump remains secondary:

- only consider bumping checkpoint version if also planning explicit store-meta/runtime version migration handling.

## Safety and correctness measures for store-ref rewrite

1. Single-parent linear history

- align with sync parent selection (remote head preferred)

2. Idempotency

- canonical store with complete invariants should be no-op (unless forced)

3. Backup refs

- create backup before local ref rewrite, not only on divergence

4. Push retry

- refetch and recompute on retryable push failures

5. Daemon health

- best-effort refresh notification after migration

## Rollout and backfill strategy

### Phase 1: Ship tooling

Release version with:

- `bd migrate detect` structural classification
- `bd migrate to 1` hard-cutover migration
- strict-load hint for `missing field cc`

### Phase 2: Backfill repos

Per repo:

1. `bd migrate detect`
2. `bd migrate to 1 --dry-run`
3. `bd migrate to 1`
4. verify `bd list`, `bd blocked`, `bd ready`

### Phase 3: Fleet automation

Document recipe/script to iterate repositories:

- run detect
- run migrate when needed
- optionally run local-only pass first (`--no-push`)

Document in `MIGRATION.md` (or dedicated migration architecture doc).

### Phase 4: Checkpoint cleanup policy

After rollout:

- incompatible checkpoints are ignored/rebuilt automatically
- expose operator guidance for forcing rebuild where needed

## Key impacts and tradeoffs

1. Per-edge provenance loss

- legacy per-edge provenance fields are not represented in canonical OR-Set deps snapshot

2. Legacy delete limitations

- if delete history is absent in legacy data, full historical remove/add intent is unrecoverable

3. Older binary incompatibility

- older clients expecting line-per-edge deps will fail after cutover

## Summary of touched files

CLI:

- `crates/beads-cli/src/commands/migrate.rs`
- `crates/beads-cli/src/backend.rs`
- `crates/beads-rs/src/cli/backend.rs`

Git/wire/sync:

- `crates/beads-git/src/wire.rs`
- `crates/beads-git/src/sync.rs`

Checkpoint/daemon:

- `crates/beads-git/src/checkpoint/import.rs`
- `crates/beads-daemon/src/runtime/core/checkpoint_import.rs`
- `crates/beads-daemon/src/runtime/core/repo_load.rs`
- `crates/beads-daemon/src/runtime/git_worker.rs`
- `crates/beads-git/src/checkpoint/publish.rs`
- optional: `crates/beads-core/src/store_meta.rs`

Docs:

- `MIGRATION.md`

## Test plan

Unit:

- legacy dep shape parsing + kind normalization + warning paths
- deterministic dot derivation stability
- runtime parser strictness boundary (legacy still rejected)
- checkpoint incompatible classification tests

Integration:

- detect classifies legacy deps even with meta v1
- detect handles missing local ref with remote present
- migrate dry-run non-mutating
- migrate writes strict deps + notes + checksums
- divergence refusal vs `--force`
- push disposition correctness (`pushed`, `skipped_no_push`, `skipped_no_remote`)

Daemon/checkpoint:

- incompatible checkpoint does not block boot and triggers rebuild scheduling
- invalid previous checkpoint is not reused
- publish path avoids repeated failures from stale invalid previous payload

## Acceptance criteria

- `bd migrate detect` accurately reports structural migration need.
- `bd migrate to 1` repairs legacy deps stores into strict readable v1 commits.
- runtime strict load path remains uncompromised (no fallback compat parsing).
- checkpoint incompatibility is classified and self-healed via rebuild path.
- migration remains safe under remote contention via retryable push flow.
