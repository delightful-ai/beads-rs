# Rich Status Hard Cut Migration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the public `tracker_state` plus coarse `status` split with one first-class rich `status` surface, shipped behind an explicit canonical-store migration and a first-class local daemon store rebuild path.

**Architecture:** Bump the canonical git-backed store from format version `1` to `2`, keep legacy decode only inside the migration path, and move normal runtime/API/CLI/Symphony surfaces to a single rich `status` enum. Treat the daemon-local store as derivative: migration must explicitly reset/rebuild it or guide the user through that reset, and normal startup must not strand the user on an opaque local-version mismatch.

**Tech Stack:** Rust, serde JSONL wire format, `bd migrate`, daemon sqlite/WAL durability, IPC/CLI surfaces, Symphony Elixir beads adapter.

## Execution Status

Completed on the sibling `jj` workspace at `/Users/darin/src/personal/beads-rs-symphony-api`.

Landings:

- canonical repo-store format is now `v2`
- public/API/CLI beads state is one rich `status` surface
- migration detect/apply and local daemon-store rebuild behavior are updated to the `v2` story
- stale `tracker_state` / coarse-status residue in tests, scripts, and helper apps was cut over
- magic `blocked` / `all` pseudo-status selectors were deleted; `status` filters are now typed canonical status only
- Symphony's beads client now speaks beads' canonical `status` / `statuses` wire shape and maps it back into Symphony's internal `Issue.state`

Fresh verification completed on the landed tree:

- `cargo fmt --all`
- `just dylint`
- `cargo clippy --all-features -- -D warnings`
- `cargo xtest`
- `cargo nextest run --profile slow --workspace --all-features --features slow-tests`
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s /Users/darin/src/personal/beads-rs-symphony-api/scripts -p 'test_*.py' -v`
- `mise exec -- mix test test/symphony_elixir/extensions_test.exs test/symphony_elixir/beads_client_test.exs`
- `mise exec -- mix specs.check`
- `python3 /Users/darin/src/personal/beads-rs-symphony-api/scripts/e2e_symphony_beads.py --skip-build --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir`

Remaining honest gap:

- terminal branch metadata is still split out from canonical status lifecycle in core; the follow-on architectural fusion remains the next cleanup if we want the full type-level cut

---

## Kind

Implementation/migration plan.

## References

- Current hardening note: `docs/plans/2026-04-19-beads-symphony-api-hardening.md`
- Existing tracker-state cut bead: `beads-rs-suj8.5`
- Local daemon store sharp-edge follow-up: `beads-rs-ee69`

## Locked Decisions

- Hard cut only. No long-lived dual public surface where `tracker_state` and coarse `status` both appear as peer truths.
- The rich board/work enum becomes the canonical user/API/CLI `status`.
- `open`, `in_progress`, and `closed` survive only as internal bucket helpers where still useful for summaries or gating. `blocked` remains derived dependency state, not a status value.
- The canonical repo store gets an explicit `v1 -> v2` migration. Legacy `v1` decode remains migration-only after the cut.
- The daemon-local store is derivative. We do not build a field-by-field WAL/checkpoint translator for this rename; we rebuild/reset it intentionally.
- Local-store handling must be first-class. `beads-rs-ee69` is part of this cut, not optional cleanup.
- Closing semantics remain typed terminal outcomes (`done`, `cancelled`, `duplicate`). Human free-text explanation stays separate from canonical status.

## Target Vocabulary

Post-cut, the canonical status enum should be the current rich tracker taxonomy under the plain name `status`:

- `Todo`
- `InProgress`
- `HumanReview`
- `Rework`
- `Merging`
- `Done`
- `Cancelled`
- `Duplicate`

Naming target:

- core type: `IssueStatus` or `BeadStatus`
- stored field: `status`
- public API field: `status`
- tracker payload field currently named `state`: rename to `status`

Avoid the name `TrackerState` in public/user-facing surfaces after the cut.

## Explicit Migration Split

There are two different migrations. They must not be conflated.

### 1. Canonical repo-store migration (`beads/store`)

This is durable truth. It gets a versioned migration:

1. Detect repo store format version.
2. If already `2`, no-op.
3. If `1`, load local/remote `beads/store` refs with a migration-only legacy parser.
4. Convert legacy wire rows:
   - `tracker_state` field -> `status`
   - coarse legacy `status` only used for legacy import/migration decode, not re-emitted
   - tracker payload/JSON field names that still say `state` get renamed in the new format
5. Write a new `beads/store` commit at `format_version = 2`.
6. Preserve current backup-ref safety behavior before moving refs.
7. Push as usual unless `--no-push`.

### 2. Local daemon-store migration (derivative sqlite/WAL/checkpoints)

This is not canonical truth. It should not get a bespoke semantic translator.

Desired behavior:

1. `bd migrate to 2` either:
   - explicitly resets/rebuilds the local daemon store for the target repo, or
   - marks/schedules that reset and tells the user exactly what will happen next.
2. If normal startup still encounters an old local store, it should:
   - detect the unsupported local-store version,
   - present an intentional reset/rebuild path or perform it automatically,
   - rebuild from the migrated git store,
   - never leave the user with a mysterious unsupported-version dead end.

This behavior is tracked by `beads-rs-ee69` and should land before we call the overall cut finished.

## Required Runtime Shape After The Cut

- Normal runtime/load paths accept only canonical `v2` repo-store shape.
- Migration paths may still decode `v1`, but only behind `bd migrate`.
- Public/API/CLI issue surfaces expose rich `status`, not coarse `WorkflowStatus`.
- Default list/read behavior hides terminal work via `status.is_terminal()`.
- Count/status summary code may still compute coarse buckets internally, but not as a first-class user state model.

## Implementation Sequence

Do not reorder the major phases below.

1. Land migration scaffolding while the old names still compile.
2. Land first-class local daemon store rebuild/reset handling (`beads-rs-ee69`).
3. Bump canonical repo-store format to `2` and make normal runtime loads `v2`-only.
4. Rename core/public/API/CLI surfaces from `tracker_state`/coarse `status` onto rich `status`.
5. Re-run Symphony end-to-end on a freshly migrated disposable backend.

## Task 1: Version Scaffolding And Migration Entry Points

**Files:**
- Modify: `crates/beads-core/src/meta.rs`
- Modify: `crates/beads-git/src/wire.rs`
- Modify: `crates/beads-git/src/sync.rs`
- Modify: `crates/beads-rs/src/cli/backend.rs`
- Modify: `crates/beads-cli/src/commands/migrate.rs`
- Test: `crates/beads-rs/tests/integration/cli/migration.rs`

**Steps:**

1. Add `FormatVersion::CURRENT = 2` in `crates/beads-core/src/meta.rs`.
2. Split repo-store parsing in `crates/beads-git/src/wire.rs` into:
   - normal/runtime `v2` parser
   - migration-only `v1` parser
3. Move the current legacy `inject_legacy_tracker_state()` style behavior behind the migration-only path.
4. Extend `bd migrate detect` / `bd migrate to` plumbing so `2` is the latest supported target.
5. Keep the existing safety/ref-update behavior from `migrate_store_ref_to_v1`, but point it at the new `v2` conversion.

**Targeted verification:**

- `cargo test -p beads-git wire`
- `cargo test -p beads-rs --test integration cli::migration::`

## Task 2: First-Class Local Daemon Store Handling (`beads-rs-ee69`)

**Files:**
- Modify: `crates/beads-core/src/store_meta.rs`
- Modify: `crates/beads-daemon/src/runtime/store/runtime.rs`
- Modify: `crates/beads-rs/src/cli/backend.rs`
- Modify: `crates/beads-cli/src/commands/migrate.rs`
- Test: daemon store runtime tests
- Test: integration migration/restart tests

**Steps:**

1. Decide and encode the local-store version bump needed for the rename cut.
2. Implement one intentional local-store path:
   - migrate command triggers reset/rebuild, or
   - startup performs reset/rebuild when canonical store is already migrated
3. Make the user-visible behavior explicit and actionable in CLI/runtime errors.
4. Add coverage that starts from an old local-store shape and proves successful rebuild from the repo store.
5. Keep strict failure for genuinely unknown/newer local-store versions that we cannot safely recover from.

**Must prove:**

- Users do not get stranded on `UnsupportedStoreMetaVersion` after a canonical store migration.
- Rebuild path is deterministic and derives only from canonical repo truth.

**Targeted verification:**

- `cargo test -p beads-daemon runtime::store::runtime::tests`
- one integration test that migrates repo store, then forces local-store rebuild

## Task 3: Core Rename To First-Class `status`

**Files:**
- Modify: `crates/beads-core/src/composite.rs`
- Modify: `crates/beads-core/src/bead.rs`
- Modify: `crates/beads-core/src/lib.rs`
- Modify: `crates/beads-core/src/apply.rs`
- Modify: `crates/beads-core/src/wire_bead.rs`
- Modify: `crates/beads-core/src/state/canonical.rs`
- Modify: `crates/beads-core/tests/support/event_body.rs`

**Steps:**

1. Rename `TrackerState` to the chosen canonical rich status type.
2. Rename `BeadFields::tracker_state` to `status`.
3. Keep terminal helpers (`is_terminal`, `closed_reason`) on the renamed type.
4. Delete `WorkflowStatus` as a public first-class type.
5. Convert any remaining coarse status logic into internal helper methods instead of public model types.

**Notes:**

- `closed_on_branch` still remains the separate terminal metadata field for now.
- Do not fold `closed_on_branch` into this cut unless it falls out trivially. That is a distinct architectural cleanup.

**Targeted verification:**

- `cargo test -p beads-core --lib`

## Task 4: Patch/Query/Filter Cutover

**Files:**
- Modify: `crates/beads-surface/src/ops.rs`
- Modify: `crates/beads-surface/src/query.rs`
- Modify: `crates/beads-surface/src/ipc/payload.rs`
- Modify: `crates/beads-surface/src/ipc/types.rs`
- Modify: `crates/beads-api/src/issues.rs`
- Modify: `crates/beads-api/src/tracker.rs`
- Modify: `crates/beads-daemon/src/runtime/mutation_engine.rs`
- Modify: `crates/beads-daemon/src/runtime/query_executor.rs`
- Modify: `crates/beads-daemon/src/runtime/tracker.rs`

**Steps:**

1. Remove `OpenInProgress` from update patches and let patches carry rich `status`.
2. Make filters typed around rich `status` where feasible.
3. Rename tracker request/response payload fields from `state` to `status`.
4. Make ordinary `Issue` / `IssueSummary` expose rich `status`.
5. Keep derived bucket/group logic private to query code where still useful.
6. Ensure list/count/stale/status overview behavior still defaults to hiding terminal issues.

**Important rule:**

- `blocked` stays a derived dependency/result grouping, not a status enum variant.

**Targeted verification:**

- `cargo test -p beads-daemon --lib`
- `cargo test -p beads-rs --test integration daemon::http::tracker_http_flow_projects_blockers_and_supports_tracker_mutations --features e2e-tests -- --exact`

## Task 5: CLI Surface Rewrite

**Files:**
- Modify: `crates/beads-cli/src/parsers.rs`
- Modify: `crates/beads-cli/src/filters.rs`
- Modify: `crates/beads-cli/src/commands/update.rs`
- Modify: `crates/beads-cli/src/commands/close.rs`
- Modify: `crates/beads-cli/src/commands/common.rs`
- Modify: `crates/beads-cli/src/commands/list.rs`
- Modify: `crates/beads-cli/src/commands/show.rs`
- Modify: `crates/beads-cli/src/commands/ready.rs`
- Modify: `crates/beads-cli/src/commands/prime.rs`
- Modify: `crates/beads-cli/src/render.rs`
- Test: `crates/beads-rs/tests/integration/cli/critical_path.rs`

**Steps:**

1. Make `bd update --status` accept rich statuses:
   - `todo`
   - `in_progress`
   - `human_review`
   - `rework`
   - `merging`
   - `done`
   - `cancelled`
   - `duplicate`
2. Keep `bd close` as a convenience wrapper for terminal outcomes.
3. Make default human/JSON issue rendering show rich `status`.
4. Keep `bd list` default terminal-hiding behavior via `status.is_terminal()`.
5. Update `bd prime` examples and any CLI help text to match the new vocabulary.

**Desired adjacent polish (do not block the rename if it grows):**

- allow `bd close <id> --reason duplicate --note "duplicate of beads-rs-123"` by treating note text as a separate comment/note path instead of overloading canonical reason parsing

**Targeted verification:**

- `cargo test -p beads-cli`
- `cargo test -p beads-rs --test integration --features e2e-tests cli::critical_path::test_create_show_close_workflow -- --exact`
- `cargo test -p beads-rs --test integration --features \"e2e-tests slow-tests\" cli::critical_path::test_update_bead -- --exact`

## Task 6: Symphony / Tracker Adapter Rename

**Files:**
- Modify: `vendor/github.com/openai/symphony/elixir/lib/symphony_elixir/beads/*.ex`
- Modify: `vendor/github.com/openai/symphony/elixir/lib/symphony_elixir/codex/dynamic_tool.ex`
- Modify: `scripts/e2e_symphony_beads.py`
- Modify: `scripts/test_e2e_symphony_beads.py`

**Steps:**

1. Update the beads client/adapter to consume renamed `status` fields.
2. Update any tracker-tool payload handling that still assumes `state`.
3. Keep the same lifecycle proof:
   - list/fetch issues
   - create follow-up
   - add blocker
   - create comment
   - update status
4. Re-run the preserved disposable proof against the migrated backend.

**Targeted verification:**

- `mise exec -- mix test test/symphony_elixir/dynamic_tool_test.exs test/symphony_elixir/app_server_test.exs test/symphony_elixir/workspace_and_config_test.exs test/symphony_elixir/core_test.exs test/symphony_elixir/extensions_test.exs test/symphony_elixir/orchestrator_status_test.exs`
- `mise exec -- mix specs.check`
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest /Users/darin/src/personal/beads-rs-symphony-api/scripts/test_e2e_symphony_beads.py -v`

## Task 7: Final Migration And Proof Matrix

**Files:**
- Modify if needed: `docs/plans/2026-04-19-beads-symphony-api-hardening.md`
- Modify if needed: migration/help docs that mention latest format version or status vocabulary

**Proof sequence:**

1. Start from a disposable repo store at `format_version = 1`.
2. Run `bd migrate detect` and verify it reports `1 -> 2`.
3. Run `bd migrate to 2`.
4. Verify canonical store files now emit `status`, not `tracker_state`.
5. Verify local daemon store handling:
   - rebuild/reset path happened, or
   - explicit migrate-managed reset completed
6. Run normal `bd list`, `bd show`, `bd update`, `bd close`, and tracker HTTP flows.
7. Run Symphony e2e harness on the migrated backend.

**Full verification gate:**

- `cargo fmt --all`
- `cargo test -p beads-core --lib`
- `cargo test -p beads-daemon --lib`
- `cargo test -p beads-git`
- `cargo test -p beads-cli`
- `cargo test -p beads-rs --test core_state_fingerprint`
- `cargo test -p beads-rs --test integration --features e2e-tests daemon::http::tracker_http_flow_projects_blockers_and_supports_tracker_mutations -- --exact`
- `cargo test -p beads-rs --test integration --features e2e-tests cli::critical_path::test_create_show_close_workflow -- --exact`
- `cargo test -p beads-rs --test integration --features "e2e-tests slow-tests" cli::critical_path::test_update_bead -- --exact`
- `cargo test -p beads-rs --test integration --features "e2e-tests slow-tests" cli::critical_path::test_reopen_closed -- --exact`
- `cargo test -p beads-rs --test integration --features "e2e-tests slow-tests" cli::migration::test_migrate_fixture_rich_workflow_rewrites_and_preserves_state -- --exact`
- `just dylint`
- `cargo clippy --all-features -- -D warnings`
- `mise exec -- mix test test/symphony_elixir/dynamic_tool_test.exs test/symphony_elixir/app_server_test.exs test/symphony_elixir/workspace_and_config_test.exs test/symphony_elixir/core_test.exs test/symphony_elixir/extensions_test.exs test/symphony_elixir/orchestrator_status_test.exs`
- `mise exec -- mix specs.check`
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest /Users/darin/src/personal/beads-rs-symphony-api/scripts/test_e2e_symphony_beads.py -v`
- `python3 /Users/darin/src/personal/beads-rs-symphony-api/scripts/e2e_symphony_beads.py --skip-build --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir`

## Done Checklist

- [x] `bd migrate detect` / `bd migrate to 2` exist and treat `2` as the latest canonical repo-store format
- [x] Legacy `v1` decode is confined to migration code, not normal runtime store loading
- [x] `beads-rs-ee69` lands or is explicitly subsumed by the migration flow in the same branch
- [x] Core/public/API/CLI surfaces use rich `status` as the one first-class state field
- [x] Coarse workflow buckets are no longer exposed as peer truth
- [x] `bd list` still defaults to hiding terminal issues
- [x] Tracker payloads and Symphony adapter use `status`, not `state`
- [x] Disposable migration proof successfully upgrades a `v1` store and rebuilds local daemon state
- [x] Symphony lifecycle harness passes against the migrated backend
