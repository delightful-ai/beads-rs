# Migration: beads-go → beads-rs

This guide covers migrating an existing beads-go dataset into beads-rs.

## Prereqs

- You have a git repo (the one you want beads to live in).
- You have an `origin` remote configured (recommended; local-only works, but sync won’t).
- You have a beads-go JSONL export file (`issues.jsonl`).

## Quick path

1. Install `bd` (beads-rs) and ensure it’s on your `PATH`.
2. In the target repo, run a dry run:

   ```bash
   bd migrate from-go --input /path/to/issues.jsonl --dry-run
   ```

3. If the report looks right, run the real import:

   ```bash
   bd migrate from-go --input /path/to/issues.jsonl
   ```

   If you want to rewrite the ID prefix during import (e.g. to `bd-…` or to a different repo slug):

   ```bash
   bd migrate from-go --input /path/to/issues.jsonl --root-slug myrepo
   ```

4. Verify:

   ```bash
   bd list
   bd ready
   bd show <some-id>
   ```

Notes:
- If `refs/heads/beads/store` already exists, `bd migrate …` will refuse unless you pass `--force` (it will merge the imported state into the existing store).
- Use `--no-push` if you want to inspect locally before pushing to `origin`.

## What changes (data model)

beads-rs stores canonical state in git on `refs/heads/beads/store` (separate from your code branches). The store contains JSONL files like `state.jsonl`, `tombstones.jsonl`, and `deps.jsonl`.

During `from-go` import:

- **IDs**: preserved (including hierarchical IDs like `bd-epic.1`).
- **Types**: `task`, `bug`, `feature`, `epic`, `chore` are mapped directly.
- **Status/workflow**:
  - `open` → open
  - `in_progress` → in progress
  - `closed` → closed (close reason is preserved when present)
  - `blocked` is treated as open
  - unknown/custom statuses are treated as open
- **Assignee/claim**: preserved; if an issue is claimed but its status is `open`, beads-rs will treat it as `in_progress` to match the invariant “claimed implies in progress”.
- **Labels**: preserved.
- **Dependencies**: imported when possible; supported kinds include `blocks`, `related`, `parent-child`, and `discovered-from`.
- **Comments/notes**:
  - beads-go `notes` (single string) becomes a synthetic note (`legacy-notes`)
  - beads-go `comments[]` are imported as notes

The importer emits a warnings list for anything it had to skip or normalize (e.g. malformed deps, mismatched comment IDs).

## What changes (CLI + workflow)

beads-rs is designed for many agents working concurrently:

- A local daemon holds canonical state in memory and syncs in the background.
- Most commands talk to the daemon over a unix socket; the CLI auto-starts it on demand.
- `bd sync` is effectively “wait for flush” (not a required workflow step).

## Rollback / safety

The canonical store lives on its own git ref. If you want a “checkpoint” before importing, create a backup ref:

```bash
git show-ref refs/heads/beads/store >/dev/null 2>&1 && \
  git branch beads/store-backup refs/heads/beads/store
```

## Legacy Deps Cutover (`deps.jsonl` line-per-edge -> OR-Set v1)

Some historical repos have `refs/heads/beads/store` with legacy `deps.jsonl`
that stores one edge per JSON line. Current runtime loads expect strict OR-Set
deps (`WireDepStoreV1` with `cc` + `entries`). Strict canonical load paths can
fail with errors like:

```text
missing field `cc`
```

Checkpoint import handles this differently: legacy checkpoint deps are
classified as incompatible, skipped, and rebuilt from canonical store state.

Use the explicit migration flow:

`bd migrate to` currently supports only the latest format target (`1`).
Any other target version is rejected.

Before any pushed cutover, upgrade every daemon/CLI in the fleet to a binary
that understands strict OR-Set v1 `deps.jsonl`. This migration is not backward
compatible for older clients that still expect legacy line-per-edge deps.

1. Detect format/invariants:

   ```bash
   bd migrate detect --json
   ```

   Example signals:
   - `"deps_format":"legacy_edges"`
   - `"notes_present":false`
   - `"checksums_present":false`
   - `"needs_migration":true`

2. Preview migration (no local ref writes):

   ```bash
   bd migrate to 1 --dry-run --json
   ```

3. Execute migration:

   ```bash
   bd migrate to 1 --json
   ```

   Optional flags:
   - `--no-push` to rewrite locally only
   - `--force` to continue on safety checks (for example divergence or warningful legacy parses)

Migration rewrites `refs/heads/beads/store` with canonical v1 files:
`state.jsonl`, `tombstones.jsonl`, `deps.jsonl`, `notes.jsonl`, `meta.json`.
`deps.jsonl` is rewritten to strict OR-Set shape, and `meta.json` checksums are
backfilled.

### Per-repo playbook

Recommended sequence for a single repository:

1. Inventory current shape:

   ```bash
   bd migrate detect --json
   ```

2. Preview the rewrite and warnings:

   ```bash
   bd migrate to 1 --dry-run --json
   ```

   For a normal cutover preview, expect `"push": "skipped_no_push"` because
   dry-run never creates a commit, updates `refs/heads/beads/store`, or mutates
   the caller repo's tracking refs. It may still contact `origin` to preview the
   current remote store state.

   If local and remote store histories are unrelated, dry-run refuses with the
   same `no common ancestor between local and remote` safety error that execute
   would return unless you also pass `--force`.

3. Execute the rewrite:

   ```bash
   result="$(bd migrate to 1 --json)"
   printf '%s\n' "$result"
   printf '%s\n' "$result" |
     jq -e '(.push == "pushed") or (.push == "skipped_no_push" and .commit_oid == null)' >/dev/null
   ```

   Normal outcomes:
   - `"push": "pushed"` with non-null `commit_oid`: this repo performed a fresh pushed cutover.
   - `"push": "pushed"` with `commit_oid: null`: this repo was already canonical locally, but published the missing remote `beads/store` ref to `origin`.
   - `"push": "skipped_no_push"` with `commit_oid: null`: this repo was already converged on canonical content, so no new commit or push was needed.

   Non-fleet outcomes:
   - `--no-push` rehearsal: expect `"push": "skipped_no_push"` with a local-only rewrite.
   - no remote push target: expect `"push": "skipped_no_remote"` only when there is no `origin`.

4. Verify the result:

   ```bash
   bd migrate detect --json
   bd list >/dev/null
   bd ready >/dev/null
   bd blocked >/dev/null
   git fetch origin refs/heads/beads/store:refs/remotes/origin/beads/store
   ```

Expected verification signals:
- `bd migrate detect --json` reports `"needs_migration": false`
- strict store-load commands like `bd list`, `bd ready`, and `bd blocked` succeed
- if you pushed, the explicit fetch above succeeds and leaves local/remote store refs aligned

`bd sync` is still useful as a local flush sanity check, but it is not
independent proof that another clone can read the migrated store.

### Backup refs and rollback

When a local `refs/heads/beads/store` already exists, migration creates a
backup ref under `refs/beads/backup/<oid>` before rewriting the local store ref.
You can inspect retained backups with:

```bash
git for-each-ref refs/beads/backup --format='%(refname) %(objectname)'
```

If you want an explicit manual restore handle before running migration, create
one yourself and keep it until verification passes:

```bash
if git show-ref refs/heads/beads/store >/dev/null 2>&1; then
  git branch beads/store-backup refs/heads/beads/store
else
  git fetch origin refs/heads/beads/store:refs/remotes/origin/beads/store
  git branch beads/store-backup refs/remotes/origin/beads/store
fi
```

If you may need to roll back after a pushed cutover, treat that explicit manual
restore handle as a precondition. Automatic `refs/beads/backup/<oid>` refs are
local-only and may not exist in remote-only migration cases.

#### Local-only rollback

If the migration has not been pushed yet (`"push": "skipped_no_push"` or
`"push": "skipped_no_remote"`), restoring the local store ref is enough. If you
made an explicit manual restore handle first, use it directly:

```bash
git show-ref refs/heads/beads/store-backup >/dev/null 2>&1 &&
  git update-ref refs/heads/beads/store refs/heads/beads/store-backup
```

If you relied on the automatic backup refs instead, restore from the newest
local backup ref that migration created before moving your local store ref:

```bash
backup_ref="$(git for-each-ref refs/beads/backup --sort=-creatordate --format='%(refname)' | head -n1)"
test -n "$backup_ref" &&
  git update-ref refs/heads/beads/store "$backup_ref"
```

For a remote-only `--no-push` rehearsal there was no local store ref to
restore. In that case, roll back by deleting the materialized local ref:

```bash
git update-ref -d refs/heads/beads/store
```

#### Pushed rollback

If the migration already reported `"push": "pushed"`, a local `git update-ref`
is not sufficient. You must restore `origin` as well, otherwise other clones
remain on the migrated store ref.

One explicit rollback flow is:

```bash
backup_oid="$(git rev-parse refs/heads/beads/store-backup)"
git update-ref refs/heads/beads/store "$backup_oid"
git push --force-with-lease origin "$backup_oid:refs/heads/beads/store"
git fetch origin refs/heads/beads/store:refs/remotes/origin/beads/store
```

Use pushed rollback only with operator coordination: every clone that already
fetched the migrated store ref must fetch again before continuing normal work.


### Fleet rollout

Dry-run-first inventory across many repos:

```bash
while read -r repo; do
  echo "== $repo =="
  (
    cd "$repo" &&
    bd migrate detect --json &&
    bd migrate to 1 --dry-run --json
  )
done < repos.txt
```

After reviewing the inventory, run the actual rewrite only for the repos you
intend to cut over:

```bash
while read -r repo; do
  echo "== $repo =="
  (
    cd "$repo" &&
    result="$(bd migrate to 1 --json)" &&
    printf '%s\n' "$result" &&
    printf '%s\n' "$result" |
      jq -e '(.push == "pushed") or (.push == "skipped_no_push" and .commit_oid == null)' >/dev/null &&
    bd migrate detect --json &&
    bd list >/dev/null &&
    bd ready >/dev/null &&
    bd blocked >/dev/null &&
    git fetch origin refs/heads/beads/store:refs/remotes/origin/beads/store &&
    test "$(git rev-parse refs/heads/beads/store)" = "$(git rev-parse refs/remotes/origin/beads/store)"
  )
done < repos-to-migrate.txt
```

If you want a local-only rehearsal first, do not reuse the pushed-cutover loop
unchanged. A real `--no-push` rewrite returns `"push": "skipped_no_push"` with
a non-null `commit_oid`, and local/remote store refs are intentionally *not*
equal afterward.

One rehearsal-only loop is:

```bash
while read -r repo; do
  echo "== $repo =="
  (
    cd "$repo" &&
    result="$(bd migrate to 1 --no-push --json)" &&
    printf '%s\n' "$result" &&
    printf '%s\n' "$result" |
      jq -e '(.push == "skipped_no_push") and (.commit_oid != null)' >/dev/null &&
    bd migrate detect --json &&
    bd list >/dev/null &&
    bd ready >/dev/null &&
    bd blocked >/dev/null
  )
done < repos-to-migrate.txt
```

Do not treat `"skipped_no_remote"` as fleet rollout success. A plain
`"skipped_no_push"` only counts as pushed-cutover success when `commit_oid` is
`null`, which means the repo was already converged and there was nothing left
to push.

### Compatibility and caveats

This is a hard cutover. Older binaries that expect line-per-edge `deps.jsonl`
will not be able to read the migrated store.

Other operator-facing tradeoffs:
- legacy per-edge dependency provenance is collapsed into the canonical OR-Set snapshot during rewrite
- deleted-edge fidelity is limited to what the legacy store actually encoded; migration cannot recover historical intent that was never stored
- incompatible legacy checkpoint payloads do not need manual cleanup before migration; runtime ignores them and rebuilds fresh checkpoints from canonical store state after cutover
