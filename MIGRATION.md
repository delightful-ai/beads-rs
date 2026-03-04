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

1. Detect format/invariants:

   ```bash
   bd migrate detect --json
   ```

   Example signals:
   - `"deps_format":"legacy_edges"`
   - `"notes_present":false`
   - `"checksums_present":false`
   - `"needs_migration":true`

2. Preview migration (no writes):

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

### Rollback snippet

```bash
git show-ref refs/heads/beads/store >/dev/null 2>&1 && \
  git branch beads/store-backup refs/heads/beads/store

# ... run migration ...

# restore if needed
git show-ref refs/heads/beads/store-backup >/dev/null 2>&1 && \
  git update-ref refs/heads/beads/store refs/heads/beads/store-backup
```

### Compatibility note

This is a hard cutover. Older binaries that expect line-per-edge `deps.jsonl`
will not be able to read the migrated store.
