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
deps with `cc` and `entries`. When an old local or remote-tracking ref is still
on the legacy shape, strict load paths can fail with errors like:

```text
missing field `cc`
```

Use the explicit migration flow:

1. Detect structural state:

   ```bash
   bd migrate detect
   ```

   Important fields in the JSON:
   - `"deps_format":"legacy_edges"` means deps still need cutover
   - `"notes_present":false` means `notes.jsonl` will be backfilled
   - `"checksums_present":false` means `meta.json` checksums will be backfilled
   - `"needs_migration":true` means at least one store ref still needs rewrite

2. Preview the rewrite:

   ```bash
   bd migrate to 1 --dry-run
   ```

3. Execute the rewrite:

   ```bash
   bd migrate to 1
   ```

   Useful flags:
   - `--no-push` rewrites only the local ref
   - `--force` continues through warningful legacy parses or divergent local/remote history

`bd migrate to 1` rewrites `refs/heads/beads/store` into canonical v1 shape:
`state.jsonl`, `tombstones.jsonl`, `deps.jsonl`, `notes.jsonl`, and `meta.json`.
It converts legacy line-per-edge deps to strict OR-Set deps, backfills missing
`notes.jsonl`, and writes store checksums into `meta.json`.

Checkpoint imports handle legacy deps differently: incompatible checkpoint deps
are skipped and rebuilt from canonical store state rather than being parsed in
runtime compatibility mode.

### Rollback snippet

```bash
git show-ref refs/heads/beads/store >/dev/null 2>&1 && \
  git branch beads/store-backup refs/heads/beads/store

# ... run migration ...

git show-ref refs/heads/beads/store-backup >/dev/null 2>&1 && \
  git update-ref refs/heads/beads/store refs/heads/beads/store-backup
```

### Compatibility note

This is a hard cutover. Older binaries that expect line-per-edge `deps.jsonl`
will not be able to read the migrated store after `bd migrate to 1`.
