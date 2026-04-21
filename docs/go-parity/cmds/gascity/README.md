# Gascity Surface — Parity Backlog

**Pin:** Go `bd` v1.0.2 (`c446a2ef`) vs. beads-rs at current HEAD (`bd 0.2.0-alpha`).
**Scope:** the 14 commands gascity actually invokes from `internal/beads/bdstore.go`, `internal/beads/bdstore_graph_apply.go`, and `internal/doctor/checks_custom_types.go`, plus the JSON wire shape each side produces. Nothing else.

Every per-command file in this directory contains live JSON captures from fresh `/tmp/gap-go` and `/tmp/gap-rs` playgrounds on 2026-04-20. Do not rely on paraphrased shapes; re-run the captures if refreshing the pin.

This directory replaces the monolithic `GASCITY_SURFACE.md`. Each command gets its own file so individual porting work can close a file at a time.

## Files

- [`00-envelope.md`](00-envelope.md) — global JSON envelope divergence, status field mapping, decision record
- [`01-init.md`](01-init.md) — `bd init --server -p <prefix> --skip-hooks [--server-host H] [--server-port P]`
- [`02-config.md`](02-config.md) — `bd config set <key> <value>` + `bd config get --json <key>`
- [`03-purge.md`](03-purge.md) — `bd purge --json [--dry-run]`
- [`04-create.md`](04-create.md) — `bd create --json <title> -t <type> [...]`
- [`05-create-graph.md`](05-create-graph.md) — `bd create --graph <file> --json`
- [`06-show.md`](06-show.md) — `bd show --json <id>`
- [`07-update.md`](07-update.md) — `bd update --json <id> [...]`
- [`08-list.md`](08-list.md) — `bd list --json [...]`
- [`09-close.md`](09-close.md) — `bd close --json <id1> [<id2>...]` (batch)
- [`10-delete.md`](10-delete.md) — `bd delete --force --json <id>`
- [`11-ready.md`](11-ready.md) — `bd ready --json --limit 0`
- [`12-dep-add.md`](12-dep-add.md) — `bd dep add <issueID> <dependsOnID> --type <depType>`
- [`13-dep-remove.md`](13-dep-remove.md) — `bd dep remove <issueID> <dependsOnID>`
- [`14-dep-list.md`](14-dep-list.md) — `bd dep list <id(s)> [--json] [--direction=up]`
- [`summary.md`](summary.md) — counts + critical blockers in implementation order

## Critical blockers (from `summary.md`, in implementation order)

1. `bd config set` / `bd config get --json types.custom` — doctor check runs every `gc doctor`
2. Custom bead types (close `BeadType` enum to validated string; see `../../primitives/custom-types.md` — to be written)
3. Wire-compat: envelope drop, `issue_type` rename, RFC3339 timestamps, `dependency_type` on dep-list rows
4. `bd dep list` subcommand (missing entirely)
5. `bd dep add --type` alias + `bd dep remove` alias for `rm`
6. `bd create --graph <file>` (atomic molecule instantiation)
7. `bd close id1 id2 ...` batch + `bd ready` bare-array output
8. Metadata-flag replacements (see `../../primitives/metadata-remapping.md`)
9. `bd purge` (implement, stub, or decline)
10. `bd init` flag tolerance + fix "no origin remote" fatal

## Related docs

- [`../../GASCITY_SURFACE.md`](../../GASCITY_SURFACE.md) — the previous monolithic version, retained as a single-file reference. **Prefer the per-command files here for editing.**
- [`../../primitives/dep-kinds.md`](../../primitives/dep-kinds.md) — dep-kind enumeration
- [`../../primitives/metadata-remapping.md`](../../primitives/metadata-remapping.md) — typed-field replacement for every gascity metadata key
- [`../../primitives/namespaces.md`](../../primitives/namespaces.md) — namespace architecture for sessions / extmsg / wisps
