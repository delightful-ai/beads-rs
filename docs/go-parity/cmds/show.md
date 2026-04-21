# bd show

**Go source:** `cmd/bd/show.go` + `show_display.go`, `show_format.go`, `show_children.go`, `show_refs.go`, `show_thread.go`, `show_unit_helpers.go`
**Pin:** v1.0.2 (c446a2ef)
**Parity status:** `simplified` — beads-rs `bd show` exists but emits a different JSON envelope + field shape than Go. Command is implemented; wire compat pending (see `../GASCITY_SURFACE.md` for the full delta).
**Rust source:** `crates/beads-cli/src/commands/show.rs`

## Purpose

`bd show` reads one or more issues by ID and returns their full detail. This is the primary read path for both humans (`bd show ID`) and agent consumers (`bd show ID --json`). It establishes the canonical `Issue` JSON shape that every other command's output references.

The command has several modes:
- **Default:** one or more IDs as positional args, one output row per ID.
- **`--current`:** no ID — resolve to the currently active issue (in-progress, hooked, or last-touched).
- **`--children`:** emit the given issue's children rather than the issue itself.
- **`--refs`:** reverse lookup — show issues that reference this one.
- **`--thread`:** for message-type issues, emit the full conversation thread.
- **`--watch`:** tail-style live refresh (human output only).

## Invocation

```
bd show [id...] [--id=<id>...] [--current] [flags]
```

Aliases: `show`, `view`.

## Arguments

| Name | Type | Required | Semantics |
|------|------|----------|-----------|
| `id...` | positional strings | one of `id...`, `--id`, or `--current` | Issue IDs to show. Multiple IDs return a JSON array. |

## Flags

Captured from `/tmp/go-bd-playground/bd-go show --help` against v1.0.2.

| Flag | Type | Default | Semantics |
|------|------|---------|-----------|
| `--as-of` | string | — | "Show issue as it existed at a specific commit hash or branch (requires Dolt)" |
| `--children` | bool | false | "Show only the children of this issue" |
| `--current` | bool | false | "Show the currently active issue (in-progress, hooked, or last touched)" |
| `--id` | stringArray | — | "Issue ID (use for IDs that look like flags, e.g., --id=gt--xyz)" |
| `--local-time` | bool | false | "Show timestamps in local time instead of UTC" |
| `--long` | bool | false | "Show all available fields (extended metadata, agent identity, gate fields, etc.)" |
| `--refs` | bool | false | "Show issues that reference this issue (reverse lookup)" |
| `--short` | bool | false | "Show compact one-line output per issue" |
| `--thread` | bool | false | "Show full conversation thread (for messages)" |
| `-w, --watch` | bool | false | "Watch for changes and auto-refresh display" |

Global flags that affect output: `--json` (machine output), `-q/--quiet`, `-v/--verbose`.

Notes on non-obvious behavior:

- `--as-of` is **Dolt-specific**. In beads-rs this flag is declined (see parity notes); the CRDT log provides equivalent functionality through replica/stamp queries but they don't match a Dolt commit hash.
- `--long` surfaces **extended metadata only when present**. On a minimal issue (newly created, no labels, no gate fields), the `--long` and default JSON are identical. On a richer issue the additional fields appear — `labels`, `started_at`, gate metadata, slots, etc.
- `--short` produces a human-readable one-liner, not a narrower JSON shape. With `--json`, the output is identical to default.

## Output (live capture)

### Single open task, minimal fields, default shape

```bash
$ cd /tmp/go-bd-play && /tmp/go-bd-playground/bd-go show go-bd-play-9m1 --json
```

```json
[
  {
    "id": "go-bd-play-9m1",
    "title": "seed issue for json capture",
    "description": "used to validate show output shape",
    "status": "open",
    "priority": 2,
    "issue_type": "task",
    "owner": "darinkishore@protonmail.com",
    "created_at": "2026-04-20T18:48:58Z",
    "created_by": "Darin Kishore",
    "updated_at": "2026-04-20T18:48:58Z"
  }
]
```

Note: `bd show --json` **always returns a JSON array**, even for a single ID. Compare to `bd create` which returns a bare object.

### Richer issue (epic, labels, design + acceptance)

```bash
$ /tmp/go-bd-playground/bd-go show go-bd-play-8mz --long --json
```

```json
[
  {
    "id": "go-bd-play-8mz",
    "title": "issue with more stuff",
    "description": "body",
    "design": "design body",
    "acceptance_criteria": "- [ ] one\\n- [ ] two",
    "status": "open",
    "priority": 1,
    "issue_type": "epic",
    "owner": "darinkishore@protonmail.com",
    "created_at": "2026-04-20T18:49:09Z",
    "created_by": "Darin Kishore",
    "updated_at": "2026-04-20T18:49:09Z",
    "labels": [
      "bar",
      "foo"
    ]
  }
]
```

Labels appear sorted lexicographically.

### After `bd update --status in_progress` — `started_at` appears

```bash
$ /tmp/go-bd-playground/bd-go show go-bd-play-9m1 --long --json
```

```json
[
  {
    "id": "go-bd-play-9m1",
    "title": "seed issue for json capture",
    ...
    "status": "in_progress",
    "updated_at": "2026-04-20T18:49:15Z",
    "started_at": "2026-04-20T18:49:15Z"
  }
]
```

`started_at` (added in v1.0.1) is populated on the first transition into `in_progress` and never updated afterward. It's omitted when the issue has never entered `in_progress`.

### Multi-ID invocation returns an array in the order given

```bash
$ /tmp/go-bd-playground/bd-go show go-bd-play-9m1 go-bd-play-8mz --json
```

Returns `[issue1, issue2]` in argument order.

### Error case: unknown ID

```bash
$ /tmp/go-bd-playground/bd-go show nonexistent-id --json; echo "EXIT=$?"
```

```
Error fetching nonexistent-id: no issue found matching "nonexistent-id"
{
  "error": "no issues found matching the provided IDs"
}
EXIT=1
```

Two artifacts: a human-readable `Error fetching ...` line on stderr AND a JSON error envelope on stdout. Exit code is `1`.

## Data model impact

- **Reads:** the `issues` table, plus labels via join when needed for output. `--long` additionally reads extended metadata slots, gate fields, agent identity, and convergence.* keys (v1.0.0+). `--refs` reads the `dependencies` table in reverse-edge direction. `--children` reads by `parent_id`.
- **Writes:** none. This is a pure read command.
- **Invariants enforced:** none.
- **Hooks fired:** none. Reads are not hooked.

## Error cases

- **Unknown ID(s):** stderr line per missing ID + stdout `{"error": "no issues found matching the provided IDs"}` if ALL given IDs are missing. Exit 1.
- **No ID given without `--current` or `--id`:** usage error, exit 2.
- **`--as-of <ref>` against a non-Dolt backend:** error indicating `--as-of` requires Dolt history.
- **`--thread` on a non-message issue:** treated as equivalent to default (no error); message threading is only meaningful for `issue_type=message`.

## Rust parity notes

### Current state (v0.2.0-alpha)

beads-rs has `bd show` and it works. The gap is wire-shape, not capability. Key divergences:

| Dimension | Go v1.0.2 | beads-rs | Decision |
|-----------|-----------|----------|----------|
| Envelope | bare array `[{...}]` | wrapped `{"result": "issue"\|"issues", "data": ...}` | **Drop the wrapper for gascity-shaped commands** — user decided to adopt Go's CLI shape. |
| Type field | `"issue_type"` | `"type"` (serde-renamed internally) | **Rename the wire serialization to `"issue_type"`** — keep internal `type` field, serde-rename only. |
| Timestamps | RFC3339 string | `{wall_ms: u64, counter: u32}` | **Emit RFC3339 on the wire** — keep HLC `WriteStamp` as the internal ordering primitive; lossy wire is fine, domain model stays rich. |
| Assignee | `"owner"` (email, v1.0.0 rename) | `"assignee"` | **Rename wire to `"owner"`** — matches Go v1.0.0+ convention. |
| Dependencies | flattened to full issue objects with `dependency_type` grafted | `deps_incoming[]` / `deps_outgoing[]` with `{from, to, kind}` shape | **Emit flattened Go shape on wire** — keep the directional split internally (it's the right model), project to Go's shape at the wire layer. |
| Metadata | `metadata{}` arbitrary JSON | absent | **Stays absent** — see `../primitives/metadata-remapping.md` (typed fields on typed variants replace metadata). Keys gascity uses surface as typed enums on typed bead variants. |
| Namespace | implicit in ID prefix | explicit `"namespace"` field | Keep Rust's explicit namespace; Go doesn't have first-class namespaces. See `../primitives/namespaces.md`. |
| Content hash | absent | `"content_hash"` always present | Keep — load-bearing for CRDT dedup + sync. |

### Forcing functions

- **Wire vs domain are different layers.** `WriteStamp` stays `{wall_ms, counter}` internally; the wire serialization emits RFC3339. Losing the counter dimension on the wire is acceptable because consumers who need causality query the daemon directly, not parse wire JSON.
- **No metadata field.** Intentional — see `../primitives/metadata-remapping.md` and the per-type files under `../types/`. Every key gascity writes (`convergence.*`, `gc.*`, `convoy.*`, session state, extmsg records) has a typed home on a typed bead variant.

### Parity path

Gascity-compat depends on beads-rs work not in this file:

1. **Envelope + field-rename wire layer** — top-priority structural CLI work (see `../GASCITY_SURFACE.md §0`).
2. **Open BeadType enum** so `--type=molecule` (etc.) doesn't error. Prerequisite for any custom-type consumer.
3. **Namespace cross-ref fix** so `show` of a bead whose dep points at another namespace returns coherent deps, not orphans. See `../primitives/namespaces.md`.
4. **`--as-of`** declined — CRDT stamps don't map to Dolt commit hashes. Equivalent beads-rs path is query at a specific `WriteStamp`.
5. **`--current`** requires session-state lookup; blocked on the sessions namespace landing.
6. **`--thread`** requires `Message` type + `replies-to` dep kind (see `../types/message.md`).

### What matches today

- Positional multi-ID invocation works.
- `--json` flag honored.
- Core field set (id, title, description, status, priority, labels, assignee, created_at, updated_at) populated.
- Unknown-ID error path has a JSON envelope and non-zero exit.
- Hierarchical child IDs (`rs-id-test-vd0.1`) parse and display correctly — confirmed via `BeadId::parse` at `crates/beads-core/src/identity.rs:535-536`.
