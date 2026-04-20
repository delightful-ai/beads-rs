# bd show

**Go source:** `cmd/bd/show.go` + `show_display.go`, `show_format.go`, `show_children.go`, `show_refs.go`, `show_thread.go`, `show_unit_helpers.go`
**Pin:** v1.0.2 (c446a2ef)
**Parity status:** `deferred` — beads-rs `bd show` exists but emits a different JSON shape (see Rust parity notes).
**Rust source:** `crates/beads-cli/src/commands/show/` (diverged)

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

### Current divergence (v0.2.0-alpha)

beads-rs's `bd show` emits a **different JSON shape** than Go. Key differences from the beads-rs `Issue` schema (`crates/beads-api/src/issues.rs`):

| Field | Go v1.0.2 | beads-rs | Notes |
|-------|-----------|----------|-------|
| type | `issue_type` | `type` (serde-renamed) | Go uses longer key; Rust matches internal field name. |
| timestamps | RFC3339 string | `{wall_ms: u64, counter: u32}` | Rust emits structured HLC `WriteStamp`; breaks RFC3339 parsing on Go consumers. |
| assignee | `owner` (email) | `assignee` (string) | Go v1.0.0 renamed to `owner` in output; Rust kept `assignee`. |
| dependencies | flattened when present | `deps_incoming` / `deps_outgoing` arrays | Rust splits direction into two typed lists. |
| metadata | absent from default output; `--long` surfaces slots | no metadata field at all | Rust has no metadata primitive yet (see `primitives/slots.md`). |
| namespace | implicit | `namespace` always present | Rust has first-class namespaces; Go uses prefix-in-ID. |
| content_hash | absent | `content_hash` always present | Rust exposes CRDT content hash for dedup/sync. |

### Forcing functions

- **`WriteStamp` is not RFC3339.** beads-rs's ordering primitive is an HLC pair `(wall_ms, counter)`, deliberately exposed on the wire so consumers can reason about causality without parsing timestamps back into HLC ordering. Emitting RFC3339 would lose the counter dimension and make "same wall_ms different counter" events indistinguishable — which is the exact problem HLCs exist to solve.
- **CRDT content hash is load-bearing.** `content_hash` is how replicas confirm they agree on a bead's state without full comparison. Omitting it from output would hide useful consumer-side deduplication.
- **No metadata field yet.** This is the direct mapping question this spec subtree exists to answer. Go's Slots (v1.0.0+) is the closest concept — see `primitives/slots.md` (to be written).

### Parity path

To reach `faithful` status, beads-rs needs (in priority order):

1. **Compat JSON mode** on read commands: `--wire=go` or similar, emitting RFC3339 timestamps, `issue_type`/`owner` field names, flattened `dependencies[]`. This is the gascity unblocker.
2. **Slots primitive** to host consumer-owned scratch, matching Go's `SlotSet`/`SlotGet`/`SlotClear` shape.
3. **`--as-of` declined explicitly** — CRDT stamps don't map to Dolt commit hashes. Document the equivalent beads-rs path (query at a specific `WriteStamp`).
4. **`--current`** requires a session-state concept beads-rs doesn't have yet. Deferred until hooks/sessions land.
5. **`--thread`** depends on message type + replies-to dependency edges, both deferred.

### What matches today

- Positional multi-ID invocation works.
- `--json` flag honored.
- Core field set (id, title, description, status, priority, labels, assignee) populated.
- Unknown-ID error path has a JSON envelope and non-zero exit.
