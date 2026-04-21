## 4. `bd create --json <title> -t <type> [--priority/--description/--assignee/--deps/--labels/--parent/--metadata]`

**Invocation** (`bdstore.go:387-449`): sequential arg construction; `--metadata` is a JSON-encoded `map[string]string`.

### Go v1.0.2 behavior

```json
{
  "id": "gp-e4f",
  "title": "Hello world",
  "status": "open",
  "priority": 2,
  "issue_type": "task",
  "owner": "darinkishore@protonmail.com",
  "created_at": "2026-04-20T19:47:01.944638Z",
  "created_by": "Darin Kishore",
  "updated_at": "2026-04-20T19:47:01.944638Z"
}
```

### beads-rs behavior

```json
{
  "result": "issue",
  "data": {
    "id": "gap-rs-8vy",
    "namespace": "core",
    "title": "Hello world",
    "description": "",
    "design": null,
    "acceptance_criteria": null,
    "status": "open",
    "priority": 2,
    "type": "task",
    "labels": [],
    "assignee": null,
    "assignee_at": null,
    "assignee_expires": null,
    "created_at": { "wall_ms": 1776712314483, "counter": 1 },
    "created_by": "darin@darinsmcstudio2.lan",
    "created_on_branch": null,
    "updated_at": { "wall_ms": 1776712314483, "counter": 1 },
    "updated_by": "darin@darinsmcstudio2.lan",
    "closed_at": null,
    "closed_by": null,
    "closed_reason": null,
    "closed_on_branch": null,
    "external_ref": null,
    "source_repo": null,
    "content_hash": "09ff781c0800feea62cca2714b70bc27b04e9dd884393b74029168bb31d63116",
    "notes": []
  }
}
```

Also emits a human warning prefix (`⚠ Creating issue 'Hello world' without description.`). `extractJSON` in gascity skips preamble so this is tolerated, but it contaminates scripted callers.

### Flag delta

| Flag | Go v1.0.2 | Rust | Gap |
|------|-----------|------|-----|
| `<title>` positional | yes | yes | faithful |
| `-t/--type` | yes (incl. custom types via `types.custom`) | yes (BUT only `bug|feature|task|epic|chore`, no custom types) | Rust rejects `molecule`, `convoy`, etc. — fatal for gascity. |
| `--priority` (accepts 0-4 or P0-P4) | yes | yes (0-4 or words like "high"; does it accept `P0`?) | Must verify; gascity passes `strconv.Itoa(*b.Priority)` so always 0-4 numeric. |
| `--description` | yes | yes | faithful |
| `--assignee` | yes | yes (compat: only current actor) | Rust's `--assignee` silently binds to the invoking actor regardless of value passed. Gascity passes an agent name like `agent-ralph`; this will mis-attribute. |
| `--deps` | `strings` (comma-joined or repeated `type:id,id`) | yes (same format) | faithful |
| `--labels` | comma-joined string | yes | faithful |
| `--parent` | yes | yes (adds parent dep) | faithful semantically |
| `--metadata <json>` | yes (JSON-encoded map) | **missing** | Rust does not accept `--metadata` — by design (no freeform map). See metadata remapping below. |
| `--graph` | yes | missing | See command 5. |
| `--json` (global) | flag (no value) | `[<JSON>]` bool (accepts `--json` as implicit true) | Syntactic difference: Rust's `--json` is a boolean with optional argument; passing `--json <next-positional>` parses `<next-positional>` as the value. Gascity passes `--json` as a bare flag which works. |

### JSON field delta

| Field | Go shape | Rust shape | Gap |
|-------|----------|------------|-----|
| envelope | bare object | `{"result":"issue","data":{...}}` | Wrapper. |
| `id` | string | string | faithful |
| `title` | string | string | faithful |
| `status` | string | string | faithful |
| `priority` | int (2 when unset) | int (2 when unset) | faithful |
| `issue_type` vs `type` | `"issue_type": "task"` | `"type": "task"` | **Rename**. Gascity reads `issue_type`. |
| `created_at` | RFC3339 string | `{wall_ms, counter}` | **Shape**. Gascity's `time.Time` parser cannot read the HLC tuple. |
| `updated_at` | RFC3339 string | `{wall_ms, counter}` | **Shape**, same as above. |
| `owner` | string | — | Gascity reads `owner` but falls back to `metadata.from` (see `toBead` in bdstore.go:337-360). Rust has no `owner`. |
| `created_by` / `updated_by` | string | string | faithful (naming matches) |
| `description` | present when passed | always present (empty string when absent) | extended; tolerated |
| `labels` | absent when empty | `[]` when empty | extended; tolerated |
| `metadata` | map (omitted when empty) | absent | Gascity reads metadata; see remapping notes. |
| `parent` | string (bead id) | **absent** (only in `notes`/derived) | Gascity reads `parent` from top-level. |
| `ref` | string (formula step id) | — | Gascity reads `ref`, falls back to empty. Not load-bearing for generic tasks. |
| `needs` | `[]string` | — | Gascity reads `needs`. |
| `dependencies` | `[]Dep` | — | Gascity reads; missing on Rust create response. |

### Required work in beads-rs

1. **Rename JSON field `type` → `issue_type`** (or add both) on all wire-level issue shapes. Owner: `beads-api::wire_bead` or wherever the issue JSON projection lives. This is the single highest-leverage field change in the whole doc.
2. **Emit RFC3339 timestamps for `created_at`/`updated_at`/`closed_at`** in the Go-compat wire mode. Preserve the HLC internally; project to a wall-clock string on output. Decision: probably a `to_go_compat_json()` projection on `WireBead` + a CLI format gate.
3. **Drop the `{"result":"issue","data":...}` wrapper** when in compat mode; emit the bare object.
4. **Accept custom bead types.** Block: Rust's `BeadType` enum (crates/beads-core/src/domain.rs:17-23) is closed. Need a path for user-registered types:
   - Either (a) add an `Other(ValidatedCustomTypeName)` variant to `BeadType`,
   - or (b) make `BeadType` a validated string wrapper with a `is_well_known()` predicate (matching Go's pattern in types.go:806-827).
   Tracking in `primitives/custom-types.md`.
5. **Wire `--assignee <name>` to set the assignee, not implicitly to the current actor.** Rust's current "compat; only supports current actor" stance will mis-attribute every bead gascity creates. At minimum, warn when a different value is passed; ideally, actually honor it.
6. **Suppress the "creating issue without description" stderr warning** when `--json` is set. It's write to stderr so gascity's `extractJSON` tolerates it, but other JSON-parsing callers (beads-mcp, agent.py) do not.
7. **Emit `labels`, `description`, `metadata` fields** with the same omit-when-empty semantics Go uses (either rely on gascity's tolerant `extractJSON` and always emit, or add a compat flag that mimics `omitempty`).
8. **Add `needs` and `dependencies` rollups to the create response** so gascity's `toBead()` populates correctly. Currently gascity tolerates `nil` here so it's not fatal, but converging to Go's shape eliminates a class of surprises.

### Honor-no-metadata notes

Go's `--metadata '{"from":"agent-ralph"}'` shape encodes three distinct concerns that must be split into typed flags:

- **Provenance** (`metadata.from`): who/what created this bead. Replace with a typed `--from <actor|agent|job>` flag backed by a validated enum plus a free-text fallback for opaque identifiers. Owner doc: `primitives/metadata-remapping.md`.
- **Convergence state** (`metadata.convergence.gate_outcome`, `metadata.convergence.gate_id`, etc.): this is gate state, not metadata. Replace with a typed `bd gate set-outcome <id> --outcome=<pass|fail>` subcommand family rooted in `types/gate.md`.
- **Gascity idempotency key** (`metadata.gc.idempotency_key`): cross-system dedupe identifier. Replace with a typed `--idempotency-key <uuid>` flag on both `create` and `update`.

The final wire shape is commands like `bd create "Title" -t task --from agent-ralph --idempotency-key <uuid>` rather than `bd create "Title" -t task --metadata '{"from":"agent-ralph","gc.idempotency_key":"..."}'`. See `types/gate.md` and `primitives/metadata-remapping.md` (parallel agents) for the authoritative flag catalog. Do not resolve the mapping here.

---


