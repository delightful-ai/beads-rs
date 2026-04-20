# Gascity Surface — Parity Backlog

**Pin:** Go `bd` v1.0.2 (`c446a2ef`) vs. beads-rs at current HEAD (`bd 0.2.0-alpha`).
**Scope:** the 14 commands gascity actually invokes from `internal/beads/bdstore.go`, `internal/beads/bdstore_graph_apply.go`, and `internal/doctor/checks_custom_types.go`, plus the JSON wire shape each side produces. Nothing else.

Live captures: all JSON output below was copy-pasted from fresh `/tmp/gap-go` and `/tmp/gap-rs` playgrounds running the pinned binaries on 2026-04-20. Do not rely on paraphrased shapes anywhere in this doc.

---

## 0. Global JSON envelope

| Axis | Go v1.0.2 | beads-rs | Divergence |
|------|-----------|----------|------------|
| Single-issue op (`create`, `update`, `close`, `delete`) | Bare JSON object with issue fields | `{"result": "issue", "data": {...}}` for `create`/`update`; `{"result":"closed","id":...,"receipt":{...}}` for `close`; `[{"status":"deleted","issue_id":...}]` for `delete` | Rust wraps, Go does not. Different result tags for different mutations. |
| Multi-issue read (`show`, `list`, `ready`, `dep list`) | Bare JSON array `[...]` | `{"result":"issues","data":[...]}` for `list`; `{"result":"issue","data":{...}}` for single-id `show`; `{"result":"ready","data":{"issues":[...], "blocked_count":N, "closed_count":N}}` for `ready` | Rust wraps in envelope and changes root-level shape (`ready` nests issues). |
| Field name for bead type | `"issue_type"` | `"type"` | Rename at JSON layer. |
| Timestamp encoding | RFC3339 string (`"2026-04-20T19:47:02Z"`) | `{"wall_ms":1776712314483,"counter":1}` | Structured HLC vs. human string. |
| Parent reference | `"parent"` (bare id) | Derived from `parent` dep edge; not exposed as top-level field on `show`/`list` | Rust omits the scalar field entirely. |
| Ephemeral metadata bag | `"metadata": {"k":"v"}` map | No `metadata` field in wire output (by design) | Rust refuses to emit a freeform map; typed fields only. |
| Dependency rollup on `show`/`list` | `"dependencies":[...]`, `"dependency_count":N`, `"dependent_count":N`, `"comment_count":N` | Not present; `note_count` only | Rollups missing. |

### Envelope decision pending

Two viable options for bringing Rust onto Go-compatible wire:

1. **Drop the envelope.** On commands gascity uses with `--json`, emit the bare object/array the Go binary emits. Pros: one-line fix in gascity, preserves drop-in compatibility for every other `bd`-aware consumer out there (beads-mcp, agent.py, etc.). Cons: loses `result` tag, which is currently used by Rust’s own CLI tests and the IPC rendering layer. We'd need a `--legacy-json` or a format-gate flag.
2. **Keep the envelope, make gascity unwrap.** Teach `BdStore` to look for `{"result": ..., "data": ...}` and peel it. Pros: lets Rust keep its typed result-tag discipline. Cons: every other external consumer also needs to peel, and the project's stated policy (in `AGENTS.md`) is "preserve information until a deliberate boundary" — unwrapping to a bare object is still losing information, we'd just be doing it inside gascity instead of inside bd.

Recommended: **add a `--compat-wire=go` (or `BD_COMPAT_WIRE=go`) format gate** at the CLI layer that produces Go-shaped output. The canonical wire stays enveloped; the shim turns on for gascity until gascity is rewritten to consume envelopes directly. This is the only path that honors both code bases' type-telling-truth discipline without forking one permanently.

### Status field mapping

| Status (go) | In `WorkflowStatus` (rust) | Gascity final map (`mapBdStatus`) |
|-------------|-----------------------------|------------------------------------|
| `open` | `Open` | `open` |
| `in_progress` | `InProgress` | `in_progress` |
| `blocked` | **absent** (Rust has no `Blocked` variant) | `open` (gascity fallback) |
| `review` | **absent** | `open` |
| `testing` | **absent** | `open` |
| `closed` | `Closed` | `closed` |

Rust's `WorkflowStatus` (crates/beads-core/src/wire_bead.rs:246-250) is deliberately 3-valued. Gascity already collapses Go's 6 values to 3, so the collapse is safe on the wire. **Required work:** document that Rust-produced statuses never include `blocked`/`review`/`testing`, and confirm gascity's switch-default path remains valid. No new Rust variants needed.

---

## 1. `bd init --server -p <prefix> --skip-hooks [--server-host H] [--server-port P]`

**Invocation** (`bdstore.go:116-129`):

```go
args := []string{"init", "--server", "-p", prefix, "--skip-hooks"}
// optionally: --server-host, --server-port
```

### Go v1.0.2 behavior

Non-interactive init succeeds; no JSON output (`--json` flag is global but `init` does not honor it).

```text
✓ bd initialized successfully!
  Backend: dolt
  Mode: embedded
  Database: gpi
  Issue prefix: gpi
```

### beads-rs behavior

`bd init --json` emits a minimal envelope:

```json
{ "result": "initialized" }
```

But requires a git remote to be configured (`ERROR error: no origin remote configured`) — gascity currently passes a fresh `s.dir` that has only `git init -q` done, so this blows up immediately.

### Flag delta

| Flag | Go v1.0.2 | Rust | Gap |
|------|-----------|------|-----|
| `--server` | yes (embedded dolt server off, use external) | no | Rust has no notion of a dolt SQL server — its backing store is the CRDT log plus git. Gascity must stop passing `--server`. |
| `-p/--prefix` | yes | no | Rust derives prefix from repo path / normalized remote URL; cannot override. |
| `--skip-hooks` | yes | no | Rust has no post-commit git hook to skip. |
| `--server-host` | yes | no | Same as `--server`: not applicable. |
| `--server-port` | yes | no | Same. |
| `--non-interactive` | yes | no (never interactive) | Harmless to ignore. |
| `--repo` | no | yes | Rust-specific. |
| `--namespace` | no | yes | Rust-specific. |

### JSON field delta

Not applicable: Go init doesn't emit JSON. Rust's `{"result":"initialized"}` is informational only.

### Required work in beads-rs

1. **Accept and ignore the dolt-server flags** when Rust is running in drop-in mode. `--server`, `--server-host=<h>`, `--server-port=<p>`, `--skip-hooks`: parse them, silently ignore. Alternative: hard-error with a clear message pointing gascity at the compat shim.
2. **Accept `-p <prefix>` as an alias for namespace/prefix override.** Even though Rust usually derives prefix from the repo URL, gascity passes `-p $namespace` and expects the resulting bead IDs to use that prefix. Either wire `-p` into the prefix derivation or hard-error with a documented migration note.
3. **Gate the "no origin remote configured" error.** Gascity inits in a directory with a clean `git init` and no remote; Rust rejects this. Two options: (a) infer a synthetic remote URL from the repo path when no remote is set (risky — collides with the beads-daemon keying), or (b) document that gascity must `git remote add origin ...` before calling `bd init`. Tracked separately in `primitives/ids-and-prefixes.md`.
4. **No envelope-unwrapping needed** since gascity discards init's stdout.

### Honor-no-metadata notes

None — init takes no metadata.

---

## 2. `bd config set <key> <value>` and `bd config get --json <key>`

**Invocation** (`bdstore.go:132-138`, `checks_custom_types.go:152-158`, `178-182`):

```go
// set
s.runner(s.dir, "bd", "config", "set", key, value)
// get (parsed as {"key":..., "value":...} with types.custom's value a comma-joined string)
exec.Command("bd", "config", "get", "--json", "types.custom")
```

### Go v1.0.2 behavior

```text
$ bd config set types.custom molecule,convoy
Set types.custom = molecule,convoy

$ bd config get --json types.custom
{
  "key": "types.custom",
  "value": "molecule,convoy"
}
```

If unset, `value` is an empty string (never the human sentinel `(not set)` — that's the point of `--json`).

### beads-rs behavior

```text
$ bd config --help
error: unrecognized subcommand 'config'
  tip: a similar subcommand exists: 'count'
```

Not implemented. Rust has **no** `config` subcommand; configuration is file-based (see `crates/beads-bootstrap/src/config.rs` for the schema, which is TOML at `~/.config/beads-rs/config.toml`).

### Flag delta

| Flag | Go v1.0.2 | Rust | Gap |
|------|-----------|------|-----|
| `config set` subcmd | yes | missing | Need full subcommand. |
| `config get --json <key>` subcmd | yes | missing | Need full subcommand. |

### JSON field delta

| Field | Go | Rust | Gap |
|-------|----|----|-----|
| `key` | string | — | add |
| `value` | string ("" when unset) | — | add |

### Required work in beads-rs

1. **Add a `bd config` subcommand family** covering at minimum `set <key> <value>`, `get [--json] <key>`, and `list [--json]`.
2. **Decide the storage layer.** Three options:
   - (a) Write into the bootstrap TOML (`~/.config/beads-rs/config.toml`). Clean; matches existing precedence rules in `beads-bootstrap`.
   - (b) Write into a new per-repo config file under `.beads/config.toml`. Matches Go's per-project semantics but requires a second config schema.
   - (c) Treat config keys as well-known events in the CRDT log. Correct for the project philosophy but over-engineered for gascity's current `types.custom`-only use.
   Recommended: (b) for gascity compatibility — `.beads/config.toml`, with a documented list of keys (`types.custom`, plus whatever else shows up).
3. **Implement `types.custom` specifically** as a list of validated custom type names. Parse the comma-joined input, store as a `Vec<ValidatedBeadTypeName>`, emit back as a single comma-joined string on `get --json` to match gascity's parser (`parseCustomTypesJSON` in `checks_custom_types.go`).
4. **Must honor the "empty value" contract.** When `types.custom` is unset, `{"key":"types.custom","value":""}` is what gascity expects. Do not emit `null` or a missing field.

### Honor-no-metadata notes

`config set`/`config get` are structured k/v, not metadata, and are acceptable. The gascity consumer in `checks_custom_types.go` only ever touches `types.custom`; we can implement a narrow, typed first version and grow the key surface as needed. A follow-up bead should enumerate the full Go key namespace (export.*, jira.*, linear.*, github.*, custom.*, status.*, doctor.suppress.*) and decide which land in Rust and which get declined.

---

## 3. `bd purge --json [--dry-run]`

**Invocation** (`bdstore.go:149-184`): runs from `filepath.Dir(beadsDir)` with `BEADS_DIR=<beadsDir>` env override.

### Go v1.0.2 behavior

```json
{
  "message": "No closed ephemeral beads to purge",
  "purged_count": 0
}
```

With actual purge work, `purged_count` becomes nonzero and more stats fields appear.

### beads-rs behavior

```text
$ bd purge --help
error: unrecognized subcommand 'purge'
```

Not implemented. Rust does not currently have an ephemeral/wisp subsystem at all — no TTL compaction, no `Ephemeral` flag on beads.

### Flag delta

| Flag | Go | Rust | Gap |
|------|----|------|-----|
| `--dry-run` | yes | missing | — |
| `--force` | yes (required for non-dry-run outside safe defaults) | missing | — |
| `--older-than <dur>` | yes | missing | — |
| `--pattern <glob>` | yes | missing | — |

### JSON field delta

| Field | Go | Rust | Gap |
|-------|----|----|-----|
| `purged_count` | int (nullable on gascity side via `*int`) | — | gascity parses `purged_count`; must be an integer. |
| `message` | string | — | Optional — gascity doesn't read it. |

### Required work in beads-rs

1. **Decide whether beads-rs has ephemeral beads at all.** Gascity uses purge to clean closed wisps/transient molecules. Rust currently has no ephemeral bit on the bead state, so "purge" has nothing to do. Either:
   - (a) Add an `ephemeral` flag to the core bead state, plumbed through apply/event/wire, and implement real purge semantics. Large piece of work.
   - (b) Ship a no-op `bd purge --json` that returns `{"purged_count": 0, "message": "not implemented"}`. Makes gascity's doctor path happy without doing anything.
   - (c) Decline purge in Rust and teach gascity to skip it on Rust backends.
2. **Honor the `BEADS_DIR` env override** if purge lands as a real implementation. Rust currently discovers repo from cwd or `--repo`; `BEADS_DIR` would need to be recognized as a compat env in bootstrap.
3. **JSON field shape must be `{"purged_count": <int>}`** at minimum; gascity's parser tolerates a nullable `*int` but will fail on a non-object or non-integer.

### Honor-no-metadata notes

Not applicable; purge operates on bead lifecycle state, not metadata.

---

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

## 5. `bd create --graph <file> --json`

**Invocation** (`bdstore_graph_apply.go:43`): writes a `GraphApplyPlan` JSON to a tempfile under `.gc/tmp/`, then `bd create --graph <path> --json`.

### Go v1.0.2 behavior

Input:

```json
{
  "commit_message": "test graph",
  "nodes": [
    {"key":"a","title":"Node A","type":"task"},
    {"key":"b","title":"Node B","type":"task"}
  ],
  "edges": [{"from_key":"b","to_key":"a","type":"blocks"}]
}
```

Output:

```json
{
  "ids": {
    "a": "gp-trm",
    "b": "gp-l7e"
  }
}
```

Gascity's `GraphApplyResult` (graph_apply.go:52-54) reads only the `ids` map. The plan's per-node fields (priority, description, assignee, labels, metadata, parent_key/parent_id, assign_after_create, from, metadata_refs) are authoritative.

### beads-rs behavior

```text
$ bd create --graph /tmp/graph.json --json
error: unexpected argument '--graph' found
```

Not implemented.

### Flag delta

| Flag | Go | Rust | Gap |
|------|----|------|-----|
| `--graph <file>` | yes | missing | Entire feature absent. |

### JSON field delta

**Plan input** (gascity → bd stdin via file):

| Node field | Required | Notes |
|------------|---------|-------|
| `key` | yes | Caller-stable symbolic id; returned in `ids` map. |
| `title` | yes | — |
| `type` | no (defaults to `task`) | Must accept custom types (`molecule`, `convoy`, etc.). |
| `priority` | no | int 0-4 |
| `description` | no | — |
| `assignee` | no | Gascity passes an agent id. |
| `assign_after_create` | no | If true, assign *after* the create event so hooks see unclaimed state first. Load-bearing for gascity's hook model. |
| `from` | no | Provenance — see metadata remapping notes. |
| `labels` | no | `[]string` |
| `metadata` | no | `map[string]string` — honor-no-metadata concern. |
| `metadata_refs` | no | `map[string]string` — references to other beads by key (e.g., `{"parent":"rootKey"}`). Honor-no-metadata concern. |
| `parent_key` | no | Reference another node in the same plan. |
| `parent_id` | no | Reference an existing bead by id. |

**Edge fields:**

| Edge field | Required | Notes |
|------------|---------|-------|
| `from_key`/`from_id` | one required | — |
| `to_key`/`to_id` | one required | — |
| `type` | no (defaults to `blocks`) | Must accept all 19 Go `DependencyType` variants; see `primitives/dep-kinds.md`. |
| `metadata` | no | Edge metadata (string, e.g., `waits_for.gate=all-children`). Honor-no-metadata concern. |

**Output:**

| Field | Type |
|-------|------|
| `ids` | `map[string]string` — symbolic key → concrete bead id |

### Required work in beads-rs

1. **Add `--graph <path>` to `bd create`.** Parses the plan JSON into a typed `GraphApplyPlan`, validates (at least one key, no duplicate keys, edge refs resolve), then applies the whole graph as one atomic transaction.
2. **Atomicity is the contract.** The whole point of `--graph` (vs. N sequential creates + M `dep add`s) is that partial failure leaves zero beads. Rust's CRDT log must either commit the plan as a single batched event list or roll back cleanly.
3. **Typed plan schema in `beads-api`** — define `GraphApplyPlan`/`GraphApplyNode`/`GraphApplyEdge` as Rust structs matching the JSON field names; version the schema.
4. **Symbolic key resolution** — while applying, maintain a `HashMap<PlanKey, BeadId>` so `parent_key`/`from_key`/`to_key` edges resolve to the IDs assigned during this apply.
5. **`assign_after_create`** — support the "assign after first event" semantic. Likely a second event in the same transaction.
6. **JSON output: bare `{"ids": {...}}` object.** No `{"result":"graph","data":...}` wrapper — gascity's parser expects the raw shape.

### Honor-no-metadata notes

The plan's per-node `metadata`/`metadata_refs` and edge `metadata` are direct consequences of the same design problem as command 4. Every use of `metadata` in gascity plan construction corresponds to something typed:

- Per-node `from` → `--from` typed flag (covered in command 4 remapping).
- Per-node `metadata.gc.*` → typed fields on the plan schema (e.g., `idempotency_key`, `convergence_key`).
- Per-node `metadata_refs` → first-class `refs` structure on the node, keyed by purpose (`{"parent_ref": "rootKey"}` becomes a typed field, not a map).
- Edge `metadata` (e.g., `waits-for` gate config) → typed `edge.gate` struct with `{"kind": "all-children" | "any-children", "spawner_id": "..."}`.

The Rust plan schema should be fully typed with no `HashMap<String, String>` escape hatch. See `primitives/metadata-remapping.md` for the flag→field catalog this crate must mirror.

---

## 6. `bd show --json <id>`

**Invocation** (`bdstore.go:452-468`): `bd show --json <id>`.

### Go v1.0.2 behavior

```json
[
  {
    "id": "gp-e4f",
    "title": "Hello world",
    "status": "open",
    "priority": 2,
    "issue_type": "task",
    "owner": "darinkishore@protonmail.com",
    "created_at": "2026-04-20T19:47:02Z",
    "created_by": "Darin Kishore",
    "updated_at": "2026-04-20T19:47:02Z"
  }
]
```

Note: Go's `show` returns a **JSON array** even for a single id. Gascity parses as `[]bdIssue` and takes index 0 (`bdstore.go:460-466`).

### beads-rs behavior

```json
{
  "result": "issue",
  "data": {
    "id": "gap-rs-8vy",
    ...
  }
}
```

Single-id `show` returns a wrapped object, not an array.

### Flag delta

| Flag | Go | Rust | Gap |
|------|----|------|-----|
| `<id>` positional | yes | yes | faithful |
| Multiple positionals | yes (returns all) | yes (multiple `[ID]...`) | faithful |
| `--refs` | yes | yes | faithful |
| `--children` | yes | yes (via `--tree` on list?) | verify |

### JSON field delta

Same issue_type/type, created_at shape, envelope issues as command 4.

### Required work in beads-rs

1. **Emit a bare array** `[{...}]` from `bd show --json <id>` in compat mode, matching Go's shape even for a single id.
2. Apply the same wire-compat fixes as command 4 (field rename, timestamp shape, envelope drop).
3. Preserve `owner`, `ref`, `needs`, `dependencies`, `metadata` fields where Go emits them. `owner` is specifically the email-like string gascity reads to populate `Bead.From` when `metadata.from` is absent.

### Honor-no-metadata notes

Gascity reads `metadata` on show for every bead. The typed replacements happen at write time (see commands 4 and 7); on read, the Rust binary must still emit a backward-compat `metadata` map of the typed fields gascity knows about, OR gascity must be updated to read the typed fields directly. Until `primitives/metadata-remapping.md` is settled, this is the single biggest blocker for gascity→Rust drop-in.

---

## 7. `bd update --json <id> [--title/--status/--type/--priority/--description/--parent/--assignee/--set-metadata*/--add-label*/--remove-label*]`

**Invocation** (`bdstore.go:471-522`): sequential arg construction.

### Go v1.0.2 behavior

```json
[
  {
    "id": "gp-trm",
    "title": "Node A",
    "status": "open",
    "priority": 1,
    "issue_type": "task",
    "created_at": "2026-04-20T19:48:48Z",
    "updated_at": "2026-04-20T19:48:56Z"
  }
]
```

Returns a **JSON array** even for a single id. Gascity discards the body (`_, err := s.runner(...)`), so wire shape on update is less critical than on create/show — but the success/failure signaling is.

### beads-rs behavior

```json
{
  "result": "issue",
  "data": {
    "id": "gap-rs-ekq",
    ...
  }
}
```

### Flag delta

| Flag | Go v1.0.2 | Rust | Gap |
|------|-----------|------|-----|
| `--title` | yes | yes | faithful |
| `--status` | yes | yes | faithful |
| `--type` | yes | yes | faithful |
| `--priority` | yes | yes | faithful |
| `--description` | yes | yes | faithful |
| `--parent` | yes (empty string removes) | yes + `--no-parent` | Rust splits into two flags; gascity never passes empty string so safe. |
| `--assignee` | yes | yes (compat limitations) | Same issue as command 4. |
| `--add-label` (repeatable) | yes | yes | faithful |
| `--remove-label` (repeatable) | yes | yes | faithful |
| `--set-metadata k=v` (repeatable) | yes | **missing** | Gascity's whole `SetMetadataBatch`/`SetMetadata` path depends on this. |
| `--unset-metadata k` (repeatable) | yes | missing | Gascity doesn't currently call unset, but likely will once metadata lifecycle grows. |
| `--metadata <json>` | yes | missing | Rarely used by gascity compared to `--set-metadata`. |
| Multiple positional ids (batch update) | yes (`[id...]`) | no (single `<ID>`) | Gascity does sequential updates currently (`SetMetadataBatch` iterates ids), so safe to leave as single-id for now. |
| `--append-notes` / `--notes` | yes | `--notes <N>` single | Gascity doesn't call notes on update, skip. |

### JSON field delta

Same as command 4 (envelope, field rename, timestamp shape).

### Required work in beads-rs

1. **Support multi-id update** (`bd update id1 id2 --status closed`) if gascity's future `CloseAll`-like batch paths land on `update`. Low priority — gascity's current `SetMetadataBatch` uses a single id.
2. **Wire compat fixes** as in command 4.
3. **Metadata flags: replace with typed flags per `primitives/metadata-remapping.md`.** Every `--set-metadata k=v` gascity emits maps to a typed field update; see honor-no-metadata.

### Honor-no-metadata notes

Gascity emits these `--set-metadata` keys today (based on the code paths in `cmd_convoy_dispatch.go`, `cmd_molecule.go`, `cmd_convergence/*.go`, `cmd_sling.go`):

- `from=<actor>` → `--from <actor>` typed flag.
- `gc.idempotency_key=<uuid>` → `--idempotency-key <uuid>` typed flag.
- `gc.convoy_id=<id>` → typed `--convoy <id>` reference; semantically a dep edge of kind `tracks`, not metadata (see `primitives/dep-kinds.md`).
- `convergence.gate_outcome=<pass|fail>` → **not metadata**. This is gate terminal state. Replace with a `bd gate resolve <gate-id> --outcome=pass|fail` subcommand family; see `types/gate.md`.
- `convergence.gate_id=<id>` → replaced by the subject of the gate subcommand — there's no separate metadata field because the gate's identity is its bead id.
- `patrol.muted_until=<ts>` → first-class `muted_until` field on the patrol bead type; see `types/molecule.md` / `primitives/metadata-remapping.md`.
- `mol.phase=proto|mol|wisp` → first-class `phase` enum on the molecule bead type; same doc.

Rust's update command should expose a typed flag per real field. The shape of those flags is locked in `primitives/metadata-remapping.md` and the per-type docs under `types/`. This file only enumerates the need, not the final naming.

---

## 8. `bd list --json [--label=/--assignee=/--status=/--type=/--all/--parent/--metadata-field k=v]* --include-infra --include-gates --limit N`

**Invocation** (`bdstore.go:628-678`): complex flag building with `--include-infra --include-gates` always present.

### Go v1.0.2 behavior

```json
[
  {
    "id": "gp-e4f",
    "title": "Hello world",
    "status": "open",
    "priority": 2,
    "issue_type": "task",
    "owner": "darinkishore@protonmail.com",
    "created_at": "2026-04-20T19:47:02Z",
    "created_by": "Darin Kishore",
    "updated_at": "2026-04-20T19:47:02Z",
    "dependency_count": 0,
    "dependent_count": 0,
    "comment_count": 0
  }
]
```

### beads-rs behavior

```json
{
  "result": "issues",
  "data": [
    {
      "id": "gap-rs-8vy",
      "type": "task",
      ...
      "note_count": 0
    }
  ]
}
```

### Flag delta

| Flag | Go v1.0.2 | Rust | Gap |
|------|-----------|------|-----|
| `--label=<l>` | yes | `-l/--label` (AND) + `--label-any` (OR) | faithful for AND, bonus capability |
| `--assignee=<a>` | yes | `-a/--assignee` | faithful |
| `--status=<s>` | yes | `-s/--status` | faithful |
| `--type=<t>` | yes | `-t/--type` | faithful syntactically; Rust rejects custom types (same block as command 4). |
| `--all` | yes (include closed) | yes | faithful |
| `--parent=<id>` | yes | yes | faithful |
| `--include-infra` | yes | **missing** | Rust has no concept of "infra" bead types to hide. |
| `--include-gates` | yes | **missing** | Same — no gate type in Rust yet. |
| `--include-templates` | yes | missing | Not used by gascity. |
| `--metadata-field k=v` | yes (repeatable) | **missing** | Gascity uses this heavily via `ListByMetadata`. |
| `--has-metadata-key k` | yes | missing | Not currently used by gascity. |
| `--limit N` | yes | `-n/--limit N` | faithful (Rust adds `-n` short) |

### JSON field delta

| Field | Go | Rust | Gap |
|-------|----|----|-----|
| envelope | bare array | `{"result":"issues","data":[...]}` | Wrapper. |
| `issue_type` vs `type` | `"issue_type"` | `"type"` | Rename. |
| `created_at`/`updated_at`/`closed_at` | RFC3339 | `{wall_ms, counter}` | Shape. |
| `dependency_count` | int | missing | gascity doesn't hard-require; tolerated absent. |
| `dependent_count` | int | missing | tolerated. |
| `comment_count` | int | `note_count` (different field name) | Rename. |
| `dependencies` (expanded on some views) | `[]Dep` | missing | tolerated. |
| `owner` | string | missing | See command 4. |
| `metadata` | map | missing | See honor-no-metadata below. |

### Required work in beads-rs

1. **Add `--include-infra` and `--include-gates` flags**, initially as accepted-and-ignored no-ops (Rust has no infra/gate types today). When molecule/gate types land (see `types/gate.md`, `types/molecule.md`), wire these to actual filters.
2. **Replace `--metadata-field k=v` with typed field filters.** Gascity's current callers are (auditable via grep of `ListByMetadata`): molecule phase, convoy id, convergence gate id, patrol state, session ownership. Every one of these maps to a typed filter (`--mol-phase proto|mol|wisp`, `--convoy <id>`, `--gate <id>`, etc.). See `primitives/metadata-remapping.md`.
3. **Emit `issue_type` in compat mode**, per command 4.
4. **Emit RFC3339 timestamps** in compat mode, per command 4.
5. **Drop envelope** in compat mode, per command 0.
6. **Emit rollup counts** (`dependency_count`, `dependent_count`, `comment_count`) to match Go's shape. Comment count is a genuine semantic gap (Rust tracks notes, not comments as a distinct concept); decide whether to alias `note_count → comment_count` in compat mode or to leave gascity with a null.
7. **Tolerant parse is the escape valve.** Gascity's `parseIssuesTolerant` (bdstore.go:313-333) skips entries that fail to parse. Any JSON shape change should aim for the happy path to decode cleanly while relying on tolerance only for edge cases.

### Honor-no-metadata notes

Same as command 7. Every call site gascity has for `bd list --metadata-field foo=bar` maps to a typed filter in the final design. See `primitives/metadata-remapping.md` for the catalog.

---

## 9. `bd close --json <id1> [<id2>...]` (batch)

**Invocation** (`bdstore.go:568-594`): batches all ids into one `bd close` call; falls back to per-id on failure.

### Go v1.0.2 behavior (batch of 2)

```json
[
  {
    "id": "gp-h9h",
    "status": "closed",
    "issue_type": "task",
    "closed_at": "2026-04-20T19:48:13Z",
    "close_reason": "Closed",
    ...
  }
]
```

### beads-rs behavior

```text
$ bd close id1 id2 --json
error: unexpected argument 'id2' found
```

Single-id only. The output envelope (for one id) is:

```json
{
  "result": "closed",
  "id": "gap-rs-ekq",
  "receipt": { ... (large durability proof tree) ... }
}
```

### Flag delta

| Flag | Go | Rust | Gap |
|------|----|------|-----|
| `[id...]` positional batch | yes | **no (single `<ID>`)** | Batch missing. |
| `--reason` | yes | yes | faithful |
| `--force` | yes | missing | Rust doesn't have "blocked by open issues" semantics yet (no DAG-cycle-aware close); check once gate/blocked semantics land. |
| `--suggest-next` | yes | missing | Not used by gascity. |
| `--continue` | yes | missing | Molecule-specific, not used by gascity's bead path. |

### JSON field delta

| Field | Go | Rust | Gap |
|-------|----|----|-----|
| envelope | array of closed issues | `{"result":"closed","id":...,"receipt":{...}}` | Fundamentally different shape. |
| `receipt.durability_proof.*` | — | deeply nested (durable_seq w/ byte arrays) | Rust-specific; bloats output considerably. |
| `close_reason` | string | part of wire_bead on a separate fetch | Shape mismatch. |

Gascity ignores the close body on success (`_, err := s.runner(...)`), so wire shape is less critical than batch-capability.

### Required work in beads-rs

1. **Support batch close** — accept `bd close id1 id2 id3 --json`. Gascity's single-call batch is a 4x round-trip optimization when closing many beads.
2. **Trim the `receipt` payload** or gate it behind `--verbose`. Gascity ignores the body but agents piping `bd close --json | jq ...` will get blindsided by the durability-proof bytes-array deluge.
3. **Return bare array** in compat mode, one object per closed issue, matching Go's shape (`[{ "id":..., "status":"closed", "close_reason":..., ...}]`).
4. **Honor Go's idempotency semantics.** Go errors with `invalid transition from closed to closed`. Gascity's fallback logic in `bdstore.go:600-613` re-reads the bead and treats "already closed" as success. Rust should either (a) match the error class string so gascity's fallback triggers, or (b) cleanly return success on already-closed in compat mode.

### Honor-no-metadata notes

Close is metadata-adjacent via gascity's `CloseAll(ids, metadata)` which pre-writes metadata on each id before closing (`bdstore.go:575-579`). That prewrite is via `SetMetadataBatch` → `bd update --set-metadata`. The close itself never touches metadata. Typed-flag replacements happen on command 7.

---

## 10. `bd delete --force --json <id>`

**Invocation** (`bdstore.go:616-625`): single-id delete.

### Go v1.0.2 behavior

```json
{
  "deleted": "gp-dpr",
  "dependencies_removed": 0,
  "references_updated": 0
}
```

### beads-rs behavior

```json
[
  {
    "status": "deleted",
    "issue_id": "gap-rs-bo3"
  }
]
```

### Flag delta

| Flag | Go | Rust | Gap |
|------|----|------|-----|
| `--force` | yes | missing (Rust does it always) | Rust currently doesn't require `--force`; accept and ignore. |
| Batch (`id1 id2 ...`) | yes | yes (`<IDS>...`) | Rust has batch, Go has batch. faithful |
| `--dry-run` | yes | missing | Not used by gascity. |
| `--from-file` | yes | missing | Not used by gascity. |

### JSON field delta

| Field | Go (single) | Rust (array, even for 1) | Gap |
|-------|-------------|--------------------------|-----|
| shape | bare object `{"deleted":..., ...}` | bare array `[{"status":"deleted","issue_id":...}]` | Different envelope. |
| `deleted` | bead id string | — | Rust uses `issue_id` inside an object with `status`. |
| `dependencies_removed` | int | — | Missing. |
| `references_updated` | int | — | Missing. |

Gascity discards the body, so the only fatal case is an actual error exit code.

### Required work in beads-rs

1. **Accept `--force`** as a no-op flag.
2. **Decide: single vs. batch output shape.** If Rust always returns an array (even for one id) that's fine semantically, but gascity's `Delete` passes one id and currently checks only the exit code. For strict compat, single-id delete should emit the bare `{"deleted": ..., "dependencies_removed": ..., "references_updated": ...}` object when exactly one id is given.
3. **Emit `dependencies_removed` and `references_updated` counts.** This is genuinely new information Rust's engine can produce (event log of the delete transaction), not compat padding.

### Honor-no-metadata notes

N/A — delete takes no metadata.

---

## 11. `bd ready --json --limit 0`

**Invocation** (`bdstore.go:735-749`): returns all ready beads, filters out `IsReadyExcludedType` on the gascity side.

### Go v1.0.2 behavior

```json
[
  {
    "id": "gp-h9h",
    "title": "Blocker",
    "status": "open",
    "priority": 2,
    "issue_type": "task",
    "owner": "darinkishore@protonmail.com",
    "created_at": "2026-04-20T19:47:45Z",
    "created_by": "Darin Kishore",
    "updated_at": "2026-04-20T19:47:45Z",
    "dependency_count": 0,
    "dependent_count": 1,
    "comment_count": 0
  }
]
```

### beads-rs behavior

```json
{
  "result": "ready",
  "data": {
    "issues": [],
    "blocked_count": 0,
    "closed_count": 0
  }
}
```

### Flag delta

| Flag | Go | Rust | Gap |
|------|----|------|-----|
| `--limit 0` = unlimited | yes | yes (`-n/--limit`) | faithful, verify 0-semantics |
| `--json` | yes | yes | faithful |
| `--exclude-type` | yes | missing | Gascity exclusion is client-side via `IsReadyExcludedType` — tolerated. |
| `--include-ephemeral` | yes | missing | Not used by gascity. |
| `--parent`, `--mol`, `--mol-type` | yes | missing | Not used by gascity's `Ready()`. |

### JSON field delta

| Field | Go | Rust | Gap |
|-------|----|----|-----|
| envelope | bare array | `{"result":"ready","data":{"issues":[...], "blocked_count":N, "closed_count":N}}` | Fundamentally different. Gascity does `parseIssuesTolerant(extractJSON(out))` which expects a JSON array; it will fail on Rust's current shape. |

This is the single command where Rust's envelope shape is not just wrapped — it adds nested summary counts that gascity cannot parse through `extractJSON`.

### Required work in beads-rs

1. **Produce bare-array output** `[{...},{...}]` in compat mode. The `blocked_count`/`closed_count` summary is genuinely useful; preserve it in the canonical (non-compat) wire but drop it for gascity.
2. **Verify `--limit 0` = unlimited** in Rust. If it's "zero results", gascity breaks immediately.
3. **Exclusion: server-side vs. client-side.** Gascity filters `IsReadyExcludedType` client-side today. When molecule/gate types land in Rust, implement `--exclude-type` to move this filtering server-side — but until then, server returning everything is correct.

### Honor-no-metadata notes

N/A — ready is a query, not a mutation.

---

## 12. `bd dep add <issueID> <dependsOnID> --type <depType>`

**Invocation** (`bdstore.go:753-765`): direction is "issueID depends on dependsOnID" (FROM → TO).

### Go v1.0.2 behavior

```json
{
  "depends_on_id": "gp-h9h",
  "issue_id": "gp-e4f",
  "status": "added",
  "type": "blocks"
}
```

### beads-rs behavior

```json
{
  "result": "dep_added",
  "from": "gap-rs-cpf",
  "to": "gap-rs-n7c",
  "receipt": { /* large durability proof */ }
}
```

### Flag delta

| Flag | Go | Rust | Gap |
|------|----|------|-----|
| `<issueID> <dependsOnID>` positional | yes (FROM, TO) | yes (FROM, TO; same direction) | faithful semantically |
| `--type <depType>` | yes | **Rust uses `--kind <depType>`** | **Flag rename: `--type` → `--kind`**. Gascity will fail. |
| `--blocked-by` / `--depends-on` | yes (alias) | missing | Not used by gascity. |

### JSON field delta

| Field | Go | Rust | Gap |
|-------|----|----|-----|
| envelope | bare object | `{"result":"dep_added","from":...,"to":...,"receipt":{...}}` | Wrapper + receipt pollution. |
| `issue_id` | string | `from` (renamed) | Rename. |
| `depends_on_id` | string | `to` (renamed) | Rename. |
| `status` | `"added"` | in `result` tag | Missing. |
| `type` | string (dep kind) | not on response; in request it's `--kind` | Missing from output. |

Gascity discards the body on success (`_, err := s.runner(...)`).

### Required work in beads-rs

1. **Add `--type <depType>` as an alias for `--kind <depType>`** on `bd dep add` and `bd dep rm`. Gascity invokes `--type`. Rust docs can keep `--kind` as the primary name but must accept `--type`.
2. **Emit the Go-shaped response** in compat mode: `{"issue_id":..., "depends_on_id":..., "status":"added", "type":"..."}`.
3. **Trim the receipt bytes-array** from non-verbose output.
4. **Accept the full Go DepKind catalog.** See `primitives/dep-kinds.md` — Rust's enum only covers 4 of Go's 19+ variants today.
5. **Idempotency on duplicate add.** Go's gascity-side `DepAdd` logic short-circuits on `parent-child` dupes (`bdstore.go:754-759`); that's a gascity concern. But `bd dep add gp-x gp-y --type blocks` run twice should not error.

### Honor-no-metadata notes

Dep edges carry optional metadata in Go (on `bd dep add` it's not exposed, but via `bd create --graph` edges can have `metadata` strings — e.g., `waits-for` gates). Replace edge-metadata with typed edge kinds and typed edge-local structs:

- `waits-for` edges with `WaitsForMeta` → a typed `bd dep add --kind waits-for --gate all-children|any-children --spawner <id>` flag surface. See `types/gate.md`.
- Other edge metadata uses are rare in gascity; audit per `primitives/metadata-remapping.md`.

---

## 13. `bd dep remove <issueID> <dependsOnID>`

**Invocation** (`bdstore.go:768-774`).

### Go v1.0.2 behavior

Succeeds silently; gascity discards body.

### beads-rs behavior

Subcommand is named `bd dep rm`, not `bd dep remove`.

```text
$ bd dep remove id1 id2
error: unrecognized subcommand 'remove'
```

### Flag delta

| Flag | Go | Rust | Gap |
|------|----|------|-----|
| subcommand name | `remove` | `rm` | **Rename: need `remove` alias.** |
| `<issueID> <dependsOnID>` positional | yes | yes | faithful |
| `--type <depType>` (to distinguish kinds) | yes | `--kind` | Same issue as command 12. Gascity doesn't pass `--type` on remove, so secondary. |

### Required work in beads-rs

1. **Add `remove` as a subcommand alias for `rm`.** Single-line clap change.
2. **Accept `--type` as alias for `--kind`** (same as command 12).
3. **Bare-object success envelope** `{"issue_id":..., "depends_on_id":..., "status":"removed"}` in compat mode, matching Go.

### Honor-no-metadata notes

N/A — remove takes no metadata.

---

## 14. `bd dep list <id(s)> [--json] [--direction=up]`

**Invocation** (`bdstore.go:784-821` single, `bdstore.go:825-865` batch): positional ids, optional direction, JSON always.

### Go v1.0.2 behavior

**Single id, `--direction=down` (default), `--json`:**

```json
[
  {
    "id": "gp-h9h",
    "title": "Blocker",
    "status": "open",
    "priority": 2,
    "issue_type": "task",
    "owner": "darinkishore@protonmail.com",
    "created_at": "2026-04-20T19:47:45Z",
    "created_by": "Darin Kishore",
    "updated_at": "2026-04-20T19:47:45Z",
    "dependency_type": "blocks"
  }
]
```

Each row is a full `bdIssue` object with an extra top-level `dependency_type` field. Gascity parses as `bdDepIssue` (bdstore.go:778-781) and rebuilds `Dep{IssueID, DependsOnID, Type}` using direction semantics.

**Single id, `--direction=up`, `--json`:** same shape, rows represent issues that point AT the query id.

**Batch (multiple ids):**

```json
[
  {
    "issue_id": "gp-e4f",
    "depends_on_id": "gp-h9h",
    "type": "blocks",
    "created_at": "2026-04-20T12:47:52Z",
    "created_by": "Darin Kishore",
    "metadata": "{}"
  }
]
```

**Different shape!** Raw dependency records instead of bdIssues. Gascity's `DepListBatch` (bdstore.go:825-865) hard-codes this and parses with a dedicated struct.

### beads-rs behavior

```text
$ bd dep list <id> --json
error: unrecognized subcommand 'list'
```

No `dep list` subcommand at all. Rust has `bd dep tree <id> --json` which emits a completely different shape:

```json
{
  "result": "dep_tree",
  "data": {
    "root": "gap-rs-cpf",
    "edges": [{"from": "gap-rs-cpf", "to": "gap-rs-n7c", "kind": "blocks"}]
  }
}
```

### Flag delta

| Flag | Go | Rust | Gap |
|------|----|------|-----|
| `dep list` subcommand | yes | **missing** | Biggest deficit after `config`/`purge`. |
| `<id>` positional | yes | N/A | — |
| Multiple positional ids (batch) | yes | N/A | — |
| `--direction=up\|down` | yes | N/A | — |
| `--type <t>` (filter by kind) | yes | N/A | — |
| `--json` | yes | N/A | — |

### JSON field delta (single-id form)

| Field | Go | Rust | Gap |
|-------|----|----|-----|
| envelope | bare array | — | Needs `[...]`. |
| row shape | bdIssue + `dependency_type` | — | — |
| `dependency_type` | string (dep kind) | — | Load-bearing key. |
| all bdIssue fields | yes | — | Same wire-compat concerns as command 8. |

### JSON field delta (batch form)

| Field | Go | Rust | Gap |
|-------|----|----|-----|
| envelope | bare array | — | Needs `[...]`. |
| `issue_id` | string | — | — |
| `depends_on_id` | string | — | — |
| `type` | string (dep kind) | — | — |
| `created_at` | RFC3339 | — | — |
| `created_by` | string | — | — |
| `metadata` | string (JSON-encoded map, e.g., `"{}"`) | — | Honor-no-metadata concern — see below. |

### Required work in beads-rs

1. **Add `bd dep list` subcommand.** Mandatory for gascity. Spec:
   - Accepts one or more positional ids.
   - `--direction down` (default) or `--direction up`.
   - `--type <kind>` filter.
   - `--json` flag.
2. **Dual output shape is required.**
   - 1 id → array of bdIssue-shaped records with `dependency_type` tagged on each row.
   - 2+ ids → array of raw `{issue_id, depends_on_id, type, created_at, created_by, metadata}` records.
   This is genuinely the Go contract, not an accident; gascity's single/batch parsers depend on it.
3. **Emit `metadata` as a JSON-encoded string** (not a nested object) on batch output — this is Go's actual shape (`"metadata": "{}"`). Or — better — replace with typed per-edge fields and provide a compat path.
4. **Every dep kind must round-trip.** Gascity persists 19+ kinds (see `primitives/dep-kinds.md`); dep list must surface all of them correctly.

### Honor-no-metadata notes

Dep list's per-edge `metadata` field is a stringified JSON blob today. It carries gate configuration (for `waits-for` edges) and nothing else we've seen. Typed replacement: emit a typed `gate: {kind: "all-children", spawner_id: "..."}` object on edges where it's meaningful, and omit entirely elsewhere. See `types/gate.md` and `primitives/metadata-remapping.md`.

---

## Summary

**Count (of 14 commands):**

- **0 faithful** — no command is fully compatible today. Field renames, envelope wrappers, or timestamp shapes block every single one.
- **6 require wire-compat only** — `show`, `list`, `ready`, `dep list` (once subcommand added), `close` (once batch added), `dep add` (once `--type` alias added). These need the compat shim listed above (bare envelope, `issue_type` field, RFC3339 timestamps, etc.), plus small flag renames/aliases.
- **4 require wire-compat plus a missing flag or semantic** — `create` needs custom-type acceptance + `--metadata` or a typed replacement, `update` needs `--set-metadata` or typed replacement, `dep remove` needs an alias for `rm`, `delete` needs `--force` no-op.
- **4 commands/subcommands are entirely absent** — `config set`, `config get`, `purge`, `create --graph`.

**Critical blockers (in implementation order):**

1. **`bd config set` / `bd config get --json types.custom`** — the doctor check runs on every `gc doctor` invocation. Without it, gascity cannot even declare Rust-bd as "usable." (Command 2.)
2. **Custom bead types.** Without `types.custom`, `bd create -t molecule` / `-t convoy` / `-t gate` etc. fail immediately. This is a core-crate change (close enum `BeadType` to validated string). (Command 4 + `primitives/custom-types.md`.)
3. **Wire-compat mode** covering the envelope drop, `issue_type` rename, RFC3339 timestamps, and `dependency_type` on dep-list rows. Ships a flag (`--compat-wire=go`) or env var gate. (Commands 4, 6, 7, 8, 11, 14.)
4. **`bd dep list`** — subcommand is missing entirely. (Command 14.)
5. **`bd dep add --type` alias** and `bd dep remove` as alias for `rm`. (Commands 12, 13.)
6. **`bd create --graph <file>`** — without graph-apply, gascity cannot instantiate molecules atomically. (Command 5.)
7. **`bd close id1 id2 ...` batch** and `bd ready` bare-array output. (Commands 9, 11.)
8. **Metadata flag replacements** — the single largest semantic design pass. See `primitives/metadata-remapping.md` for the full flag catalog. Affects commands 4, 5, 7, 8, 14.
9. **`bd purge`** — either implement, stub as no-op, or explicitly decline. (Command 3.)
10. **`bd init` flag acceptance** — ignore `--server`, `--skip-hooks`, `--server-host`, `--server-port`, accept `-p <prefix>`, and fix the "no origin remote" fatal. (Command 1.)

**Non-blockers (important but not gating drop-in use):**

- Durability-proof bloat in mutation responses (trim or gate).
- Assignee compat mode actually honoring the passed value.
- `--include-infra` / `--include-gates` / `--include-templates` on list (accept-and-ignore until types exist).
- `dependencies_removed` / `references_updated` fields on delete.
