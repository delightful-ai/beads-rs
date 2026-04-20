# bd create

**Go source:** `cmd/bd/create.go` + `create_embedded_test.go`, `create_graph.go`, `create_embedded_graph_test.go`
**Pin:** v1.0.2 (c446a2ef)
**Parity status:** `simplified` — beads-rs `bd create` exists but covers a narrow subset of Go's flag surface.
**Rust source:** `crates/beads-cli/src/commands/create.rs` (partial)

## Purpose

`bd create` is the primary write path for new issues. It covers three distinct modes:

1. **Single-issue creation** — title + optional flags → one new issue.
2. **Batch from markdown (`--file`)** — parse an issues-shaped markdown document into multiple issues.
3. **Graph from JSON plan (`--graph`)** — parse a dependency-graph JSON file into a connected set of issues with edges. Replaced the standalone `bd graph-apply` command in v0.63.3.

It also supports preview (`--dry-run`), script-friendly output (`--silent`, `--json`), validation (`--validate`), and extension points for new issue types (event payload flags, molecule/wisp types, waits-for gates).

## Invocation

```
bd create [title] [flags]
```

Aliases: `create`, `new`.

## Arguments

| Name | Type | Required | Semantics |
|------|------|----------|-----------|
| `title` | positional string | required unless `--title`, `--file`, or `--graph` is used | Issue title. If omitted, one of `--title`, `--file`, or `--graph` must be present. |

## Flags

Captured from `/tmp/go-bd-playground/bd-go create --help` against v1.0.2.

### Field flags

| Flag | Type | Default | Semantics |
|------|------|---------|-----------|
| `--title` | string | — | "Issue title (alternative to positional argument)" |
| `-d, --description` | string | — | "Issue description" |
| `--body-file` | string | — | "Read description from file (use - for stdin)" |
| `--stdin` | bool | false | "Read description from stdin (alias for --body-file -)" |
| `--design` | string | — | "Design notes" |
| `--design-file` | string | — | "Read design from file (use - for stdin)" |
| `--acceptance` | string | — | "Acceptance criteria" |
| `--notes` | string | — | "Additional notes" |
| `--append-notes` | string | — | "Append to existing notes (with newline separator)" |
| `--context` | string | — | "Additional context for the issue" |
| `-t, --type` | string | `"task"` | "Issue type (bug\|feature\|task\|epic\|chore\|decision); custom types require types.custom config; aliases: enhancement/feat→feature, dec/adr→decision" |
| `-p, --priority` | string | `"2"` | "Priority (0-4 or P0-P4, 0=highest)" |
| `-a, --assignee` | string | — | "Assignee" |
| `-l, --labels` | strings | — | "Labels (comma-separated)" |
| `-e, --estimate` | int | — | "Time estimate in minutes (e.g., 60 for 1 hour)" |
| `--external-ref` | string | — | "External reference (e.g., 'gh-9', 'jira-ABC')" |
| `--spec-id` | string | — | "Link to specification document" |
| `--skills` | string | — | "Required skills for this issue" |
| `--id` | string | — | "Explicit issue ID (e.g., 'bd-42' for partitioning)" |
| `--metadata` | string | — | "Set custom metadata (JSON string or @file.json to read from file)" |

### Structural flags

| Flag | Type | Default | Semantics |
|------|------|---------|-----------|
| `--parent` | string | — | "Parent issue ID for hierarchical child (e.g., 'bd-a3f8e9')" |
| `--no-inherit-labels` | bool | false | "Don't inherit labels from parent issue" |
| `--deps` | strings | — | "Dependencies in format 'type:id' or 'id' (e.g., 'discovered-from:bd-20,blocks:bd-15' or 'bd-20')" |
| `--waits-for` | string | — | "Spawner issue ID to wait for (creates waits-for dependency for fanout gate)" |
| `--waits-for-gate` | string | `"all-children"` | "Gate type: all-children (wait for all) or any-children (wait for first)" |
| `--defer` | string | — | "Defer until date (issue hidden from bd ready until then). Same formats as --due" |
| `--due` | string | — | "Due date/time. Formats: +6h, +1d, +2w, tomorrow, next monday, 2025-01-15" |

### Lifecycle flags

| Flag | Type | Default | Semantics |
|------|------|---------|-----------|
| `--ephemeral` | bool | false | "Create as ephemeral (short-lived, subject to TTL compaction)" |
| `--wisp-type` | string | — | "Wisp type for TTL-based compaction: heartbeat, ping, patrol, gc_report, recovery, error, escalation" |
| `--mol-type` | string | — | "Molecule type: swarm (multi-agent), patrol (recurring ops), work (default)" |
| `--no-history` | bool | false | "Skip Dolt commit history without making GC-eligible (for permanent agent beads)" |

### Event-type flags (require `--type=event`)

| Flag | Type | Default | Semantics |
|------|------|---------|-----------|
| `--event-actor` | string | — | "Entity URI who caused this event" |
| `--event-category` | string | — | "Event category (e.g., patrol.muted, agent.started)" |
| `--event-payload` | string | — | "Event-specific JSON data" |
| `--event-target` | string | — | "Entity URI or bead ID affected" |

### Batch/routing flags

| Flag | Type | Default | Semantics |
|------|------|---------|-----------|
| `-f, --file` | string | — | "Create multiple issues from markdown file" |
| `--graph` | string | — | "Create a graph of issues with dependencies from JSON plan file" |
| `--repo` | string | — | "Target repository for issue (overrides auto-routing)" |

### Control flags

| Flag | Type | Default | Semantics |
|------|------|---------|-----------|
| `--dry-run` | bool | false | "Preview what would be created without actually creating" |
| `--validate` | bool | false | "Validate description contains required sections for issue type" |
| `--force` | bool | false | "Force creation even if prefix doesn't match database prefix" |
| `--silent` | bool | false | "Output only the issue ID (for scripting)" |

## Output (live capture)

### Default single-issue create

```bash
$ cd /tmp/go-bd-play && /tmp/go-bd-playground/bd-go create \
    --title "seed issue for json capture" \
    --description "used to validate show output shape" \
    --priority 2 \
    --json
```

```json
{
  "id": "go-bd-play-9m1",
  "title": "seed issue for json capture",
  "description": "used to validate show output shape",
  "status": "open",
  "priority": 2,
  "issue_type": "task",
  "owner": "darinkishore@protonmail.com",
  "created_at": "2026-04-20T18:48:58.081447Z",
  "created_by": "Darin Kishore",
  "updated_at": "2026-04-20T18:48:58.081447Z"
}
```

`bd create --json` returns a **bare object**, not an array (contrast with `bd show --json` which always wraps in `[]`).

### Create with metadata (JSON payload)

```bash
$ /tmp/go-bd-playground/bd-go create \
    --title "mdtest" \
    --metadata '{"k":"v","nested":{"a":1}}' \
    --json
```

```json
{
  "id": "go-bd-play-9rb",
  "title": "mdtest",
  "status": "open",
  "priority": 2,
  "issue_type": "task",
  "owner": "darinkishore@protonmail.com",
  "created_at": "2026-04-20T18:53:58.xxxxxxZ",
  "created_by": "Darin Kishore",
  "updated_at": "2026-04-20T18:53:58.xxxxxxZ",
  "metadata": {
    "k": "v",
    "nested": {
      "a": 1
    }
  }
}
```

`metadata` is **arbitrary JSON**, not a `map[string]string`. Nested objects and arrays round-trip. It surfaces in `bd show --long` too.

### Create with parent (hierarchical ID)

```bash
$ /tmp/go-bd-playground/bd-go create \
    --title "parent demo" \
    --parent go-bd-play-8mz \
    --labels "inherit-test" \
    --json
```

```json
{
  "id": "go-bd-play-8mz.1",
  "title": "parent demo",
  ...
}
```

Parent-ID is encoded into the **child's ID** via dotted suffix (`8mz` → `8mz.1`, `8mz.2`, etc.). The parent-child edge is implicit in the ID structure; no dedicated `parent_id` field in the default JSON.

### Create with typed dependencies

```bash
$ /tmp/go-bd-playground/bd-go create \
    --title "dep demo" \
    --deps "blocks:go-bd-play-9m1,discovered-from:go-bd-play-8mz" \
    --json
```

The create output does NOT include dependencies. To see the resulting edges:

```bash
$ /tmp/go-bd-playground/bd-go show go-bd-play-0rx --long --json
```

```json
[
  {
    "id": "go-bd-play-0rx",
    "title": "dep demo",
    ...
    "dependencies": [
      {
        "id": "go-bd-play-8mz",
        ...
        "dependency_type": "discovered-from"
      }
    ],
    "dependents": [
      {
        "id": "go-bd-play-9m1",
        ...
        "dependency_type": "blocks"
      }
    ]
  }
]
```

**Critical shape detail:** Go's `dependencies[]` / `dependents[]` contain **fully-hydrated issue objects** with a `dependency_type` field grafted on, not bare `{id, type}` records.

### Silent mode (script-friendly)

```bash
$ /tmp/go-bd-playground/bd-go create --title "silent test" --silent
```

```
go-bd-play-0d1
```

One line, one ID, no trailing newline. No JSON. Intended for `X=$(bd create --title ... --silent)` patterns.

### Dry-run preview

```bash
$ /tmp/go-bd-playground/bd-go create --title "dry run" --dry-run --json
```

```json
{
  "id": "",
  "title": "dry run",
  "status": "open",
  "priority": 2,
  "issue_type": "task",
  "owner": "darinkishore@protonmail.com",
  "created_at": "0001-01-01T00:00:00Z",
  "created_by": "Darin Kishore",
  "updated_at": "0001-01-01T00:00:00Z"
}
```

Dry-run returns a **shape-correct envelope** with empty ID and zero-valued timestamps. Callers who only want to confirm validation should pipe through `jq -e` or check exit code rather than parsing the zero-value placeholders.

### Error case: invalid type

```bash
$ /tmp/go-bd-playground/bd-go create --title "bad type" --type=nonsense --json
```

```json
{
  "error": "validation failed for issue : invalid issue type: nonsense"
}
```

Exit code `1`. Error envelope is a flat `{"error": "..."}` object. The validation path fires BEFORE any Dolt write, so the database is untouched on error.

## Data model impact

- **Reads:** parent issue (when `--parent` or label inheritance applies), referenced dependency targets (for existence check).
- **Writes:**
  - `issues` row: all field flags map to columns directly.
  - `labels` rows: one per label (inheriting from parent unless `--no-inherit-labels`).
  - `dependencies` rows: one per `--deps` entry + the `--waits-for` edge if present.
  - `metadata` column (JSON blob): stored verbatim if `--metadata` given.
  - `started_at` is NOT written here; it's populated on the first `in_progress` transition via `bd update`.
- **ID assignment:**
  - If `--id=<explicit>` given, that ID is used (after prefix validation — use `--force` to override).
  - If `--parent` given, ID is parent's ID + `.N` where N is the next child index.
  - Otherwise, new hash-style ID (`<prefix>-<hash4>`).
- **Invariants enforced:**
  - Type must be a valid built-in (`bug|feature|task|epic|chore|decision`) or configured custom type (`types.custom`). v1.0.0+ additionally accepts `spike`, `story`, `milestone`, plus all v1.0.0 type expansions (`message`, `event`, `slot`, `convoy`, `gate`, etc.) when enabled.
  - Priority must be 0–4 or P0–P4.
  - Title is required (via positional or `--title`).
  - Event fields (`--event-*`) require `--type=event`.
  - `--waits-for` requires a valid spawner ID.
- **Hooks fired:** `on_create` (v1.0.0+, fires in the storage layer via `HookFiringStore` decorator).

## Error cases

| Condition | Output | Exit |
|-----------|--------|------|
| Invalid `--type` | `{"error": "validation failed for issue : invalid issue type: <value>"}` | 1 |
| Missing title | Usage error to stderr | 2 |
| `--parent <id>` where id doesn't exist | Error referring to unknown parent | 1 |
| `--deps type:id` where id doesn't exist | Error referring to unknown dep target | 1 |
| `--id <explicit>` with wrong prefix without `--force` | Prefix mismatch error | 1 |
| `--metadata @file.json` where file doesn't exist | File read error | 1 |
| `--file <md>` parse error | Per-issue error envelope in the batch output | 1 if any fail |
| `--graph <json>` parse error | JSON parse error | 1 |
| Readonly or sandbox mode with write attempted | Mode-violation error | 1 |

## Rust parity notes

### Current divergence (v0.2.0-alpha)

beads-rs's `bd create` covers the basic field set (title, description, type, priority, labels, assignee) but is missing most of Go's structural and lifecycle flags. Major gaps:

| Capability | Go v1.0.2 | beads-rs | Parity gap |
|------------|-----------|----------|------------|
| `--metadata` | first-class, arbitrary JSON | not implemented | Deferred until Slots primitive lands. |
| `--parent` + hierarchical IDs | dotted suffix convention | not implemented | beads-rs uses flat IDs + explicit `parent-child` dep edges. Deliberate divergence. |
| `--deps type:id` | typed edges on create | not implemented | Must currently two-step: `bd create` then `bd dep add`. |
| `--file <md>` batch | bulk create from markdown | not implemented | |
| `--graph <json>` | bulk create from plan | not implemented | |
| `--ephemeral` / `--wisp-type` | wisp lifecycle | not implemented | Deferred until wisp primitive lands. |
| `--mol-type` | molecule typing | not implemented | Deferred until molecule primitive lands. |
| `--event-*` | event-type fields | not implemented | Deferred until event type lands. |
| `--waits-for` | fanout gate dep | not implemented | Deferred until gate primitive lands. |
| `--dry-run` | preview envelope | not implemented | |
| `--validate` | section-required validation | not implemented | |
| `--silent` | ID-only output | not implemented | |
| `--defer` / `--due` | scheduled visibility | not implemented | |

### Forcing functions

- **Metadata needs a home.** The "Slots as opaque consumer scratch" primitive (see `primitives/slots.md`, to be written) is the canonical answer. Until it lands, `--metadata` has no target shape in beads-rs.
- **Hierarchical IDs vs flat + dep edges.** Go overloads the ID string with parentage (`bd-8mz.1`). beads-rs uses flat hash IDs plus explicit `parent-child` dependency edges, matching the CRDT substrate better. A flag like `--parent` becomes sugar for "create + dep-add parent-child".
- **Dotted IDs on `children` lookup.** Go's children query can scan by ID prefix (`bd-8mz.*`); beads-rs must go through the dep graph. Equivalent, not identical.
- **Dry-run envelope shape.** The zero-valued timestamp placeholder is arguably a Go design wart (`0001-01-01T00:00:00Z` is the nil `time.Time`). beads-rs should emit an envelope WITHOUT those fields rather than reproducing the zero-value quirk.

### Parity path

1. **Slots primitive + `--metadata` support** — critical for gascity integration.
2. **`--parent` as syntactic sugar** — translate to flat ID creation + `parent-child` dep edge. Preserves Go's CLI ergonomics without adopting Go's ID overloading.
3. **`--deps type:id` multi-value parsing** — translate to one dep-add call per entry in the same transaction.
4. **Lifecycle flags as the corresponding primitives land** — `--ephemeral`, `--wisp-type`, `--mol-type`, `--event-*`, `--waits-for`.
5. **Dry-run as the CRDT-stamp-ID-preview envelope** — beads-rs can show the computed ID and planned `WriteStamp` without persisting. Don't reproduce Go's zero-timestamp placeholder.
6. **Batch (`--file`, `--graph`) last** — these are convenience, not structural.

### What matches today

- Positional title → new issue works.
- `--type`, `--priority`, `--labels`, `--description` honored.
- `--json` produces a bare object (not array).
- Default values for `--type` (`task`) and `--priority` (`P2`) align.
- Validation error path returns non-zero exit.
