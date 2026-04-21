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


