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


