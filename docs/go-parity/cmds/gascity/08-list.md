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


