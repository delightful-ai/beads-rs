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


