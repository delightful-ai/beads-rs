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


