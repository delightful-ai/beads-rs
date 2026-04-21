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


