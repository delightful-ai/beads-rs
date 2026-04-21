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


