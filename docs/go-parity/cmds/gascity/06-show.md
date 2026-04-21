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


