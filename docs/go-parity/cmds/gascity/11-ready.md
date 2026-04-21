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


