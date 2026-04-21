## 0. Global JSON envelope

| Axis | Go v1.0.2 | beads-rs | Divergence |
|------|-----------|----------|------------|
| Single-issue op (`create`, `update`, `close`, `delete`) | Bare JSON object with issue fields | `{"result": "issue", "data": {...}}` for `create`/`update`; `{"result":"closed","id":...,"receipt":{...}}` for `close`; `[{"status":"deleted","issue_id":...}]` for `delete` | Rust wraps, Go does not. Different result tags for different mutations. |
| Multi-issue read (`show`, `list`, `ready`, `dep list`) | Bare JSON array `[...]` | `{"result":"issues","data":[...]}` for `list`; `{"result":"issue","data":{...}}` for single-id `show`; `{"result":"ready","data":{"issues":[...], "blocked_count":N, "closed_count":N}}` for `ready` | Rust wraps in envelope and changes root-level shape (`ready` nests issues). |
| Field name for bead type | `"issue_type"` | `"type"` | Rename at JSON layer. |
| Timestamp encoding | RFC3339 string (`"2026-04-20T19:47:02Z"`) | `{"wall_ms":1776712314483,"counter":1}` | Structured HLC vs. human string. |
| Parent reference | `"parent"` (bare id) | Derived from `parent` dep edge; not exposed as top-level field on `show`/`list` | Rust omits the scalar field entirely. |
| Ephemeral metadata bag | `"metadata": {"k":"v"}` map | No `metadata` field in wire output (by design) | Rust refuses to emit a freeform map; typed fields only. |
| Dependency rollup on `show`/`list` | `"dependencies":[...]`, `"dependency_count":N`, `"dependent_count":N`, `"comment_count":N` | Not present; `note_count` only | Rollups missing. |

### Envelope decision pending

Two viable options for bringing Rust onto Go-compatible wire:

1. **Drop the envelope.** On commands gascity uses with `--json`, emit the bare object/array the Go binary emits. Pros: one-line fix in gascity, preserves drop-in compatibility for every other `bd`-aware consumer out there (beads-mcp, agent.py, etc.). Cons: loses `result` tag, which is currently used by Rust’s own CLI tests and the IPC rendering layer. We'd need a `--legacy-json` or a format-gate flag.
2. **Keep the envelope, make gascity unwrap.** Teach `BdStore` to look for `{"result": ..., "data": ...}` and peel it. Pros: lets Rust keep its typed result-tag discipline. Cons: every other external consumer also needs to peel, and the project's stated policy (in `AGENTS.md`) is "preserve information until a deliberate boundary" — unwrapping to a bare object is still losing information, we'd just be doing it inside gascity instead of inside bd.

Recommended: **add a `--compat-wire=go` (or `BD_COMPAT_WIRE=go`) format gate** at the CLI layer that produces Go-shaped output. The canonical wire stays enveloped; the shim turns on for gascity until gascity is rewritten to consume envelopes directly. This is the only path that honors both code bases' type-telling-truth discipline without forking one permanently.

### Status field mapping

| Status (go) | In `WorkflowStatus` (rust) | Gascity final map (`mapBdStatus`) |
|-------------|-----------------------------|------------------------------------|
| `open` | `Open` | `open` |
| `in_progress` | `InProgress` | `in_progress` |
| `blocked` | **absent** (Rust has no `Blocked` variant) | `open` (gascity fallback) |
| `review` | **absent** | `open` |
| `testing` | **absent** | `open` |
| `closed` | `Closed` | `closed` |

Rust's `WorkflowStatus` (crates/beads-core/src/wire_bead.rs:246-250) is deliberately 3-valued. Gascity already collapses Go's 6 values to 3, so the collapse is safe on the wire. **Required work:** document that Rust-produced statuses never include `blocked`/`review`/`testing`, and confirm gascity's switch-default path remains valid. No new Rust variants needed.

---


