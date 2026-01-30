# Bot Comment Triage (from sweep 2026-02-06T08:20:09Z)

Source sweep:
- /Users/darin/src/personal/beads-rs/review-sweeps/bot-comment-sweep-20260206T082009Z.md

## Must Fix Now (Still Applies)

1. PR #28 — canonical SHA broadcast payload mismatch
- Bot: Codex (P1)
- Files: /Users/darin/src/personal/beads-rs/crates/beads-rs/src/daemon/core.rs:1574, /Users/darin/src/personal/beads-rs/crates/beads-rs/src/daemon/core.rs:1710
- Why: Canonical SHA is computed from canonical bytes but broadcast uses incoming bytes; can trigger hash mismatch on subscribers.

2. PR #29 — partial apply when dep-add cycle check fails
- Bot: Codex (P1)
- Files: /Users/darin/src/personal/beads-rs/crates/beads-core/src/apply.rs:71, /Users/darin/src/personal/beads-rs/crates/beads-core/src/apply.rs:298
- Why: apply_event mutates state op-by-op without rollback; later dep-add failure can leave partial state.

3. PR #35 — reader expected protocol version never updated post-handshake
- Bot: Codex (P1)
- Files: /Users/darin/src/personal/beads-rs/crates/beads-rs/src/daemon/repl/server.rs:540, /Users/darin/src/personal/beads-rs/crates/beads-rs/src/daemon/repl/server.rs:830
- Why: reader may reject negotiated higher-version envelopes after successful handshake.

4. PR #36 — strict redundant-field validation breaks legacy checkpoint reads
- Bot: Codex (P1)
- Files: /Users/darin/src/personal/beads-rs/crates/beads-rs/src/git/wire.rs:56, /Users/darin/src/personal/beads-rs/crates/beads-rs/src/git/wire.rs:265, /Users/darin/src/personal/beads-rs/crates/beads-rs/src/git/wire.rs:397
- Why: parser now requires redundant stamps that older v1 snapshots did not emit.

5. PR #36 — parent filter excludes children when parent is tombstoned
- Bot: Cursor BugBot (Medium)
- Files: /Users/darin/src/personal/beads-rs/crates/beads-core/src/state.rs:1321, /Users/darin/src/personal/beads-rs/crates/beads-rs/src/daemon/query_executor.rs:186
- Why: parent_edges_to early-returns empty if parent is tombstoned; query filter then drops all children.

6. PR #37 — snapshot import misses legacy note/label absorption ordering
- Bot: Codex (P2)
- Files: /Users/darin/src/personal/beads-rs/crates/beads-core/src/wire_bead.rs:763, /Users/darin/src/personal/beads-rs/crates/beads-core/src/wire_bead.rs:787, /Users/darin/src/personal/beads-rs/crates/beads-core/src/state.rs:1079
- Why: insert_live runs before note/label stores are loaded; absorb_legacy_lineage can miss migration.

## Needs Policy Decision

1. PR #38 — strict welcome_nonce echo for v1 compatibility
- Bot: Codex (P1)
- File: /Users/darin/src/personal/beads-rs/crates/beads-rs/src/daemon/repl/session.rs:865
- Why: strict mismatch reject may break legacy v1 peers; security strictness vs compatibility decision.

## No New Action (Recent Sweep)

- PRs #25, #26, #27, #30, #31, #32, #33, #34: only “no major issues” bot comments in the last 60 minutes.

