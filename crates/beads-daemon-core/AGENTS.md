## Boundary
`src/lib.rs` re-exports only `durability`, `repl`, `wal`, and `beads_core`. This crate owns typed daemon-adjacent primitives; it does not own daemon process orchestration or git/checkpoint behavior.

## Route Work
- Put protocol/schema changes in `src/repl/proto.rs`. The runtime `crates/beads-daemon/src/runtime/repl/*.rs` files consume those wire types; they are not the source of truth.
- Put shared replication invariants in `src/repl/contiguous_batch.rs`, `src/repl/gap_buffer.rs`, and `src/repl/peer_acks.rs`.
- Put backend-neutral WAL laws in `src/wal/contract.rs`, `src/wal/replay.rs`, and `src/wal/seams.rs`.
- Put offline verification/repair result shapes in `src/wal/fsck.rs`. Keep typed repairs such as `FsckRepair::PrefixSalvageTruncate` and `FsckRepair::QuarantineNoValidPrefix` explicit.

## Keep Out
- Do not pull daemon runtime/session orchestration down here because `beads-daemon` consumes these modules. That ownership stays in `crates/beads-daemon/src/runtime/**`.
- Do not pull checkpoint refs, checkpoint cache, or store-branch migration into `wal`/`repl`; that belongs in `crates/beads-git/src/checkpoint/**` and `crates/beads-git/src/sync.rs`.
- Do not copy from `crates/beads-daemon/src/runtime/wal/mod.rs`. That file is a compatibility shim and legacy bait; shared WAL mechanics belong here.
- Do not weaken replay atomicity or typed cursor/watermark handling with ad-hoc sqlite assumptions outside `src/wal/replay.rs` and `src/wal/contract.rs`. Recent replay hardening lives there for a reason.

## Tests
- Keep crate-local tests here when they exercise shared primitives only.
- If a test needs the daemon runtime process, crate-level orchestration, or the daemon public seam, move it to `crates/beads-daemon`.

## Verification
- Run `cargo test -p beads-daemon-core --all-features` for the full primitive matrix.
- Run `cargo check -p beads-daemon-core --all-features` when touching `wal-fs`, `wal-sqlite`, `test-harness`, `model-testing`, or other gated code.
- When changing `src/repl/proto.rs` or `src/wal/contract.rs`, keep or add local unit coverage in the same module. That is the cheapest correct proof loop here.
