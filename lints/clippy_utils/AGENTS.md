## Boundary
This directory is a vendored copy of upstream `clippy_utils` used to author custom lints.
Depends on: pinned nightly compiler internals (`nightly-2026-01-22` in `/Users/darin/src/personal/beads-rs/lints/rust-toolchain.toml`).
Depended on by: `/Users/darin/src/personal/beads-rs/lints/beads_lints`.
NEVER: add beads-rs specific policy logic here.

## How to work here
Treat this as third-party code first.

When you think you need to change this directory:
1. Confirm the needed helper does not already exist in `clippy_utils`.
2. If repo-specific behavior is needed, implement it in `beads_lints` instead.
3. Only patch vendored code for compatibility/unblockers, and keep patches minimal + isolated.
4. Preserve upstream style and module layout.

Verification:
- `cargo test -p beads_lints --manifest-path /Users/darin/src/personal/beads-rs/lints/Cargo.toml`
- `cargo dylint --path /Users/darin/src/personal/beads-rs/lints --pattern beads_lints --all`

## Don't copy this
Legacy pattern: forking deep utility code for one lint rule.
Use instead: compose existing `clippy_utils` APIs plus small local helpers in `beads_lints`.

Legacy pattern: ignoring nightly coupling.
Use instead: keep changes compatible with the pinned toolchain and document any required sync.
