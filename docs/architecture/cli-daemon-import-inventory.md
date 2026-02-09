# CLI -> Daemon Direct Import Inventory

Date: 2026-02-09

## Scope

- Tree scanned:
  - `crates/beads-rs/src/cli/**/*.rs`
  - `crates/beads-cli/src/**/*.rs`
- Inventory rule: direct imports matching `use crate::daemon::...`

## Current State

No direct `crate::daemon::*` imports remain under either CLI tree.

## Ownership Notes

- Top-level parse/dispatch is owned by `crates/beads-cli/src/cli.rs`.
- Command handlers are owned by `crates/beads-cli/src/commands/**`.
- `crates/beads-rs/src/cli/mod.rs` is a host adapter only (repo/config/runtime hooks + delegation).
- `crates/beads-rs/src/cli/backend.rs` provides the typed host backend seam implementation.

## Verification Commands

```bash
rg -n "use crate::daemon::" crates/beads-rs/src/cli crates/beads-cli/src
just dylint
```

Expected result: no matches from the `rg` command and a passing `just dylint` run.
