## Boundary
This directory owns the CLI parse/dispatch surface and command handlers for `bd`.
Depends on: `beads-core`, `beads-api`, `beads-surface`, and internal runtime/render helpers.
Depended on by: `crates/beads-rs/src/cli/mod.rs` host adapter.
NEVER: import `beads-rs` or daemon internals directly.

## Shared Helpers
- Reuse command helpers from `commands/common.rs` before adding local duplicates.
- Use `commands/common::fmt_wall_ms` for wall-clock timestamp rendering.
- Use `commands/common::fetch_issue` for shared show-fetch logic (`Request::Show` + `QueryResult::Issue` extraction).
- If a helper is reused by multiple command modules, move it to `commands/common.rs`.

## Verification
- `cargo fmt --all`
- `just dylint`
- `cargo clippy --all-features -- -D warnings`
- `cargo test --all-features`
