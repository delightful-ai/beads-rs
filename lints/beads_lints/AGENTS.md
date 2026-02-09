## Boundary
This crate owns beads-rs specific lint policies (currently crate-boundary enforcement for CLI/daemon seams).
Depends on: `dylint_linting`, `rustc_*` internals, and local `clippy_utils`.
Depended on by: `just dylint` and all lint CI checks.
NEVER: depend on `crates/beads-*` runtime code or leak policy checks into non-lint crates.

## How to work here
Golden example:
- `/Users/darin/src/personal/beads-rs/lints/beads_lints/src/lib.rs` (`CLI_DAEMON_BOUNDARY`) for lint declaration, registration, and diagnostics.

When adding or changing a lint:
1. Declare lint + pass registration close together so scope is explicit.
2. Prefer `clippy_utils` traversal/diagnostic helpers before hand-rolled AST matching.
3. Keep matching logic in small helpers (path parsing, filename checks, allowlists) to keep `check_*` readable.
4. Add a failing fixture and expected stderr in `/Users/darin/src/personal/beads-rs/lints/beads_lints/ui/src/`.
5. Add/adjust a passing fixture for non-regression.

Tests that matter:
- `cargo test -p beads_lints --manifest-path /Users/darin/src/personal/beads-rs/lints/Cargo.toml`
- `cargo dylint --path /Users/darin/src/personal/beads-rs/lints --pattern beads_lints --all`

Invariants:
- Diagnostics must explain the boundary violation and point to the allowed alternative seam.
- Lints should be deterministic (no file-order dependence).
- Avoid duplicate emissions for the same source location.

## Don't copy this
Legacy pattern: giant `check_*` methods with inlined string matching.
Use instead: extract helper functions and centralize canonical matching rules.

Legacy pattern: writing a lint without UI fixtures.
Use instead: always pair lint logic with `.rs` and `.stderr` fixtures.
