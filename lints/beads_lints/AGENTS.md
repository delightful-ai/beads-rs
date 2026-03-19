## Boundary
This crate owns beads-rs specific lint policies and the diagnostics that point code back to the allowed seam.
Depends on: `dylint_linting`, `rustc_*` internals, and local `clippy_utils`.
NEVER: depend on `crates/beads-*` runtime code from lint logic.

## How to work here
Golden example:
- `lints/beads_lints/src/lib.rs` (`CLI_DAEMON_BOUNDARY`) for lint declaration, registration, and diagnostics.

When adding or changing a lint:
1. Declare lint + pass registration close together so scope is explicit.
2. Prefer `clippy_utils` traversal/diagnostic helpers before hand-rolled AST matching.
3. Keep matching logic in small helpers (path parsing, filename checks, allowlists) to keep `check_*` readable.
4. Add a failing fixture and expected stderr in `lints/beads_lints/ui/src/`.
5. Add/adjust a passing fixture for non-regression.
6. Make diagnostics point to the allowed seam, not just the forbidden one.

Verification:
- `cargo test -p beads_lints --manifest-path lints/Cargo.toml`
- `cargo dylint --path lints --pattern beads_lints --all`

Invariants:
- Diagnostics must explain the boundary violation and point to the allowed alternative seam.
- Lints should be deterministic (no file-order dependence).
- Avoid duplicate emissions for the same source location.

## Don't copy this
Legacy pattern: giant `check_*` methods with inlined string matching.
Use instead: extract helper functions and centralize canonical matching rules.

Legacy pattern: writing a lint without UI fixtures.
Use instead: always pair lint logic with `.rs` and `.stderr` fixtures.
