## Boundary
This directory is the lint workspace for custom Dylint rules and their vendored lint authoring utilities.
Depends on: `rustc_private` nightly APIs, `dylint_linting`, local `clippy_utils`.
Depended on by: repo lint gates (`just dylint`, CI lint jobs).
NEVER: ship product/runtime behavior from here or encode business logic that belongs in `crates/*`.

## How to work here
Golden examples:
- `/Users/darin/src/personal/beads-rs/lints/beads_lints/src/lib.rs` for a repo-specific lint.
- `/Users/darin/src/personal/beads-rs/lints/beads_lints/tests/ui.rs` for UI-style lint tests.

When changing lint behavior:
1. Prefer `clippy_utils` helpers over ad-hoc `rustc_*` traversals when a helper exists.
2. Keep diagnostics stable and actionable (`what failed`, `why`, `what to do instead`).
3. Add/update UI fixtures under `/Users/darin/src/personal/beads-rs/lints/beads_lints/ui/`.
4. Run the verification commands below.

Verification:
- `cargo dylint --path lints --pattern beads_lints --all`
- `cargo test -p beads_lints --manifest-path /Users/darin/src/personal/beads-rs/lints/Cargo.toml`
- From repo root: `just dylint`

## Don't copy this
Legacy pattern: manually rebuilding path/AST utilities that already exist in `clippy_utils`.
Use instead: import the relevant `clippy_utils::*` helper and keep local logic focused on policy.

Legacy pattern: editing vendored `clippy_utils` for repo-specific behavior.
Use instead: keep repo-specific helpers in `beads_lints`; treat vendored code as upstream.
