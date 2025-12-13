## Boundary
This directory is CI/CD configuration (GitHub Actions) and GitHub issue templates.
Depends on: GitHub Actions marketplace actions and repo scripts.
Depended on by: contributors (PR checks) and release automation.
NEVER: make CI “green” by weakening correctness (e.g., removing `-D warnings`).

## How to work here
- Build workflow runs: `cargo check`, `cargo test`, `cargo fmt -- --check`, and `cargo clippy -- -D warnings`.
- Coverage job installs `llvm-tools-preview` + `grcov` and runs `cargo xtask coverage` (alias in `.cargo/config.toml`).

## Verification
- After editing workflows, sanity-check YAML and run the referenced `cargo` commands locally where possible.

## Gotchas
- This repo currently defines the `cargo xtask` alias but does not include an `xtask` package; coverage will fail until that’s added or CI is adjusted.
