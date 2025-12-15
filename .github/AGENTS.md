## Boundary
This directory is CI/CD configuration (GitHub Actions) and GitHub issue templates.
Depends on: GitHub Actions marketplace actions and repo scripts.
Depended on by: contributors (PR checks) and release automation.
NEVER: make CI “green” by weakening correctness (e.g., removing `-D warnings`).

## How to work here
- Build workflow runs: `cargo check`, `cargo test`, `cargo fmt -- --check`, and `cargo clippy -- -D warnings`.

## Verification
- After editing workflows, sanity-check YAML and run the referenced `cargo` commands locally where possible.

## Gotchas
- This repo does not currently run a coverage job in CI.
