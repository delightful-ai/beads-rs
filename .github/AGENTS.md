## Boundary
This directory owns GitHub Actions workflows and issue templates.
NEVER: make CI pass by weakening required checks.

## Local rules
- Keep test behavior aligned with `ci.yml`: PR/default CI runs the same all-features `cargo xtest` suite as local verification.
- Preserve the required gate set in `ci.yml`: `fmt`, `dylint`, and `clippy -D warnings` stay mandatory even when individual invocations change.
- Keep the Dylint cache/layout assumptions in sync with the workflows (`lints` checkout plus `~/.dylint_drivers`).
- If you touch release/archive shell in `workflows/release.yml`, keep artifact names and checksum flow aligned with `scripts/install.sh`.
- Re-read triggers, paths, and job names after edits so they still match the current repo layout.
