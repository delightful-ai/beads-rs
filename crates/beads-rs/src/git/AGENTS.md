## Boundary
This directory is the git sync layer.
Depends on: `git2` + `src/core/` types + `serde_json` wire helpers.
Depended on by: `src/daemon/` (background sync) and `src/repo.rs` (state loading).
NEVER: implement workflow/business rules (that belongs in `core/` + `daemon/ops.rs`).

## How to work here
- State lives on `refs/heads/beads/store`; keep it isolated from code branches.
- Preserve the sync model in `src/git/sync.rs`: linear history, retry on non-fast-forward, no merge commits.
- Wire format changes live in `src/git/wire.rs`; prefer additive changes and keep parsing tolerant.

## Verification
- `cargo test`
- Optional narrowing: `cargo test <pattern>` (e.g., `wire` / `sync`) to iterate faster.

## Don’t copy this
- Don’t reach into daemon state or CLI rendering from here; the git layer should stay “dumb but correct”.
