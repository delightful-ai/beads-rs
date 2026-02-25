## Boundary
This crate owns git-backed state sync and checkpoint publication.
Depends on: `beads-core` types, git plumbing (`git2`), and checkpoint/wire helpers.
Depended on by: `beads-daemon` runtime and migration/verification tooling.
NEVER: add daemon runtime concerns, IPC transport, or CLI rendering here.

## How To Work Here
- Keep sync deterministic and branch-local to `refs/heads/beads/store`.
- Preserve the non-fast-forward recovery flow in `sync.rs` (backup ref + retry).
- Treat wire/checkpoint formats as compatibility boundaries; make additive changes.
- Use typed digests and IDs from `beads-core`; avoid raw stringly state.

## Verification
- `cargo check -p beads-git`
- `cargo test -p beads-git`

## Don't Copy This
- Don't reach into daemon internals for metrics/state ownership decisions.
- Don't duplicate snapshot serialization logic; use the shared codec path.
- Don't introduce ad-hoc ref names or alternate store branch conventions.
