## Boundary
This crate owns canonical emitted schemas for CLI and daemon JSON surfaces.
NEVER: recreate CLI-local/daemon-local view structs or conversion layers here; `beads-api` exists because `convert.rs` and `daemon/query_model.rs` were deleted.

## Routing
- Route schema changes to their real homes: issue/status shapes in `issues.rs`, admin/daemon status in `admin.rs`, aggregate query results in `query.rs`, dependency edges/cycles in `deps.rs`, and stream payloads in `realtime.rs`.
- New issue-facing shapes usually project from `beads_core::BeadProjection` or `BeadView`; keep those conversions here rather than rebuilding them in callers.
- If a payload needs to be smaller, add an explicit summary type here; do not silently drop fields from an existing type just to satisfy one caller.
- If a change is about request/response transport or client helpers, it belongs in `beads-surface`, not here.
- Keep compatibility changes additive unless a coordinated break is intentional.

## Verification
- `cargo test -p beads-api` proves projection-to-schema conversions and string enum serde in `issues.rs`/`admin.rs`.
- `cargo check -p beads-api` is the fast downstream breakage check after changing a public schema.
