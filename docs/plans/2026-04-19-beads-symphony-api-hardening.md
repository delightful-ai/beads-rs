# Beads Symphony API Hardening Execution Note

**Kind:** implementation/execution note

**Goal:** Turn the current `beads-http` transport stub into a reliable tracker/board backend that can support a simple board client and run the local Symphony prototype against beads instead of Linear.

**Base Revision:** `claude/api-layer-beads-tQP3A@origin` (`qstxmqpytssn`, commit `5e07ea18`)

**Primary Beads:**
- `beads-rs-suj8` — epic: harden beads daemon board/tracker API for Symphony-style orchestration
- `beads-rs-suj8.1` — execution doc + verification log
- `beads-rs-suj8.2` — typed tracker/board daemon RPC surface
- `beads-rs-suj8.3` — Symphony beads tracker adapter
- `beads-rs-suj8.4` — simple board app + end-to-end proof

## Scope Boundary

- `crates/beads-http` stays a thin transport over the existing `Request` / `Response` JSON contract.
- New app-facing behavior should land as typed daemon/query surfaces in `beads-api` + `beads-surface`, then flow through the daemon and HTTP transport.
- Durable task tracking lives in `bd`, not in this document. This file exists to capture execution order, decisions, and verification evidence.

## Confirmed Starting Facts

1. The current branch already adds `crates/beads-http` with:
   - `POST /rpc`
   - `GET /healthz`
   - `GET /subscribe` returning `501`
2. `beads-http` currently forwards raw `beads_surface::Request` values to `IpcClient` and maps transport errors onto `Response::Err`.
3. `beads-core` already has `Workflow::{Open, InProgress, Closed}`. It does not natively model Symphony states such as `Human Review`, `Rework`, or `Merging`.
4. Symphony Elixir already exposes a tracker adapter seam:
   - `fetch_candidate_issues/0`
   - `fetch_issues_by_states/1`
   - `fetch_issue_states_by_ids/1`
   - `create_comment/2`
   - `update_issue_state/2`
5. Symphony currently only accepts `tracker.kind` of `linear` or `memory`.

## Locked Decisions

1. No parallel REST tracker contract inside `crates/beads-http`.
   The typed tracker surface should be added to the daemon RPC contract and reused over HTTP.
2. Hard cutover only.
   If a tracker/board concept needs a stable API, implement it as the new canonical surface rather than adding a temporary compatibility path.
3. One app-facing state model.
   Board/Symphony consumers should see a single explicit tracker-state concept instead of reverse-engineering multiple bead fields ad hoc.
4. Symphony integration must hit real beads transport.
   The acceptance bar is not “adapter compiles”; it is a local run where Symphony fetches beads work, updates beads state, and writes progress/comments back through the beads backend.

## Working Implementation Direction

### Tracker Surface

Add a typed tracker-oriented RPC layer that can support:
- candidate-issue listing by tracker state
- issue refresh by ids
- tracker-state transitions
- tracker comments / workpad notes
- board-friendly issue projections

### Tracker State Model

Current best direction:
- keep core bead workflow semantics intact
- expose an explicit tracker/board state model for app consumers
- make the storage mapping deterministic and validated

This is still the main design hinge. If the implementation proves that tracker state must become first-class in core to remain truthful, promote it there instead of hiding permanent semantics in a loose adapter.

### Symphony Integration

Modify the local Symphony Elixir prototype to add:
- `tracker.kind: beads`
- beads endpoint config
- a beads-backed tracker adapter that talks to the new daemon surface over HTTP

## Execution Order

1. Keep this execution note current while the branch evolves.
2. Define the tracker-state model and typed RPC shapes in `beads-api` / `beads-surface`.
3. Implement daemon handlers and HTTP coverage for the tracker surface.
4. Add a simple board client and automated end-to-end proof.
5. Modify Symphony to use a beads tracker adapter.
6. Run local end-to-end verification with Symphony against a beads-backed issue set.

## Verification Log

- 2026-04-19:
  - ran `bd prime`
  - created sibling workspace `/Users/darin/src/personal/beads-rs-symphony-api`
  - confirmed workspace base revision is `claude/api-layer-beads-tQP3A@origin`
  - inspected `crates/beads-http`, `beads-surface`, and local Symphony Elixir tracker boundary
  - created epic `beads-rs-suj8` with child beads `.1` through `.4`

## Open Risks

1. The honest place for tracker state may be deeper than a transport-layer addition.
2. Symphony assumes richer state names than the current beads core model exposes.
3. A thin raw RPC bridge is easy to test but not enough proof by itself; browser/manual exercise must catch shape or ergonomics problems.
