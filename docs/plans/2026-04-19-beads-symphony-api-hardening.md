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
- `beads-rs-suj8.5` — make tracker state first-class in core instead of deriving it

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

Current branch status:
- added `TrackerIssue`, `TrackerBlocker`, and `TrackerState` to `beads-api`
- added `Request::TrackerList` plus `TrackerListPayload` to `beads-surface`
- added daemon-side tracker projection and state derivation in `query_executor.rs`
- verified the read surface compiles across `beads-api`, `beads-surface`, `beads-daemon`, and `beads-http`
- remaining gap: first-class tracker write operations still need to be added so Symphony and a board client do not have to synthesize raw bead ops

### Tracker State Model

Current best direction:
- keep core bead workflow semantics intact
- expose an explicit tracker/board state model for app consumers
- make the storage mapping deterministic and validated

Current execution choice:
- start with a validated derived mapping for the Symphony-facing API so the branch can prove the end-to-end contract quickly
- track the honest long-term fix separately as `beads-rs-suj8.5`

Derived mapping currently in flight:
- `Open` -> `Todo`
- `InProgress` -> `In Progress` unless a reserved `tracker-state:*` label upgrades it to `Human Review`, `Rework`, or `Merging`
- `Closed(reason=done)` -> `Done`
- `Closed(reason=cancelled|canceled)` -> `Cancelled`
- `Closed(reason=duplicate)` -> `Duplicate`
- other closed reasons -> `Closed`

Guardrails:
- open beads may not carry reserved tracker-state labels
- closed beads may not carry reserved tracker-state labels
- in-progress beads may carry at most one reserved tracker-state label

### Symphony Integration

Modify the local Symphony Elixir prototype to add:
- `tracker.kind: beads`
- beads endpoint config
- a beads-backed tracker adapter that talks to the new daemon surface over HTTP

## Execution Order

1. Keep this execution note current while the branch evolves.
2. Finish the typed tracker-state model and RPC shapes in `beads-api` / `beads-surface`.
3. Add first-class tracker write operations so state transitions and comments do not leak raw bead internals.
4. Add daemon and transport coverage for the tracker surface.
5. Modify Symphony to use a beads tracker adapter over the HTTP RPC surface.
6. Add a simple board client and exercise the main board flows locally.
7. Run local end-to-end verification with Symphony against a beads-backed issue set.

## Detailed TODOs

- `beads-rs-suj8.1`
  - [x] Create and maintain the execution note
  - [ ] Keep verification evidence current as each surface lands
  - [ ] Record exact Symphony runtime steps once the adapter runs locally
- `beads-rs-suj8.2`
  - [x] Add a Symphony-shaped tracker read model
  - [x] Add `tracker_list` to the daemon RPC request surface
  - [ ] Add first-class tracker mutation payloads and request variants
  - [ ] Add daemon planner support for atomic tracker state transitions
  - [ ] Add daemon planner support for tracker comments as a named surface
  - [ ] Add focused owner-crate tests for tracker mapping and transition validation
- `beads-rs-suj8.3`
  - [ ] Add `tracker.kind: beads` to Symphony config validation
  - [ ] Add a beads HTTP client in the Elixir prototype
  - [ ] Add a beads tracker adapter that returns `SymphonyElixir.Linear.Issue` structs
  - [ ] Route tracker adapter selection to the beads backend
  - [ ] Cover the adapter with Elixir tests that do not require a live Linear token
- `beads-rs-suj8.4`
  - [ ] Add a small client or board app that talks to `/rpc`
  - [ ] Prove list-by-state, refresh-by-id, transition, and comment flows
  - [ ] Add one process-level daemon/HTTP assembly test
  - [ ] Run a local manual/browser sanity pass against the simple app
- `beads-rs-suj8.5`
  - [ ] Leave the bead detailed enough that a later pass can cut over cleanly
  - [ ] Revisit only if the derived mapping starts lying or blocking the Symphony flow

## Verification Log

- 2026-04-19:
  - ran `bd prime`
  - created sibling workspace `/Users/darin/src/personal/beads-rs-symphony-api`
  - confirmed workspace base revision is `claude/api-layer-beads-tQP3A@origin`
  - inspected `crates/beads-http`, `beads-surface`, and local Symphony Elixir tracker boundary
  - created epic `beads-rs-suj8` with child beads `.1` through `.5`
  - ran `cargo test -p beads-http`
  - ran `cargo check -p beads-api -p beads-surface -p beads-daemon -p beads-http`

## Open Risks

1. The honest place for tracker state may be deeper than a transport-layer addition.
2. Symphony assumes richer state names than the current beads core model exposes.
3. A thin raw RPC bridge is easy to test but not enough proof by itself; browser/manual exercise must catch shape or ergonomics problems.
4. If tracker writes stay as raw `Update` / `Close` / label operations, the API layer will remain leaky even if Symphony can be made to work.
