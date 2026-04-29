# Beads Symphony API Hardening Execution Note

**Kind:** implementation/execution note

**Goal:** Turn the current `beads-http` transport stub into a reliable tracker/board backend that can support a simple board client and run the local Symphony prototype against beads instead of Linear.

**Base Revision:** `claude/api-layer-beads-tQP3A@origin` (`qstxmqpytssn`, commit `5e07ea18`)

## Latest Status

Current landed shape:

- beads now exposes one canonical rich public `status` surface; the old public `tracker_state` plus coarse `status` split is gone
- canonical repo-store format is `v2`, and the migration flow now carries the local daemon-store rebuild path as a first-class part of the story
- helper apps, e2e scripts, and migration tests were scrubbed onto `status` / `statuses`
- `--status` now means canonical rich status only; derived dependency state stays on `bd blocked` instead of masquerading as a pseudo-status selector
- Symphony's beads adapter/client now uses beads' canonical `status` wire contract while preserving Symphony's internal `Issue.state` vocabulary

Fresh end-to-end proof on the current tree:

- `cargo xtest`
- `cargo nextest run --profile slow --workspace --all-features --features slow-tests`
- `just dylint`
- `cargo clippy --all-features -- -D warnings`
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s /Users/darin/src/personal/beads-rs-symphony-api/scripts -p 'test_*.py' -v`
- `mise exec -- mix test test/symphony_elixir/extensions_test.exs test/symphony_elixir/beads_client_test.exs`
- `mise exec -- mix specs.check`
- `python3 /Users/darin/src/personal/beads-rs-symphony-api/scripts/e2e_symphony_beads.py --skip-build --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir`

Current live disposable proof:

- disposable root: `/var/folders/td/wg0qzsf54sl9rsb3fv8htg640000gn/T/beads-symphony-e2e.pzuky38i`
- lifecycle issue reached `Done`
- Symphony observed and used:
  - `tracker_create_issue`
  - `tracker_add_blocker`
  - `tracker_create_comment`
  - `tracker_update_issue_state`
- repo-truth follow-up issue title: `Fuse terminal branch metadata into canonical status lifecycle for origin-c7r`

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
3. `beads-core` now owns a canonical `TrackerState` enum for board/work execution truth:
   - `Todo`
   - `In Progress`
   - `Human Review`
   - `Rework`
   - `Merging`
   - `Done`
   - `Cancelled`
   - `Duplicate`
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
- added `Request::TrackerList`, `Request::TrackerTransition`, and `Request::TrackerComment` to `beads-surface`
- added daemon-side tracker projection, state derivation, transition planning, and tracker-comment plumbing
- cut core bead storage over to `BeadFields::tracker_state: Lww<TrackerState>` plus terminal-only `closed_on_branch`
- removed the old `Workflow` + `TrackerActiveState` split from core writable state
- tracker transitions no longer persist reserved `tracker-state:*` labels, and they no longer fan out into multiple workflow/substate writes
- coarse `WorkflowStatus::{Open, InProgress, Closed}` remains only as a derived query/rendering view
- close-reason parsing now lives in core tracker semantics and is reused by daemon planning plus CLI validation
- migration-only git parsing now synthesizes `tracker_state` from legacy `status` rows so old store refs still migrate cleanly
- fixed tracker blocker projection to follow real bead dependency semantics (`blocked -> blocker`)
- fixed `bd-http` autostart to launch `bd daemon run` instead of recursively trying `bd-http daemon run`
- added an assembly HTTP test plus an operator-facing board client, a browser board app, and disposable-repo e2e scripts
- remaining honest gap: terminal branch metadata still lives in a separate `closed_on_branch` field, so full terminal-state unrepresentability is not finished yet

### Tracker State Model

Canonical core model now in use:
- `Todo`
- `In Progress`
- `Human Review`
- `Rework`
- `Merging`
- `Done`
- `Cancelled`
- `Duplicate`

Explicit choices:
- there is no canonical `Closed` tracker state
- tracker/board execution truth lives in one core field
- coarse workflow status is derived for compatibility and query grouping only

Derived coarse view:
- `Todo` -> `open`
- `In Progress` / `Human Review` / `Rework` / `Merging` -> `in_progress`
- `Done` / `Cancelled` / `Duplicate` -> `closed`

Terminal close semantics:
- `bd close <id>` with no reason resolves to `Done`
- allowed explicit terminal reasons are `done`, `cancelled` / `canceled`, and `duplicate`
- CLI validation and daemon planning both use the same core tracker-state mapping

Migration-only legacy mapping:
- legacy `status: open` -> `Todo`
- legacy `status: in_progress` -> `In Progress`
- legacy `status: closed` -> terminal tracker state derived from `closed_reason`, defaulting to `Done` when the old reason is freeform or absent

Guardrails:
- non-terminal tracker states clear `closed_on_branch`
- terminal tracker transitions are atomic from the client point of view
- legacy store-ref parsing accepts old `status` rows at the git boundary without reintroducing writable dual truth in core

### Symphony Integration

Completed:
- `tracker.kind: beads`
- beads repo + RPC endpoint config
- a beads-backed tracker adapter that talks to the new daemon surface over HTTP
- tracker-native Codex dynamic tools for issue fetch/list/create/comment/state-update/blocker flows
- live fetch/update/comment proof against a real beads backend

Important integration fixes discovered during live proof:
- the beads client had to unwrap the real `beads_surface::Response` envelope (`{"ok": ...}` / `{"err": ...}`) instead of assuming the inner payload was the whole HTTP body
- the service boot path requires Symphony's explicit preview acknowledgement flag; once supplied, the dashboard and JSON API run normally on a beads-backed workflow
- Symphony's Codex app-server config needed approval-policy normalization from the older reject-map shape onto the app-server's explicit enum value; the safe runtime default is now `on-request` instead of silently drifting to `never`
- Symphony's Codex turn sandbox had to opt into `"networkAccess": true` for local `bd-http` calls from worker turns
- the checked-in `bin/symphony` escript can drift behind local source edits; rebuilding it with `mise exec -- mix escript.build` was required before the CLI proof matched the live `mix run` path
- Symphony's observability API only retained the single last Codex event; a bounded recent-event ring is now exposed so fast dynamic tool calls stay visible long enough for the dashboard and the repeatable harness to prove tracker-tool usage
- the repeatable lifecycle harness originally stopped on the first repo-truth `Done` edge, which could beat the `/api/v1/state` projection by a poll or two; the proof loop now waits for both terminal repo truth and the expected tracker-tool observations before it calls the run complete
- Symphony's issue drill-down endpoint originally 404ed once work moved from `running` into `recently_completed`; it now serves completed-session details off the same recent-completion snapshot the main state payload exposes
- the dashboard originally had no section for completed Codex runs, so a lifecycle could disappear from the UI as soon as it left `running`; it now renders a dedicated recently-completed table with JSON drill-down links
- the running issue snapshot originally kept stale tracker state until a later reconcile pass even after `tracker_update_issue_state` succeeded; the orchestrator now patches the in-memory issue state immediately from the completed dynamic-tool event
- the original eight-event Codex ring was too small for real worker traffic and could evict tracker transitions behind low-signal wrapper events; the retention window is now twenty-four events

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
  - [x] Keep verification evidence current as each surface lands
  - [x] Record exact Symphony runtime steps once the adapter runs locally
- `beads-rs-suj8.2`
  - [x] Add a Symphony-shaped tracker read model
  - [x] Add `tracker_list` to the daemon RPC request surface
  - [x] Add first-class tracker mutation payloads and request variants
  - [x] Add daemon planner support for atomic tracker state transitions
  - [x] Add daemon planner support for tracker comments as a named surface
  - [x] Add focused owner-crate tests for tracker mapping and transition validation
- `beads-rs-suj8.3`
  - [x] Add `tracker.kind: beads` to Symphony config validation
  - [x] Add a beads HTTP client in the Elixir prototype
  - [x] Add a beads tracker adapter that returns `SymphonyElixir.Linear.Issue` structs
  - [x] Route tracker adapter selection to the beads backend
  - [x] Cover the adapter with Elixir tests that do not require a live Linear token
- `beads-rs-suj8.4`
  - [x] Add a small client or board app that talks to `/rpc`
  - [x] Prove list-by-state, refresh-by-id, transition, comment, create, and dependency flows
  - [x] Add one daemon/HTTP assembly test at the product seam
  - [x] Run a local manual sanity pass against the simple app and Symphony dashboard endpoints
- `beads-rs-suj8.5`
  - [x] Move canonical `TrackerState` into `beads-core`
  - [x] Cut bead storage over to `tracker_state` plus `closed_on_branch`
  - [x] Remove canonical `Closed` from the board model
  - [x] Update CLI close/update validation to only allow canonical terminal reasons
  - [x] Keep legacy migration/import paths readable by synthesizing `tracker_state` from old `status` rows

## Current Artifacts

- `crates/beads-api/src/tracker.rs` — tracker issue/state schema
- `crates/beads-core/src/composite.rs` — canonical core `TrackerState` semantics + close-reason mapping
- `crates/beads-core/src/bead.rs` / `apply.rs` / `wire_bead.rs` — canonical tracker-state storage, validation, and wire cutover
- `crates/beads-daemon/src/runtime/tracker.rs` — tracker transition planning on top of the core tracker taxonomy
- `crates/beads-rs/tests/integration/daemon/http.rs` — assembly HTTP tracker flow proof
- `scripts/tracker_board.py` — simple operator-facing board client over `/rpc`
- `scripts/tracker_board_web.py` — same-origin browser board server that proxies the tracker RPC
- `scripts/tracker_board_web/` — browser kanban assets for create / transition / comment / blocker sanity passes
- `scripts/test_tracker_board_web.py` — board-web server regression tests for health/config/error-path behavior
- `scripts/e2e-tracker-api.sh` — disposable-repo HTTP tracker e2e script
- `scripts/e2e_symphony_beads.py` — disposable end-to-end harness for Symphony tracker reads/writes plus dashboard boot against a fresh beads backend
- `scripts/test_e2e_symphony_beads.py` — focused test for the Symphony harness workflow/config rendering
- `vendor/github.com/openai/symphony/elixir/lib/symphony_elixir/beads/*.ex` — beads tracker adapter/client
- `vendor/github.com/openai/symphony/elixir/lib/symphony_elixir/codex/dynamic_tool.ex` — tracker-native Codex tool surface for beads workflows
- `vendor/github.com/openai/symphony/elixir/lib/symphony_elixir/orchestrator.ex` — bounded recent Codex-event retention on running workers plus immediate tracker-state reflection from successful state-update tools
- `vendor/github.com/openai/symphony/elixir/lib/symphony_elixir_web/presenter.ex` — observability API projection of recent running-worker and recently-completed events, including completed issue drill-down
- `vendor/github.com/openai/symphony/elixir/lib/symphony_elixir_web/live/dashboard_live.ex` — dashboard rendering for recently completed Codex sessions

## Next Planned Cut

- dedicated migration/status hard-cut plan: `docs/plans/2026-04-20-status-hard-cut-migration.md`
- local daemon store upgrade/reset sharp edge: `beads-rs-ee69`

## Live Proof Notes

Disposable repo proof used:
- repo: `/tmp/beads-symphony-e2e.GjFWGv/repo`
- beads HTTP: `http://127.0.0.1:7788/rpc`
- Symphony workflow: `/tmp/beads-symphony-e2e.GjFWGv/WORKFLOW.md`

Browser board proof used:
- repo: `/tmp/beads-board-browser.ocIdT8/repo`
- beads HTTP: `http://127.0.0.1:7791/rpc`
- board web app: `http://127.0.0.1:8091/`
- Symphony workflow: `/tmp/beads-board-browser.ocIdT8/WORKFLOW.md`
- Symphony dashboard: `http://127.0.0.1:4014/`

Direct Symphony worker proof used:
- workspace root: `/tmp/beads-symphony-worker.5B4ElT`
- worker proof artifact: `/tmp/beads-symphony-worker.5B4ElT/workspaces/origin-pz7/WORKER_PROOF.txt`
- orchestration sanity: `%{running: ["origin-pz7"], retry_attempts: %{}}`

Repeatable Symphony harness proof used:
- command: `python3 scripts/e2e_symphony_beads.py --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir`
- disposable root: `/var/folders/td/wg0qzsf54sl9rsb3fv8htg640000gn/T/beads-symphony-e2e.g60u7y1u`
- proven operations:
  - `SymphonyElixir.Tracker.fetch_candidate_issues/0`
  - `SymphonyElixir.Tracker.update_issue_state/2`
  - `SymphonyElixir.Tracker.create_comment/2`
  - `SymphonyElixir.Tracker.fetch_issue_states_by_ids/1`
  - `./bin/symphony --i-understand-that-this-will-be-running-without-the-usual-guardrails --port ...`
  - repo-truth cross-check through `bd show`
  - full Codex worker lifecycle that wrote `LIFECYCLE_PROOF.txt`, posted a tracker comment, transitioned the issue to `Done`, and preserved the proof file through the workflow hook path

Tracker-native dynamic-tool proof used:
- command: `python3 scripts/e2e_symphony_beads.py --skip-build --keep --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir`
- disposable root: `/var/folders/td/wg0qzsf54sl9rsb3fv8htg640000gn/T/beads-symphony-e2e.ctkcamwq`
- observed through `/api/v1/state` recent-event history during the live worker run:
  - `tracker_create_comment`
  - `tracker_update_issue_state`
- lifecycle proof artifact: `/var/folders/td/wg0qzsf54sl9rsb3fv8htg640000gn/T/beads-symphony-e2e.ctkcamwq/workspaces/origin-1ds/LIFECYCLE_PROOF.txt`

Expanded tracker-lifecycle proof used:
- command: `python3 scripts/e2e_symphony_beads.py --skip-build --keep --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir`
- disposable root: `/var/folders/td/wg0qzsf54sl9rsb3fv8htg640000gn/T/beads-symphony-e2e.gy0myxoj`
- observed through `/api/v1/state` during the live worker run:
  - `tracker_create_issue`
  - `tracker_add_blocker`
  - `tracker_create_comment`
  - `tracker_update_issue_state`
- proven repo-truth outcomes:
  - lifecycle issue reached `Done`
  - follow-up issue `origin-lye` was created with title `Tracker state first-class cutover for origin-cz4`
  - the follow-up issue was blocked by the lifecycle issue
  - the lifecycle proof file was preserved through the workflow cleanup hook

Post-cut Symphony proof used:
- command: `python3 scripts/e2e_symphony_beads.py --skip-build --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir`
- disposable root: `/var/folders/td/wg0qzsf54sl9rsb3fv8htg640000gn/T/beads-symphony-e2e.6d3ss81q`
- proven after the core tracker-state hard cut:
  - fresh beads repo boot still works through `bd-http`
  - Symphony still fetched candidates, transitioned state, and completed a real Codex lifecycle
  - the doing issue advanced to `Human Review`
  - the lifecycle issue `origin-q4g` reached `Done`
  - the lifecycle created follow-up issue `origin-v49`
  - the live worker used:
    - `tracker_create_issue`
    - `tracker_add_blocker`
    - `tracker_create_comment`
    - `tracker_update_issue_state`

Live bugs found and fixed:
1. tracker `blocked_by` was inverted in the daemon projection because the implementation followed incoming `blocks` edges instead of the repo's real `blocked -> blocker` outgoing edge convention
2. `bd-http` autostart was broken because `IpcClient` defaults to `current_exe() daemon run`, which becomes `bd-http daemon run` when called from the HTTP binary
3. Symphony's beads client was assuming raw payload bodies instead of the real `Response` envelope returned by `beads-http`
4. the browser board server needed explicit timeout handling and a quiet `/favicon.ico` path so app errors surfaced as structured `502`s instead of noisy hangs
5. the crate DAG policy needed to allow the intentional `beads-rs -> beads-http` assembly-test dev-dependency introduced by the new product seam
6. Symphony's app-server approval policy needed config normalization so the beads-backed Codex runtime would boot under the current local config shape
7. tracker-state reserved labels were still being used as live storage until the core `tracker_active_state` cut removed that side channel
8. the repeatable Symphony harness was initially testing a stale `bin/symphony` escript artifact instead of current source; rebuilding the escript closed the mismatch between the background CLI proof and the direct `mix run` proof
9. the lifecycle proof originally assumed workspaces survive terminal cleanup; the harness now accepts either live-workspace proof or hook-preserved proof and explicitly drives `/api/v1/refresh` so the CLI path dispatches deterministically
10. generalizing Symphony's dynamic tool surface initially regressed `linear_graphql` validation failures to a generic error path; the tool now keeps specific validation/auth error payloads while also advertising the new tracker tools
11. runtime approval-policy normalization was still treating the legacy reject-map shape as `never`, which broke the safer-default approval gate; it now resolves to `on-request` and the app-server/core tests assert the current behavior
12. the observability API only exposed the single latest Codex message, which made fast tracker-tool calls racy to prove; the orchestrator now keeps a bounded recent ring and the presenter projects it through `/api/v1/state`
13. the lifecycle harness originally treated repo-truth `Done` as immediate success/failure boundary, which made the proof flaky even when Symphony had correctly executed the final tracker tool; the harness now keeps polling `/api/v1/state` until the expected tracker-tool fragments catch up or the short post-`Done` grace window expires
14. `/api/v1/:issue_identifier` originally returned `404` for recently completed issues because it only searched `running` and `retrying`; it now serves completed issue payloads from `recently_completed`
15. the dashboard originally dropped completed sessions on the floor after they left `running`; it now exposes a `Recently completed sessions` table linked back to the JSON detail route
16. the running snapshot originally kept the old tracker state until a later tracker refresh even after the worker had already completed `tracker_update_issue_state`; the orchestrator now patches `issue.state` directly from the successful tool-call event
17. the original eight-event recent ring was too shallow for noisy real Codex traffic; after one live proof dropped `tracker_update_issue_state` behind later wrapper events and warnings, the ring was widened to twenty-four events
18. the core tracker-state hard cut initially broke legacy store-ref migration because old `state.jsonl` rows still decoded directly into a wire type that now required `tracker_state`; the git parser now synthesizes `tracker_state` from legacy `status` rows before snapshot decode

Live Symphony commands that now work:
- `mise exec -- mix run --no-start -e 'SymphonyElixir.Workflow.set_workflow_file_path(".../WORKFLOW.md"); Application.ensure_all_started(:symphony_elixir); IO.inspect(SymphonyElixir.Tracker.fetch_candidate_issues(), limit: :infinity)'`
- `mise exec -- mix run --no-start -e '... IO.inspect(SymphonyElixir.Tracker.update_issue_state("repo-813", "In Progress")); IO.inspect(SymphonyElixir.Tracker.create_comment("repo-813", "Symphony beads adapter live comment")); ...'`
- `mise exec -- ./bin/symphony --i-understand-that-this-will-be-running-without-the-usual-guardrails --port 4011 /tmp/beads-symphony-e2e.GjFWGv/WORKFLOW.md`
- `mise exec -- mix run --no-start -e '... IO.inspect(SymphonyElixir.MainLoop.run("Write /tmp/beads-symphony-worker.5B4ElT/workspaces/origin-pz7/WORKER_PROOF.txt with the exact contents worker proof ok"))'`
- `python3 scripts/e2e_symphony_beads.py --skip-build --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir` after the harness rebuilds `bin/symphony`, renders `dashboard_enabled: false`, posts `/api/v1/refresh`, and verifies the full Codex-backed `Done` transition plus proof artifact
- `python3 scripts/e2e_symphony_beads.py --skip-build --keep --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir` after the harness inspects `/api/v1/state` recent-event history and proves the live worker called `tracker_create_comment` and `tracker_update_issue_state`
- `python3 scripts/e2e_symphony_beads.py --skip-build --keep --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir` after the expanded lifecycle prompt proves the live worker called `tracker_create_issue`, `tracker_add_blocker`, `tracker_create_comment`, and `tracker_update_issue_state`, then cross-checks the created follow-up issue and dependency with `bd show`
- `python3 scripts/e2e_symphony_beads.py --skip-build --keep --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir` after the harness also verifies `/api/v1/<issue>` returns completed-session details, retains the tracker state tool in recent history, and `/` renders the recently completed dashboard section for that lifecycle

Live browser board actions that now work:
- create a bead through the board UI
- transition it from `Todo` to `In Progress`
- add a tracker comment through the board dialog
- inject a CLI-created blocker and see `Blocked by: ...` render after refresh
- cross-check the same issue through `bd show`

Live Symphony browser proof that now works:
- `mise exec -- ./bin/symphony --i-understand-that-this-will-be-running-without-the-usual-guardrails --port 4014 /tmp/beads-board-browser.ocIdT8/WORKFLOW.md`
- `GET /` renders the dashboard in a real browser
- the dashboard loads cleanly against the same beads-backed tracker state used by the browser board proof

## Verification Log

- 2026-04-19:
  - ran `bd prime`
  - created sibling workspace `/Users/darin/src/personal/beads-rs-symphony-api`
  - confirmed workspace base revision is `claude/api-layer-beads-tQP3A@origin`
  - inspected `crates/beads-http`, `beads-surface`, and local Symphony Elixir tracker boundary
  - created epic `beads-rs-suj8` with child beads `.1` through `.5`
  - ran `cargo test -p beads-http`
  - ran `cargo check -p beads-api -p beads-surface -p beads-daemon -p beads-http`
- 2026-04-20:
  - ran `python3 -B -m py_compile scripts/tracker_board.py scripts/tracker_board_web.py`
  - ran `cargo build -p beads-rs --bin bd -p beads-http --bin bd-http`
  - ran `cargo test -p beads-daemon --lib tracker_`
  - ran `cargo test -p beads-daemon --lib runtime::tracker::tests`
  - ran `cargo test -p beads-rs --test integration daemon::http::tracker_http_flow_projects_blockers_and_supports_tracker_mutations --features e2e-tests`
  - ran `./scripts/e2e-tracker-api.sh`
  - ran `mise exec -- mix test test/symphony_elixir/core_test.exs test/symphony_elixir/extensions_test.exs`
  - ran `mise exec -- mix specs.check`
  - ran live beads HTTP proof against a disposable repo, including:
    - `tracker_list`
    - `tracker_transition`
    - `tracker_comment`
    - daemon autostart through `bd-http`
  - ran live Symphony proof against the disposable beads backend, including:
    - `SymphonyElixir.Tracker.fetch_candidate_issues/0`
    - `SymphonyElixir.Tracker.fetch_issue_states_by_ids/1`
    - `SymphonyElixir.Tracker.update_issue_state/2`
    - `SymphonyElixir.Tracker.create_comment/2`
    - `./bin/symphony ... --port 4011 ...`
    - `GET /api/v1/state`
    - `GET /`
  - ran live browser board proof against `/tmp/beads-board-browser.ocIdT8/repo`, including:
    - `GET http://127.0.0.1:8091/api/config`
    - `GET http://127.0.0.1:8091/api/healthz`
    - Playwriter create / transition / comment flow at `http://127.0.0.1:8091/`
    - CLI-created blocker plus browser refresh to prove `blocked_by` rendering
  - ran `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/test_e2e_symphony_beads.py -v`
  - ran `PYTHONDONTWRITEBYTECODE=1 python3 -m py_compile scripts/e2e_symphony_beads.py scripts/test_e2e_symphony_beads.py`
  - ran `mise exec -- mix escript.build`
  - ran `python3 scripts/e2e_symphony_beads.py --skip-build --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir`
  - proved the rebuilt `./bin/symphony` CLI can:
    - fetch beads candidates
    - dispatch a real Codex turn after `/api/v1/refresh`
    - write the lifecycle proof file
    - post a tracker comment
    - transition the issue to `Done`
    - `bd show origin-3om --repo /tmp/beads-board-browser.ocIdT8/repo`
  - ran live Symphony dashboard browser proof against the same beads-backed browser rig:
    - `./bin/symphony ... --port 4014 /tmp/beads-board-browser.ocIdT8/WORKFLOW.md`
    - Playwriter load of `http://127.0.0.1:4014/`
  - ran direct Symphony worker proof against `/tmp/beads-symphony-worker.5B4ElT`, including:
    - `SymphonyElixir.MainLoop.run/1`
    - worker workspace creation under `workspaces/origin-pz7/`
    - proof artifact write to `WORKER_PROOF.txt`
    - orchestrator state check showing `%{running: ["origin-pz7"], retry_attempts: %{}}`
  - ran `cargo test -p beads-core workflow_patch_reopen_clears_tracker_active_state`
  - ran `cargo check --workspace`
  - ran `cargo fmt --all`
  - ran `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_tracker_board_web.py' -v`
  - reran `cargo test -p beads-daemon --lib tracker_`
  - reran `cargo test -p beads-rs --test integration daemon::http::tracker_http_flow_projects_blockers_and_supports_tracker_mutations --features e2e-tests`
  - reran `./scripts/e2e-tracker-api.sh`
  - hardened `scripts/e2e-tracker-api.sh` to rebuild fresh binaries by default after catching a stale-binary false proof
  - ran `cargo xtest`
  - ran `just dylint`
  - ran `cargo clippy --all-features -- -D warnings`
  - reran `mise exec -- mix test test/symphony_elixir/core_test.exs test/symphony_elixir/extensions_test.exs`
  - reran `mise exec -- mix specs.check`
  - ran `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest /Users/darin/src/personal/beads-rs-symphony-api/scripts/test_e2e_symphony_beads.py -v`
  - ran `python3 -m py_compile /Users/darin/src/personal/beads-rs-symphony-api/scripts/e2e_symphony_beads.py /Users/darin/src/personal/beads-rs-symphony-api/scripts/test_e2e_symphony_beads.py`
  - ran `python3 /Users/darin/src/personal/beads-rs-symphony-api/scripts/e2e_symphony_beads.py --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir`
  - ran `mise exec -- mix test test/symphony_elixir/dynamic_tool_test.exs`
  - ran `mise exec -- mix test test/symphony_elixir/app_server_test.exs`
  - ran `mise exec -- mix test test/symphony_elixir/workspace_and_config_test.exs`
  - ran `mise exec -- mix test test/symphony_elixir/extensions_test.exs`
  - ran `mise exec -- mix test test/symphony_elixir/orchestrator_status_test.exs`
  - ran `mise exec -- mix test test/symphony_elixir/dynamic_tool_test.exs test/symphony_elixir/app_server_test.exs test/symphony_elixir/workspace_and_config_test.exs test/symphony_elixir/core_test.exs test/symphony_elixir/extensions_test.exs test/symphony_elixir/orchestrator_status_test.exs`
  - reran `mise exec -- mix specs.check`
  - reran `mise exec -- mix escript.build`
  - reran `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest /Users/darin/src/personal/beads-rs-symphony-api/scripts/test_e2e_symphony_beads.py -v`
  - ran `python3 /Users/darin/src/personal/beads-rs-symphony-api/scripts/e2e_symphony_beads.py --skip-build --keep --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir`
  - proved the live worker completed on a fresh disposable backend while `/api/v1/state` exposed:
    - `tracker_create_comment`
    - `tracker_update_issue_state`
    - final repo truth still matched `Done` + comment + `LIFECYCLE_PROOF.txt`
  - reran `mise exec -- mix test test/symphony_elixir/orchestrator_status_test.exs test/symphony_elixir/extensions_test.exs`
  - reran `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest /Users/darin/src/personal/beads-rs-symphony-api/scripts/test_e2e_symphony_beads.py -v`
  - reran `python3 -m py_compile /Users/darin/src/personal/beads-rs-symphony-api/scripts/e2e_symphony_beads.py /Users/darin/src/personal/beads-rs-symphony-api/scripts/test_e2e_symphony_beads.py`
  - reran `python3 /Users/darin/src/personal/beads-rs-symphony-api/scripts/e2e_symphony_beads.py --skip-build --keep --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir`
  - reran `mise exec -- mix test test/symphony_elixir/dynamic_tool_test.exs test/symphony_elixir/app_server_test.exs test/symphony_elixir/workspace_and_config_test.exs test/symphony_elixir/core_test.exs test/symphony_elixir/extensions_test.exs test/symphony_elixir/orchestrator_status_test.exs`
  - reran `mise exec -- mix specs.check`
  - proved the expanded live worker lifecycle on a fresh disposable backend while `/api/v1/state` exposed:
    - `tracker_create_issue`
    - `tracker_add_blocker`
    - `tracker_create_comment`
    - `tracker_update_issue_state`
    - final repo truth matched `Done` + comment + follow-up issue creation + blocker wiring + preserved proof artifact
  - reran `mise exec -- mix test test/symphony_elixir/orchestrator_status_test.exs`
  - reran `mise exec -- mix test test/symphony_elixir/extensions_test.exs`
  - reran `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest /Users/darin/src/personal/beads-rs-symphony-api/scripts/test_e2e_symphony_beads.py -v`
  - reran `python3 -m py_compile /Users/darin/src/personal/beads-rs-symphony-api/scripts/e2e_symphony_beads.py /Users/darin/src/personal/beads-rs-symphony-api/scripts/test_e2e_symphony_beads.py`
  - reran `python3 /Users/darin/src/personal/beads-rs-symphony-api/scripts/e2e_symphony_beads.py --skip-build --keep --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir`
  - reran `mise exec -- mix test test/symphony_elixir/dynamic_tool_test.exs test/symphony_elixir/app_server_test.exs test/symphony_elixir/workspace_and_config_test.exs test/symphony_elixir/core_test.exs test/symphony_elixir/extensions_test.exs test/symphony_elixir/orchestrator_status_test.exs`
  - reran `mise exec -- mix specs.check`
  - proved on a fresh disposable backend that:
    - `/api/v1/state` exposed all four beads tracker tools during the live worker run
    - `/api/v1/<lifecycle identifier>` returned completed-session details once the lifecycle finished
    - the completed issue detail retained `tracker_update_issue_state` in recent history
    - `/` rendered the recently completed dashboard section for the completed lifecycle
  - reran `cargo fmt --all`
  - reran `cargo test -p beads-core --lib`
  - reran `cargo test -p beads-daemon --lib`
  - reran `cargo test -p beads-git`
  - reran `cargo test -p beads-cli`
  - reran `cargo test -p beads-rs --test core_state_fingerprint`
  - reran `cargo test -p beads-rs --test integration --features e2e-tests daemon::http::tracker_http_flow_projects_blockers_and_supports_tracker_mutations -- --exact`
  - reran `cargo test -p beads-rs --test integration --features e2e-tests cli::critical_path::test_create_show_close_workflow -- --exact`
  - reran `cargo test -p beads-rs --test integration --features "e2e-tests slow-tests" cli::critical_path::test_update_bead -- --exact`
  - reran `cargo test -p beads-rs --test integration --features "e2e-tests slow-tests" cli::critical_path::test_reopen_closed -- --exact`
  - reran `cargo test -p beads-rs --test integration --features "e2e-tests slow-tests" cli::migration::test_migrate_fixture_rich_workflow_rewrites_and_preserves_state -- --exact`
  - reran `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest /Users/darin/src/personal/beads-rs-symphony-api/scripts/test_e2e_symphony_beads.py -v`
  - reran `python3 /Users/darin/src/personal/beads-rs-symphony-api/scripts/e2e_symphony_beads.py --skip-build --symphony-root /Users/darin/vendor/github.com/openai/symphony/elixir`
  - proved on a fresh disposable backend after the core tracker-state cut that:
    - Symphony still transitioned the doing issue to `Human Review`
    - the lifecycle issue reached `Done`
    - the follow-up issue and blocker were still created correctly
    - the live worker still observed all four tracker tools through `/api/v1/state`

## Open Risks

1. `closed_on_branch` still lives as a separate LWW field validated against terminal `tracker_state`, so the canonical execution state is single-source now but terminal metadata is not yet fully fused into one unrepresentable type.
2. The new recent-event ring solves live observability for running workers, but completed issues still do not retain a durable Codex event history after they leave the running set; if post-hoc audit after cleanup matters, that needs a separate persistence cut.
3. The Symphony Elixir repo is still a dirty local workspace; the targeted tests were rerun and green, but the workspace has not been cleaned up or split into jj/git review units.
