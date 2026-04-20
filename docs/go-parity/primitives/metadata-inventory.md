# Metadata inventory: gascity → beads

**Pin:** beads v1.0.2 (c446a2ef) · gascity upstream at `~/vendor/github.com/gastownhall/gascity` (source-grounded against the tree as checked out).

This file is **raw territory**. It catalogs every metadata key gascity writes to or reads from bead metadata today, so that a companion file (`metadata-remapping.md`) can propose a typed home for each one. Every claim cites a source line. When a key is obviously part of a bigger feature, the group header explains the feature; individual key rows carry type, writers, readers, and concurrency risk.

There is no `metadata` primitive in beads-rs. This is the exhaustive list of things that must get a typed home or be explicitly declared "not store state" before gascity can ride on beads-rs.

---

## Method

Search scope: `~/vendor/github.com/gastownhall/gascity/internal/**/*.go`. Patterns grepped: `SetMetadata(`, `SetMetadataBatch(`, `.Metadata[`, `--set-metadata`, `--metadata-field`, namespace-prefix literals (`gc.`, `convergence.`, `convoy.`, etc.), and `MetadataKey` / `MetadataPatch` constants. Non-test `.go` files were prioritized; test files are cited where they are the only ground truth for a concurrency observation.

Types are inferred from the encoder/decoder Go actually uses: `strconv.Atoi`/`FormatInt` → int-as-string; `time.RFC3339`/`Truncate(time.Second)` → RFC3339 timestamp; `strconv.FormatBool`/`"true"`/`"false"` → bool-as-string; `json.Marshal` on the value → opaque JSON blob; else opaque string. Everything on the wire is `string`; Go runs the coercion at read time. Rows note the intended type, not the on-wire representation.

Concurrency: "**cc:**" marks each key with a one-word risk class.

- `single-writer` — exactly one subsystem writes the key; concurrent replicas cannot disagree about the value unless something is badly broken. Low risk.
- `lww-ish` — multiple replicas can legitimately write different values; last-writer-wins at the bd storage layer is currently correct by accident of map merge. Bd exposes no stamp per key, so the order at replay is whatever order the decorator happened to call `SetMetadata` in. **Any typed remap must preserve or tighten this.**
- `or-set` — the key holds an accumulating collection (CSV list, growing JSON array). Merging two writes by last-wins drops concurrent contributions — Dolt's behavior today is "last SetMetadata wins the entire value"; any typed port must use an OR-set / add-wins semantics.
- `append-only` — the key is monotonic (counter, log). Concurrent writes must resolve to max / concat-dedup rather than LWW.
- `controller-only` — written by a single controller role (daemon/supervisor); multi-replica writes are a bug, not a merge.

---

## 1. `convergence.*` — convergence loop state

One workflow primitive that keeps a convergence loop (agent verdict + optional scripted gate) alive on a single "control" bead. All keys are declared in `internal/convergence/metadata.go:13-41`. Writers live across `convergence/{create,retry,reconcile,manual,handler}.go`; readers in `convergence/{reconcile,manual,handler,evaluate,stop,template}.go` and `internal/convergence/evaluate.go:22`.

Decision axis: all of these belong to one bead (the "convergence root"). Today they are 27 scalar fields. Concurrency: a single controller drives state transitions, so most are `controller-only`, but the gate-result block is written after a long subprocess run while other writers may be mutating `state`/`active_wisp`, so the whole row needs atomic `SetMetadataBatch` semantics (which is currently simulated by sequential `SetMetadata` calls — see `bdstore.go:537-555`).

| Key | Type | Writers (file:line) | Readers | Lifecycle | cc |
|---|---|---|---|---|---|
| `convergence.state` | enum `{creating,active,waiting_manual,terminated}` — values at `metadata.go:47-52` | `create.go:78`, `create.go:72`, `retry.go:82`, `retry.go:76`, `reconcile.go:146`, `reconcile.go:192`, `reconcile.go:217`, `reconcile.go:555`, `manual.go:74`, `manual.go:192`, `manual.go:378`, `handler.go:414`, `handler.go:600` | `reconcile.go:*`, `manual.go:*`, `handler.go:*`, `stop.go`, `evaluate.go` | LWW per state machine step | controller-only |
| `convergence.iteration` | int (decimal) — encoded by `EncodeInt`/`DecodeInt` at `metadata.go:130-145` | `create.go:119`, `retry.go:124`, `reconcile.go:140`, `handler.go:190` | `reconcile.go`, `handler.go`, `evaluate.go` | monotonically increasing | append-only |
| `convergence.max_iterations` | int (decimal) | written at creation (via `var mw[]` pipeline, e.g. `create.go:96`) | `handler.go:190` area (fail-fast) | immutable after create | single-writer |
| `convergence.formula` | string (formula name) | creation-time config | `handler.go`, `retry.go:297` proxies | immutable | single-writer |
| `convergence.target` | string (target branch / identity) | creation | handler | immutable | single-writer |
| `convergence.gate_mode` | enum `{manual,condition,hybrid}` — `metadata.go:54-59`; also `events.go:77`,`98`,`125` | creation | gate-path in `evaluate.go`, `handler.go:680+` | immutable | single-writer |
| `convergence.gate_condition` | string (shell one-liner, must contain literals; see `evaluate.go:22,70` and `formula.go:24`) | creation | gate evaluator | immutable | single-writer |
| `convergence.gate_timeout` | Go duration string (parsed by `DecodeDuration` at `metadata.go:154`) | creation | gate runner | immutable | single-writer |
| `convergence.gate_timeout_action` | enum `{iterate,retry,manual,terminate}` — `metadata.go:62-67` | creation | gate runner timeout branch | immutable | single-writer |
| `convergence.active_wisp` | bead id (`BeadId` value) | `create.go:116`, `retry.go:121`, `reconcile.go:127,180,410,511`, `manual.go:197`, `handler.go:408,523` | `reconcile.go`, `handler.go`, `manual.go` | LWW; empty string means "none right now" | controller-only |
| `convergence.last_processed_wisp` | bead id | `reconcile.go:332,576`, `manual.go:106,436`, `handler.go:417,526,606` | `reconcile.go`, `handler.go`, `manual.go` | LWW advancing cursor | controller-only |
| `convergence.agent_verdict` | enum `{approve, approve-with-risks, block}` normalized by `NormalizeVerdict` at `metadata.go:113-127` | `manual.go:181,351`, `handler.go:458` | evaluator | clearable (`""` means "no verdict since last iteration") | controller-only |
| `convergence.agent_verdict_wisp` | bead id | `manual.go:184,354`, `handler.go:461` | evaluator; paired with `agent_verdict` for "whose verdict" | clearable | controller-only |
| `convergence.gate_outcome` | enum `{pass,fail,timeout,error}` — `metadata.go:78-83` | `handler.go:680` | `handler.go`, `evaluate.go` | LWW per gate run | controller-only |
| `convergence.gate_exit_code` | int-as-string (allow empty) | `handler.go:687` | `handler.go`, `evaluate.go` | LWW per gate run | controller-only |
| `convergence.gate_outcome_wisp` | bead id | `handler.go:710` | evaluator | LWW per gate run | controller-only |
| `convergence.gate_retry_count` | int | `handler.go:690` | retry policy | monotonic within a gate run | controller-only |
| `convergence.gate_stdout` | opaque blob (truncated per policy) | `handler.go:693` | evaluator, UI | LWW, potentially large | controller-only |
| `convergence.gate_stderr` | opaque blob (truncated) | `handler.go:696` | evaluator, UI | LWW | controller-only |
| `convergence.gate_duration_ms` | int ms (`strconv.FormatInt(..., 10)` at `handler.go:699`) | `handler.go:699` | evaluator | LWW per gate run | controller-only |
| `convergence.gate_truncated` | bool-as-string (`"true"`/`"false"`) | `handler.go:706` | UI/evaluator | LWW per gate run | controller-only |
| `convergence.terminal_reason` | enum `{approved,no_convergence,stopped,partial_creation}` — `metadata.go:70-75` | `reconcile.go:205`, `manual.go:65,369`, `handler.go:594` | listing | LWW at terminal step | controller-only |
| `convergence.terminal_actor` | actor identifier string | `reconcile.go:211,589`, `manual.go:68,372`, `handler.go:597` | audit | LWW at terminal step | controller-only |
| `convergence.waiting_reason` | enum `{manual,hybrid_no_condition,timeout,sling_failure}` — `metadata.go:86-91` | `reconcile.go:356`, `manual.go:71,189,375`, `handler.go:411` | reconciler | clearable | controller-only |
| `convergence.retry_source` | bead id (id of previous convergence whose retry this is) | `retry.go` (elided in scan; written by `mw[]` pipeline) | reconciler | immutable after retry create | single-writer |
| `convergence.city_path` | filesystem path | creation | check execution | immutable | single-writer |
| `convergence.evaluate_prompt` | string (prompt template) | creation | evaluator | immutable | single-writer |
| `convergence.pending_next_wisp` | bead id | `handler.go:239,781,807`, `reconcile.go:517`, `handler.go:531` | reconciler ("am I between iterations?") | clearable | controller-only |
| `var.<key>` | string (user-supplied template var) — prefix declared at `metadata.go:44` | `create.go:103`, `retry.go:108` | `template.go:42` (loops keys with `var.` prefix), `evaluate.go` | immutable per iteration; cleared on retry | single-writer per key |

Protected prefix: `convergence.*` keys are immutable from outside the convergence subsystem (`internal/convergence/acl.go:6`). Beads-rs must enforce this as a typed ACL, not as a string prefix filter.

**Event names** `convergence.created|iteration|terminated|waiting_manual|manual_approve|manual_iterate|manual_stop` at `events.go:12-18` are **event envelope labels**, not metadata keys. They live in the `EventEnvelope` stream, not on the bead. Listed here only to rule them out of the inventory.

---

## 2. `gc.*` — molecule, dispatch, routing, fanout, retry, ralph

This is the biggest namespace and the messiest. It overlays several subsystems that all stash workflow-topology state on individual step beads. There is no central constant file; keys are literal throughout. I've grouped by role.

### 2a. Molecule / workflow topology (set at creation of a step bead)

Establishes "which workflow does this bead belong to, which step is it, what kind of step is it".

| Key | Type | Writers | Readers | Lifecycle | cc |
|---|---|---|---|---|---|
| `gc.kind` | enum — empirical set: `{workflow, scope, cleanup, retry, retry-run, retry-eval, ralph, check, scope-check, run, spec, workflow-finalize}`. Collected from `molecule.go:383,414,421,449,923,987,994-998`, `molecule_test.go:104-127,462,463,1586-1610`, `formula/compile_test.go:481,560,567,764`, `ralph.go:866` (scope-check branch), `dispatch/fanout.go:346,366,456`, `api/handler_convoy_dispatch.go:68` (`"workflow"`). | formula compiler (`formula/compile.go`), molecule assembler, `graphroute.go:235,266` | step compilation; immutable per step identity | single-writer (compile), but step beads cross replicas, so `lww-ish` if two compilers ever race |
| `gc.formula_contract` | enum `{graph.v2}` (per `sling/sling_attachment.go:101`, `molecule_test.go:16,48`, `graphroute.go:84`). Marks a workflow root as using the graph.v2 format. | compile-time | `sling`, `graphroute`, `api/convoy_sql.go`, `api/orders_feed.go` | immutable | single-writer |
| `gc.formula_name` | string (formula basename) | compile-time | `api/orders_feed.go:381` | immutable | single-writer |
| `gc.workflow_id` | bead id (the root of a workflow) | `api/handler_convoy_dispatch.go:69,215`, `sling_core.go:360,523`, `sling_attachment.go:57,157-163` (uses the key as "this bead's attached workflow root"; note read and write are different beads — consumer-side on the source bead; producer-side on the root itself) | same files + `api/convoy_sql.go`, `api/orders_feed.go:258` | immutable after attach | single-writer per bead (different beads play different roles) |
| `gc.root_bead_id` | bead id (root of the workflow the step lives under) | `molecule.go:226,260,311,452,656,980`, `graph_apply.go:156-163`, `dispatch/fanout.go:51`, `dispatch/retry.go:196,297`, `dispatch/ralph.go:580,826,934`, `dispatch/runtime.go:113,470,502,632,695`, `api/orders_feed.go:84,110,126,207,258`, `api/convoy_sql.go:283-298` | every traversal that walks step→root | immutable after step creation (`molecule.go:450`: only written when empty) | single-writer |
| `gc.step_ref` | string — canonical step id within the workflow (e.g. `"scoped-demo.body"`) | `molecule.go:446,654`, `graph_apply.go:157,303`, `ralph.go:725,887,895,915`, many readers | traversal; `dispatch/fanout.go:460`, `dispatch/retry.go:413`, `api/convoy_sql.go:295` | immutable after first write (`molecule.go:445`: only set if empty) | single-writer |
| `gc.step_id` | string — unique non-positional step id (distinct from `step_ref` which carries path) | `molecule.go:998`, `dispatch/runtime.go:685`, `formula/compile_test.go:899` | `dispatch/runtime.go`, `ralph.go` | immutable | single-writer |
| `gc.logical_bead_id` | bead id — the "logical" identity of a run (stable across retries); `molecule.go:464,661`, `graph_apply.go:169,308-313`, `dispatch/ralph.go:378,384,491,660,686,922-936`, `dispatch/fanout.go:369`, `dispatch/retry.go:206`, `dispatch/runtime.go:454,661,685`, `api/convoy_sql.go:297` | same places + `dispatch/ralph.go:491` (chain-up) | immutable per bead | single-writer |
| `gc.root_store_ref` | rig / remote store identifier (freeform but shape is `"rig:<name>"` per `molecule/attach_test.go:293-306`) | `molecule.go:230,262`, `graphroute.go:424`, `molecule_test.go:383,422`, `sling_core.go:354` (via `sourceworkflow.SourceStoreRefMetadataKey`) | `sourceworkflow.go:94`, cross-store sync | immutable per bead | single-writer |
| `gc.source_store_ref` | same shape as root_store_ref, distinguishes "where this step was dispatched from" | `graphroute.go:431` | cross-store attribution | immutable | single-writer |
| `gc.source_bead_id` | bead id (the source/original bead whose workflow this is) | `sling_core.go:350`, `graphroute.go:429`, `api/orders_feed.go:156,258` | cross-store attribution, `api/orders_feed.go`, `graphroute_test.go:234` | immutable | single-writer |
| `gc.idempotency_key` | opaque string (molecule-scoped idempotency token) | `molecule.go:271,310,318`, compare at `molecule.go:318` | `molecule.go:318-321` | set at molecule-root creation only | single-writer |
| `idempotency_key` | opaque string (note: unprefixed; set on non-root children by `molecule.go:428` and `graph_apply.go:150`; distinct from `gc.idempotency_key` above) | `molecule.go:428`, `graph_apply.go:150` | `molecule_test.go:863` | immutable | single-writer |
| `gc.on_fail` | enum (`abort_scope`, others) | formula compile (`molecule_test.go:109,1693`) | molecule / dispatch scope reconcile | immutable | single-writer |
| `gc.on_exhausted` | enum `{hard_fail,soft_fail}` | formula compile (`formula/compile_test.go:885`) | `dispatch/control.go:27`, `dispatch/retry.go:20`, `dispatch/control.go:108` | immutable | single-writer |
| `gc.original_kind` | the step's pre-retry kind, preserved when compile rewrites `kind` | `formula/compile_test.go:553` | debugging / compile introspection | immutable | single-writer |
| `gc.scope_ref` | string — step ref of the body scope this member belongs to | `graphroute.go:438`, `molecule_test.go:109,127`, `dispatch/runtime.go:114`, `dispatch/ralph.go:606`, `dispatch/runtime.go:292,498,736` | many scope traversals | immutable per bead | single-writer |
| `gc.scope_role` | enum `{body,member,teardown}` | formula compile (`molecule_test.go:104-127`, `formula/compile_test.go:547,761`) | `dispatch/runtime.go:324,340,367,388,400` | immutable | single-writer |
| `gc.scope_name` | string (human scope name, e.g. `"work"`, `"worktree"`) | compile (`molecule_test.go:104`, `formula/compile_test.go:539`) | UI / debugging | immutable | single-writer |
| `gc.scope_kind` | enum `{city,...}` (partial from `graphroute_test.go:237`, `api/convoy_sql.go:280`) | `graphroute.go:435` | `api/convoy_sql.go:280` | immutable | single-writer |
| `gc.continuation_group` | string (scheduler tag) — `molecule_test.go:118,1693` | compile | scheduler | immutable | single-writer |
| `gc.template` | string (agent template name) — `agentutil/pool.go:59` | compile | pool routing | immutable | single-writer |
| `gc.session` | string? (appeared in aggregated key scan but actual usage not located in non-test .go files — see `gc.session` in scan) | unclear from source; search `gc.session` in ralph/runtime — **unclear**; needs targeted read | n/a | unclear | unclear |
| `gc.work_dir` | filesystem path | aggregated key scan only; **unclear from source**; read these: search `gc.work_dir` in ralph/runtime | n/a | unclear | unclear |
| `gc.city_path` | filesystem path (distinct from `convergence.city_path`) | aggregated scan | check execution | unclear | unclear |
| `gc.endpoint_origin`, `gc.endpoint_status` | appeared in key scan; **unclear from source**; read `fanout.go` / `runtime.go` for intent | n/a | unclear | unclear |

### 2b. Routing (gc.routed_to, gc.execution_routed_to, gc.run_target)

Which agent/role/queue should claim this bead.

| Key | Type | Writers | Readers | Lifecycle | cc |
|---|---|---|---|---|---|
| `gc.routed_to` | string — agent qualified name (e.g. `"mayor"`, `"worker"`, `"test-agent"`) | `graphroute.go:143` (primary), `molecule.go:851` (defer), `graph_apply.go:238` (defer), `molecule_test.go:382,421` | `sling_attachment.go:337,349,367,378,390`, `molecule.go` | LWW per routing decision; **`Derived`-friendly** — computed by graphroute from `gc.run_target` | controller-only (single router per iteration); but defer/undefer operates concurrently with compiles so `lww-ish` in edge cases |
| `gc.execution_routed_to` | string — execution agent (`graphroute.go:19`: const `GraphExecutionRouteMetaKey`) | `graphroute.go:156`, defer path at `graph_apply.go:239`, `molecule.go:852` | `graphroute.go:172` | LWW | controller-only |
| `gc.run_target` | string — compile-time target declaration (substituted by `formula.Substitute`) | compile, `graphroute.go:427`, `graphroute_test.go:230` | `graphroute.go:133` | immutable | single-writer |
| `gc.deferred_assignee` | string (shadowed assignee during speculative create) — `molecule.go:58` | `molecule.go:848`, `graph_apply.go:234` | `molecule.go` undefer logic | LWW, cleared on undefer | controller-only |
| `gc.deferred_routed_to` | string — shadowed `gc.routed_to` | const at `molecule.go:62`; writes via `deferBeadMetadataValue` | undefer | LWW | controller-only |
| `gc.deferred_execution_routed_to` | string — shadowed `gc.execution_routed_to` | const at `molecule.go:66`; same pattern | undefer | LWW | controller-only |
| `gc.deferred_type` | string — shadowed bead type | const at `molecule.go:71`, written at `molecule.go:843`, `graph_apply.go:234` | undefer | LWW | controller-only |

### 2c. Retry / ralph control bead state

Per-control-bead mutable state that orchestrates retry-run / retry-eval / ralph-check flows. These are the single biggest concurrency hazards because a control loop may iterate many times on the same bead.

| Key | Type | Writers | Readers | Lifecycle | cc |
|---|---|---|---|---|---|
| `gc.attempt` | int (decimal) — step attempt number | `retry.go:12,293`, `control.go:45,145`, formula compile (`formula/compile_test.go:896`) | every retry consumer | immutable per attempt bead; but control bead reads the latest attempt's value | single-writer per attempt bead; read-only on the control bead (so LWW merge of two "latest attempt seen" views is fine provided both see `max`) |
| `gc.max_attempts` | int | compile | `dispatch/control.go:23,128`, `dispatch/retry.go:16`, `dispatch/ralph.go:30` | immutable | single-writer |
| `gc.attempt_log` | **JSON blob** — array of `{attempt, outcome, reason}` records. Written by `dispatch/control.go:935-973` (`appendAttemptLog` reads current value, parses, appends, writes back). | `dispatch/control.go:973` | `dispatch/control.go:942`, tests | **append-only (semantically)**; currently implemented as read-modify-write of a JSON blob at the string-map layer, which races hard if two controllers ever append concurrently | `or-set` / append-only — LWW at the string layer drops entries |
| `gc.retry_state` | enum `{"", spawning, spawned}` | `dispatch/retry.go:137,174,139,93,112` | `dispatch/retry.go:170`, `ralph.go` | LWW state machine step | controller-only |
| `gc.next_attempt` | int (the number of the attempt being spawned) | `dispatch/retry.go:141,176` | `dispatch/retry.go` | LWW | controller-only |
| `gc.retry_session_recycled` | bool-as-string (`"true"`/`""`) | `dispatch/retry.go:164`, `dispatch/retry.go:157` | `dispatch/retry.go:157` | clearable on new retry | controller-only |
| `gc.outcome` | enum `{pass,fail,skipped,"" (pending)}` — subject-bead result | `dispatch/retry.go:266,269,272`, `dispatch/runtime.go:425` (`"skipped"`), `dispatch/runtime.go:712` (arbitrary outcome), `dispatch/control.go:189`, `dispatch/retry.go:275` (batch), `dispatch/fanout.go:68`, many readers | every outcome-gated consumer | LWW per attempt | single-writer per bead; but a retry-eval may see older subject snapshot when propagating — the batch write at `retry.go:260` is the canonical setter |
| `gc.failure_class` | enum `{"", hard, transient}` | `dispatch/retry.go:267,270,273,82,101,118,225,237`, `dispatch/control.go:75,91` | retry decision | LWW | single-writer per attempt |
| `gc.failure_reason` | free-form string (from worker stderr / check reason) | `dispatch/retry.go:82,102,119,184,253,262`, `dispatch/control.go:76,92,226` | logs, UI | LWW | single-writer per attempt |
| `gc.failed_attempt` | int | `dispatch/retry.go:80,100,116,117`, `dispatch/control.go:74,91,190,223` | UI | LWW | controller-only |
| `gc.closed_by_attempt` | int — which attempt closed the logical bead | `dispatch/retry.go:33,64,79,99,115` | `dispatch/retry.go:33` | append-only / max; LWW accepted at bd layer | controller-only |
| `gc.final_disposition` | enum `{pass, hard_fail, soft_fail, controller_error}` | `dispatch/retry.go:65,83,103,120`, `dispatch/control.go:77,110,210,227` | close-time recording | LWW at close | controller-only |
| `gc.last_failure_class` | enum (same shape as `failure_class`) | `dispatch/retry.go:184` | UI / cross-attempt history | LWW | controller-only |
| `gc.retry_count` | int — attempts seen on the logical bead | `dispatch/retry.go:183` | summary | monotonic → append-only | controller-only |
| `gc.retry_from` | bead id (which run this retry-run copies from)? Appeared in aggregated scan; **unclear from source** — re-grep `gc.retry_from` to confirm | n/a | retry attempt bookkeeping | unclear | unclear |
| `gc.controller_error` | free-form string (controller stacktrace / internal error) | `dispatch/control.go:109,208` | close-time recording | LWW | controller-only |
| `gc.partial_retry` | bool-as-string | `dispatch/ralph.go:952` (consumer); written in compile path | controller | unclear if ever written post-create (probably not) | single-writer |
| `gc.check_mode` | enum `{exec,...}` (only `"exec"` observed at `dispatch/ralph.go:22`, `molecule_test.go:1595`) | compile | `dispatch/ralph.go:22` | immutable | single-writer |
| `gc.check_path` | filesystem path (check script) | compile, `molecule_test.go:1598`, `dispatch/ralph.go:138` | `dispatch/ralph.go:138` | immutable | single-writer |
| `gc.step_timeout` | Go duration string | compile (`molecule.go:817`, `formula/compile_test.go:379`) | `dispatch/ralph.go:172` | immutable | single-writer |
| `gc.check_timeout` | Go duration string | same as above (`molecule.go:817`) | `dispatch/ralph.go:179` | immutable | single-writer |
| `gc.terminal` | bool-as-string | `dispatch/ralph.go:19` (read at consumer side) | `dispatch/ralph.go:19`, close path | unclear origin — **needs read** | unclear |
| `gc.ralph_step_id` | string — logical step id for ralph chain, `molecule.go:954`, `dispatch/runtime.go:686` | compile | ralph consumer | immutable | single-writer |
| `gc.spec_for` | string — what step this spec documents (`molecule_test.go:1607`) | compile | `molecule.go` | immutable | single-writer |
| `gc.spec_for_ref` | step ref that the spec targets (`molecule_test.go:1610`) | compile | `molecule.go` | immutable | single-writer |
| `gc.source_step_spec` | **JSON blob** — full serialized step spec used during compile. `dispatch/control_integration_test.go:41`. Used for re-compile fidelity. | compile-time | compile re-expand / retry | immutable | single-writer |

### 2d. Ralph attempt output / worker-result contract

One worker run appends its result to its own attempt bead. `gc.output_json` is the contract between worker and check script.

| Key | Type | Writers | Readers | Lifecycle | cc |
|---|---|---|---|---|---|
| `gc.output_json` | **JSON blob** — the worker result payload (e.g. `{"verdict":"approved"}`); `dispatch/control_integration_test.go:147` | worker (via `bd update --set-metadata`), then propagation: `dispatch/retry.go:55-56`, `dispatch/control.go:55-56,172-173`, `dispatch/fanout.go:479`, `dispatch/ralph.go:63`, `dispatch/runtime.go:166,245,358-375,555` | check scripts, `fanout` | LWW per attempt; propagated from attempt → logical → control | single-writer per attempt bead, but the propagation steps are `lww-ish` in theory (two concurrent reconciles could both copy) |
| `gc.output_json_required` | bool-as-string — compile-time contract declaration | compile | worker-result validator | immutable | single-writer |
| `gc.stdout` | opaque blob (truncated) | `dispatch/ralph.go:210`, `dispatch/runtime.go` check eval | UI | LWW per attempt | single-writer |
| `gc.stderr` | opaque blob (truncated) | `dispatch/ralph.go:211` | UI | LWW per attempt | single-writer |
| `gc.duration_ms` | int ms | `dispatch/ralph.go:212` | UI | LWW | single-writer |
| `gc.truncated` | bool-as-string | `dispatch/ralph.go:213` | UI | LWW | single-writer |
| `gc.exit_code` | int (may be empty) | `dispatch/ralph.go:216,218` | retry classify | LWW | single-writer |

`clearRetryEphemera` at `dispatch/ralph.go:852-883` explicitly enumerates the set of gc keys that must be wiped when preparing a retry-run clone: `gc.outcome`, `gc.exit_code`, `gc.stdout`, `gc.stderr`, `gc.output_json`, `gc.duration_ms`, `gc.truncated`, `gc.terminal`, `gc.failed_attempt`, `gc.fanout_state`, `gc.spawned_count`, `gc.retry_state`, `gc.next_attempt`, `gc.partial_retry`, `gc.failure_class`, `gc.failure_reason`, `gc.final_disposition`, `gc.closed_by_attempt`, `gc.last_failure_class`, `gc.retry_session_recycled`, plus three non-gc verdict keys: `review.verdict`, `design_review.verdict`, `code_review.verdict`. This set is the de-facto definition of "per-attempt ephemera that must not leak across a retry." Any typed re-home must preserve this clear set as an explicit operation.

### 2e. Fanout (dispatch/fanout.go)

One control bead fans out into N child beads, each with item-specific vars.

| Key | Type | Writers | Readers | Lifecycle | cc |
|---|---|---|---|---|---|
| `gc.fanout_state` | enum `{"" (initial), spawning, spawned}` | `dispatch/fanout.go:114,171`, cleared in `clearRetryEphemera` | `dispatch/fanout.go:22,48,113` | LWW state machine | controller-only |
| `gc.fanout_mode` | enum (`""` default; shape unclear, read-only at `dispatch/fanout.go:109`) | compile | `dispatch/fanout.go:109` | immutable | single-writer |
| `gc.for_each` | path/expression pointing at source for items (JSON path into `source.Metadata["gc.output_json"]`) | compile (`dispatch/fanout.go:83`) | `resolveFanoutItems` | immutable | single-writer |
| `gc.bond` | string (formula fragment name) | compile (`dispatch/fanout.go:129`) | `formula.CompileExpansionFragment` | immutable | single-writer |
| `gc.bond_vars` | **JSON blob** (declaration of which item fields bind to which vars) | compile (`dispatch/fanout.go:105`) | `parseFanoutVars` | immutable | single-writer |
| `gc.control_for` | step ref of the control's "source" (the workflow step whose output drives fanout) | compile (`dispatch/fanout.go:60`) | `dispatch/fanout.go:60` | immutable | single-writer |
| `gc.spawned_count` | int | `dispatch/fanout.go:173` | UI / reconcile | monotonic → append-only | controller-only |
| `gc.partial_fragment` | bool-as-string — marks beads from an incomplete fragment spawn | `dispatch/fanout.go:193,396` | `dispatch/fanout.go:193`, cleanup | LWW during cleanup | controller-only |
| `gc.dynamic_fragment` | bool-as-string (members of a dynamically generated fragment; treated as off-graph during retry scope) | compile | `dispatch/ralph.go:603,648`, `dispatch/runtime.go` | immutable | single-writer |

---

## 3. `convoy.*` — convoy metadata

Structured fields on a convoy bead. Declared at `internal/convoy/convoy_fields.go:12-33` as a `ConvoyFields` struct with a `convoyFieldKeys` mapping table. Already typed on the gascity side — the string-map form is just the wire representation.

| Key | Type | Writers | Readers | Lifecycle | cc |
|---|---|---|---|---|---|
| `convoy.owner` | string — managing identity | `convoy_fields.go:26,46,58` | `convoy_fields.go:73`, `api/convoy_event_stream.go`, `convoy/convoy_fields_test.go` | LWW | single-writer |
| `convoy.notify` | enum `{human, ...}` (notification target) | same file | same | LWW | single-writer |
| `convoy.molecule` | bead id — associated molecule | same file | same | LWW | single-writer |
| `convoy.merge` | enum `{direct, mr, local}` — merge strategy | same file | same | LWW | single-writer |
| `target` | string — unprefixed by design (`convoy_fields.go:30-32`: "work beads read their own value directly, while still inheriting it from convoy ancestors during sling") — branch name | `convoy_fields.go:32`, `sling/sling.go:504` | `sling.go:504` | LWW | single-writer |
| (`workflow_id`, `molecule_id` — see §7 for the unprefixed "attached root" fields that also appear on convoy children) | | | | | |

Event names `convoy.create|created|add|remove|closed` in `convoy_event_stream.go` are envelope labels, not metadata.

---

## 4. Session lifecycle (unprefixed; owned by `internal/session/`)

This is the second-biggest namespace and it is **entirely unprefixed**, which is its own flag — any port must decide whether these keys live on a `Session` bead type or get a `session.*` prefix.

Primary source of truth for the full set of keys: `internal/session/lifecycle_transition.go` (every `MetadataPatch` function exhaustively enumerates keys it touches). Constants: `internal/session/named_config.go:11-17`, `internal/session/alias.go:5`, `internal/session/submit.go:27`.

Writers are scattered across `internal/session/{manager,chat,submit,waits,names,alias,named_config,lifecycle_transition}.go`. Readers are everywhere in `internal/session/` plus `internal/worker/`, `internal/agentutil/pool.go`, `internal/api/huma_handlers_sessions_command.go`, `internal/api/handler_mail.go`.

### 4a. Identity / configuration (set at session create)

| Key | Type | Writers | Readers | Lifecycle | cc |
|---|---|---|---|---|---|
| `template` | string (agent template qualified name) | `manager.go:287` | `manager.go:1113,1157`, `agentutil/pool.go:59`, `names.go:374` | immutable | single-writer |
| `provider` | string (raw provider; normalized against `provider_kind`) | `manager.go:289` | `manager.go:153,162,1143,1162`, `chat.go:187,198,200,215,296,351-353`, `session/chat.go:801` | LWW (set at create; may be normalized later) | single-writer |
| `provider_kind` | string (canonical builtin family — injected via `extraMeta`) | `session_resolution.go:306-307` | `chat.go:351`, `session/resolve.go` | immutable after resolution | single-writer |
| `builtin_ancestor` | string (resolved canonical ancestor for a custom provider alias) | `session_resolution.go:308-309` | session resolution | immutable | single-writer |
| `transport` | enum `{tmux, acp, ...}` (normalized) | `manager.go:175,305`, `chat.go:200,295` | many | LWW | controller-only |
| `work_dir` | filesystem path | `manager.go:290` | `manager.go:1164`, `chat.go:215,320,796`, `chat.go:830` | immutable | single-writer |
| `command` | string (provider launch command) | `manager.go:291` | `manager.go:1163` | immutable | single-writer |
| `resume_flag`, `resume_style`, `resume_command` | strings (provider resume configuration) — `ProviderResume` struct | `manager.go:292-294` | `manager.go:1167-1169`, `chat.go:76` | immutable | single-writer |
| `session_name` | string — session runtime name (tmux session / ACP handle) | `manager.go:337,345,356,362,566,574,1136`, `named_config.go:208,259,264,296`, `names.go:331,343,493,541,547` | **huge** readership | LWW; may be rewritten by `names.go` assignment flow | **`lww-ish`**: multiple replicas can attempt to reserve the same name; `EnsureSessionNameAvailable` (at `session_resolution.go:317`) does pre-check but not CAS |
| `session_name_explicit` | bool-as-string | `manager.go:312,347,359,363` | `named_config.go` | LWW | single-writer |
| `alias` | string (human alias) | `manager.go:302`, `named_config.go`, `RetireNamedSessionPatch` | `manager.go:914,1161`, `named_config.go:211,243`, `names.go:343,500,544` | LWW | lww-ish per reservation |
| `alias_history` | CSV list (append-only) — `alias.go:5` declares `aliasHistoryMetadataKey` | `alias.go:27` | `alias.go:13` | **append-only CSV**; a LWW string merge drops entries | **`or-set`** — this is the clearest OR-set candidate in the whole inventory |
| `common_name` | string (fallback name, uncommon) | `names.go:375` read-only — **unclear writer** | `names.go:375` | unclear | unclear |
| `agent_name` | string | `names.go:373`, `manager.go:383`, `named_config.go:182` | multiple | LWW | single-writer |
| `session_origin` | enum `{manual, named, worker, ephemeral}` | `manager.go:317-319`, `session_resolution.go:303`, `worker/handle_construct.go:31` | `chat.go:241,346`, tests | LWW at create | single-writer |
| `session_key` | opaque string (provider's session id, used in resume commands) | `manager.go:308`, `manager.go:1200`, `chat.go:46,228,333`, `AcknowledgeDrainPatch`, `RestartRequestPatch`, `ConfigDriftResetPatch` | `chat.go:73,76,225,252,275,275+`, `resolve.go` | LWW; cleared on drain/restart/config-drift | controller-only |
| `instance_token` | opaque string (per-process nonce) | `manager.go:297`, `chat.go:228,333` | `chat.go:225,330`, tests | LWW per runtime start | controller-only |
| `generation` | int (incrementing on each runtime start) | `manager.go:295` (init to `DefaultGeneration`); incremented at `chat.go:217` read then write | `chat.go:217,322` | monotonic → append-only | controller-only |
| `continuation_epoch` | int | `manager.go:296` (init); read-mutate at `chat.go:221,326` | same | monotonic | controller-only |
| `session_key` (second appearance in `manager_test.go:2863` via `failMetadataKeyStore`) | — | — | — | — | — |

### 4b. State machine (written by `MetadataPatch` transitions)

Each `MetadataPatch` function at `lifecycle_transition.go` is a named atomic update.

| Key | Type | Writers | Readers | Lifecycle | cc |
|---|---|---|---|---|---|
| `state` | enum `BaseState` — `{"", creating, active, asleep, suspended, draining, drained, archived, orphaned, closed, closing, quarantined, stopped}` — defined at `lifecycle_projection.go:13-40` | every `MetadataPatch` | every reader | LWW state machine | controller-only (controller drives; a "reactivate" from another controller racing a "quarantine" is the only real contention, and that is the documented reason for `ClearWakeBlockersPatch`) |
| `state_reason` | string (free-form diagnostic — `"creation_complete"`, `"config-drift"`, `"crash-loop"`, `"reactivated"`, `"drain_complete"`, etc.) | every state-changing patch | UI / logs | LWW paired with `state` | controller-only |
| `pending_create_claim` | bool-as-string — advisory one-shot claim | `RequestWakePatch`, `ConfirmStartedPatch`, `SleepPatch`, `ArchivePatch`, `RestartRequestPatch`, `ConfigDriftResetPatch`, `ReactivatePatch`, `manager.go:738` | `chat.go:403`, `worker/handle_test.go:53` | **clearable boolean flag**; semantically a one-shot lease. LWW merge of two "claim" writes collapses to "one agent thinks it owns this" — **actual risk** if two controllers try to wake the same asleep bead | **`lww-ish` with real risk** — flagged in the lifecycle_transition doc comment as "one-shot claim" |
| `held_until` | RFC3339 timestamp (empty means no hold) | `RequestWakePatch`, `ClearWakeBlockersPatch`, `ClearExpiredHoldPatch`, `RetireNamedSessionPatch` | wake scheduler, `ClearExpiredHoldPatch` | LWW, clearable | controller-only |
| `quarantined_until` | RFC3339 timestamp | `QuarantinePatch`, `RequestWakePatch`, `ClearExpiredQuarantinePatch`, `ReactivatePatch`, `RetireNamedSessionPatch` | wake scheduler | LWW, clearable | controller-only |
| `quarantine_cycle` | int | `QuarantinePatch` | UI / backoff policy | monotonic per session | controller-only |
| `sleep_reason` | enum `{user-hold, wait-hold, quarantine, context-churn, drained, ...}` | `SleepPatch`, `CompleteDrainPatch`, `ClearWakeBlockersPatch`, `ClearExpiredHoldPatch`, `ClearExpiredQuarantinePatch`, `CommitStartedPatch`, `ConfirmStartedPatch`, `RetireNamedSessionPatch` | `ClearWakeBlockersPatch`, `ClearExpiredQuarantinePatch`, tests | LWW | controller-only |
| `sleep_intent` | string (planned sleep reason; cleared on sleep) | `RequestWakePatch`, `SleepPatch`, `ClearWakeBlockersPatch`, `RetireNamedSessionPatch` | scheduler | LWW | controller-only |
| `wait_hold` | bool-as-string / string | `RequestWakePatch`, `ClearWakeBlockersPatch`, `RetireNamedSessionPatch` | scheduler | LWW | controller-only |
| `wake_attempts` | int | `RequestWakePatch` (reset to 0), `ClearWakeBlockersPatch` (reset), `ClearExpiredQuarantinePatch` (reset) | backoff policy | **monotonic between resets**; a reset races with an increment → `lww-ish` but correctness-preserving (increment wins, scheduler retries) | controller-only |
| `churn_count` | int | `RequestWakePatch` (reset), `ClearWakeBlockersPatch` (reset) | context-churn detector | same as wake_attempts | controller-only |
| `crash_count` | int | `ReactivatePatch` (reset to 0) | crash-loop detector | monotonic between resets | controller-only |
| `drain_at` | RFC3339 timestamp | `BeginDrainPatch` | drain watchdog | set-once per drain | controller-only |
| `archived_at` | RFC3339 timestamp | `ArchivePatch`, cleared by `ReactivatePatch` | archival query | LWW | controller-only |
| `slept_at` | RFC3339 timestamp | `SleepPatch` | analytics | set-once per sleep | controller-only |
| `last_woke_at` | RFC3339 timestamp (cleared by `SleepPatch`, `AcknowledgeDrainPatch`, `CompleteDrainPatch`, `RestartRequestPatch`, `ConfigDriftResetPatch`, `QuarantinePatch`; set elsewhere — **unclear source of write**) | cleared many places; set in `manager.go`/`chat.go` (**grep returned only clears; need targeted read**) | scheduler freshness | LWW, clearable | controller-only |
| `creation_complete_at` | RFC3339 timestamp | `ConfirmStartedPatch`, `CommitStartedPatch` | sweep guard at `lifecycle_transition.go:109-113` | LWW | controller-only |
| `suspended_at` | RFC3339 timestamp | `manager.go:654` | lifecycle projection | set-once per suspend | controller-only |
| `closed_at` | RFC3339 timestamp | `ClosePatch`, `extmsg/transcript_service.go:427` (reused on membership beads) | close-time projection | set-once | single-writer |
| `close_reason` | enum (mirrors `state` at close time) | `ClosePatch` | same | set-once | single-writer |
| `synced_at` | RFC3339 timestamp | `ClosePatch`, `RetireNamedSessionPatch` | lifecycle projection | LWW | controller-only |
| `continuity_eligible` | bool-as-string | `ArchivePatch`, `ReactivatePatch`, `named_config.go:189` | `lifecycle_projection.go:317`-ish / `named_config.go` | LWW | controller-only |
| `continuation_reset_pending` | bool-as-string | `AcknowledgeDrainPatch`, `CompleteDrainPatch`, `RestartRequestPatch`, `ConfigDriftResetPatch`, `chat.go:52,60` | startup dialog logic | clearable | controller-only |
| `restart_requested` | bool-as-string | `RestartRequestPatch`, `ConfigDriftResetPatch` | runtime restart path | clearable | controller-only |
| `started_config_hash` | opaque hash string | `CommitStartedPatch`, cleared by `AcknowledgeDrainPatch`/`CompleteDrainPatch`/`RestartRequestPatch`/`ConfigDriftResetPatch`, `chat.go:49,59` | config-drift check | LWW per start | controller-only |
| `started_live_hash` | opaque hash string | `CommitStartedPatch`, cleared by `ConfigDriftResetPatch` | drift check | LWW | controller-only |
| `live_hash` | opaque hash string | `CommitStartedPatch`, cleared by `ConfigDriftResetPatch` | drift check | LWW | controller-only |
| `core_hash_breakdown` | string (optional debug field) | `CommitStartedPatch` | drift debug | LWW | controller-only |
| `retired_named_identity` | string — which identity was retired | `RetireNamedSessionPatch` | history | set-once | controller-only |
| `startup_dialog_verified` | bool-as-string — const `startupDialogVerifiedKey` at `submit.go:27` | `chat.go:558,564`, `submit_test.go` | `submit.go:405` | clearable | controller-only |
| `configured_named_session` | bool-as-string — const at `named_config.go:13` | `named_config.go:243` (inside `Metadata: map[string]string{...}` literal; see `session_resolution.go:300` for the write site) | `named_config.go:163`, `names.go:335,365,395,407`, `manager.go:730,922`, `api/huma_handlers_sessions_command.go:571` | immutable at create | single-writer |
| `configured_named_identity` | string — const at `named_config.go:15` (`NamedSessionIdentityMetadata`); aliased as `apiNamedSessionIdentityKey` at `api/session_resolution.go:22` | `named_config.go:244`, `session_resolution.go:301` | same places + `lifecycle_projection.go:317`, `api/handler_mail.go:225` | immutable | single-writer |
| `configured_named_mode` | string — const at `named_config.go:17` (`NamedSessionModeMetadata`) | `session_resolution.go:302` | `named_config.go:173` | immutable | single-writer |

### 4c. Worker profile (on session beads)

Written by worker factory when a session is attached to a concrete worker profile.

| Key | Type | Writers | Readers | Lifecycle | cc |
|---|---|---|---|---|---|
| `worker_profile` | string (e.g. `ProfileClaudeTmuxCLI`) | `worker/factory_test.go:142`, `worker/handle_construct.go:35` | `worker/factory.go:119` | LWW | controller-only |
| `worker_profile_provider_family` | string | `worker/handle_test.go:56` (observed; writer likely `handle_construct.go`) | profile-driven dispatch | immutable per start | single-writer |
| `worker_profile_transport_class` | string | `worker/handle_test.go:59` | same | immutable per start | single-writer |
| `worker_profile_compatibility_version` | string | `worker/handle_test.go:62` | same | immutable per start | single-writer |
| `worker_profile_certification_fingerprint` | string | `worker/handle_test.go:65` | certification check | immutable per start | single-writer |
| `mc_session_kind` | enum `{provider,...}` | `worker/factory_test.go:139`, `worker/factory.go:118` | factory | LWW | controller-only |

### 4d. Nudge / wait beads

Wait beads are created by the scheduler to gate session wake on external signals.

| Key | Type | Writers | Readers | Lifecycle | cc |
|---|---|---|---|---|---|
| `session_id` | bead id of the session the wait is attached to | `session/waits.go:80`, `nudgequeue/waits.go` creation | `session/waits.go:104,141,215` | immutable | single-writer |
| `nudge_id` | bead id | `session/waits.go:107` creation | `session/waits.go:107` | immutable | single-writer |
| `state` (on wait bead; distinct from session `state`) | enum — terminal states enumerated by `IsWaitTerminalState` | `session/waits.go:144,218,221`, `nudgequeue/waits.go:93` (`"failed"`) | same | LWW | controller-only |
| `terminal_reason` (wait) | enum `{wait-canceled, ...}` | `nudgequeue/waits.go:94` | audit | set at terminal | controller-only |
| `commit_boundary` | string (`"delivery-withdrawn"`) | `nudgequeue/waits.go:95` | audit | set at terminal | controller-only |
| `terminal_at` | RFC3339 timestamp | `nudgequeue/waits.go:96` | audit | set at terminal | controller-only |

---

## 5. `extmsg` — external message transcript / membership / binding / group / participant / delivery

All field sets are routed through `encodeMetadataFields` at `internal/extmsg/helpers.go:111` which merges user-supplied metadata with a per-service core field set. Every record type has its own field set.

### 5a. Transcript entry (per message)

Writer: `transcript_service.go:89-107`. Readers: `transcript_service.go:887-954`.

| Key | Type | Lifecycle | cc |
|---|---|---|---|
| `schema_version` | int (decimal) | immutable per bead | single-writer |
| `scope_id` | string | immutable | single-writer |
| `provider` | string | immutable | single-writer |
| `account_id` | string | immutable | single-writer |
| `conversation_id` | string | immutable | single-writer |
| `parent_conversation_id` | string | immutable | single-writer |
| `conversation_kind` | enum | immutable | single-writer |
| `sequence` | int (monotonic per conversation — written from `state.NextSequence`) | immutable per bead | single-writer |
| `kind` | enum `TranscriptMessageKind` | immutable | single-writer |
| `provenance` | enum `TranscriptProvenance` | immutable | single-writer |
| `provider_message_id` | string (may be empty) | immutable | single-writer |
| `explicit_target` | normalized handle | immutable | single-writer |
| `reply_to_message_id` | string | immutable | single-writer |
| `source_session_id` | bead id | immutable | single-writer |
| `created_at` | RFC3339 timestamp | immutable | single-writer |
| `actor_json` | **JSON blob** (optional `Actor`) | immutable | single-writer |
| `attachments_json` | **JSON blob** (optional `Attachments`) | immutable | single-writer |

### 5b. Transcript state (per conversation)

Writer: `transcript_service.go:127-135` (`next_sequence`, `earliest_available_sequence`), `transcript_service.go:618-696` (`hydration_status`).

| Key | Type | Lifecycle | cc |
|---|---|---|---|
| `next_sequence` | int (monotonic, advances on every append) | monotonic | controller-only (but multi-replica append under same conversation would race — current code serializes with `withBindingLock`) |
| `earliest_available_sequence` | int | monotonic / LWW | controller-only |
| `hydration_status` | enum `HydrationStatus` (`HydrationPending`, `HydrationComplete`, `HydrationFailed`) | LWW state machine | controller-only |
| `oldest_hydrated_message_id` | bead id | LWW | controller-only |

### 5c. Membership (per {conversation, session})

Writer: `transcript_service.go:244-372`.

| Key | Type | Lifecycle | cc |
|---|---|---|---|
| `schema_version` / `scope_id` / `provider` / `account_id` / `conversation_id` / `parent_conversation_id` / `conversation_kind` | (same semantics as transcript) | immutable | single-writer |
| `session_id` | bead id | immutable | single-writer |
| `joined_at` | RFC3339 timestamp | immutable | single-writer |
| `joined_sequence` | int | immutable | single-writer |
| `last_read_sequence` | int | monotonic per session | `append-only` — LWW of two acks drops the lower one which is correct, so LWW is fine *provided* merge takes max; bd's LWW-by-stamp may pick older value if clock skew → **risk** |
| `membership_backfill_policy` | enum `MembershipBackfillPolicy` | LWW | controller-only |
| `manual_backfill_policy` | enum (may be empty) | LWW, clearable | controller-only |
| `membership_owner_kinds` | **CSV / encoded set** of `MembershipOwner` — written via `encodeMembershipOwners`, read via `decodeMembershipOwners`. This is semantically an OR-set (owners accumulate) | read-modify-write cycle; concurrent add-then-remove via LWW on the encoded string will lose operations | **`or-set`** |

### 5d. Binding (per {conversation, session})

Writer: `binding_service.go:142-157`.

| Key | Type | Lifecycle | cc |
|---|---|---|---|
| Common schema fields (as above) | | | |
| `binding_generation` | int (monotonic per session, written once per rebind) | monotonic | single-writer per (session, rebind) |
| `bound_at` | RFC3339 timestamp | immutable | single-writer |
| `expires_at` | RFC3339 timestamp (may be empty / null) | LWW | controller-only |
| `last_touched_at` | RFC3339 timestamp | LWW | `append-only` (max wins) — `binding_service.go:271,353,384` is a read-modify-write of current time; LWW-by-bd-stamp is close to correct |
| `created_by_kind` | enum `Caller.Kind` | immutable | single-writer |
| `created_by_id` | string | immutable | single-writer |

### 5e. Group (per root conversation)

Writer: `group_service.go:59-74`.

| Key | Type | Lifecycle | cc |
|---|---|---|---|
| Common schema fields | | | |
| `mode` | enum `GroupMode` (`GroupModeLauncher`) | LWW | controller-only |
| `default_handle` | normalized handle | LWW | single-writer |
| `last_addressed_handle` | normalized handle (clearable; `group_service.go:457,473`) | LWW | controller-only |
| `fanout_enabled` | bool-as-string | LWW | controller-only |
| `fanout_allow_untargeted` | bool-as-string | LWW | controller-only |
| `fanout_max_peer_triggered_publishes` | int | LWW | controller-only |
| `fanout_max_total_peer_deliveries` | int | LWW | controller-only |

### 5f. Group participant

Writer: `group_service.go:150-156`, update at `:186`.

| Key | Type | Lifecycle | cc |
|---|---|---|---|
| `schema_version` | int | immutable | single-writer |
| `group_id` | bead id | immutable | single-writer |
| `handle` | normalized handle | immutable per record | single-writer |
| `session_id` | bead id — may change on reassignment | LWW | controller-only |
| `public` | bool-as-string | LWW | controller-only |
| `previous_session_id_pending_cleanup` | CSV of bead ids (encoded via `encodePendingCleanupSessionIDs`) — `group_service.go:186,575`, `extmsg_test.go:1792+` | **accumulating CSV → OR-set** | `or-set` |

### 5g. Delivery context

Writer: `delivery_service.go:47-60`.

| Key | Type | Lifecycle | cc |
|---|---|---|---|
| Common schema fields | | | |
| `session_id` | bead id | immutable | single-writer |
| `binding_generation` | int | immutable (per delivery-context record; tied to a binding) | single-writer |
| `last_published_at` | RFC3339 timestamp | LWW (max) | controller-only |
| `last_message_id` | bead id / provider id | LWW | controller-only |
| `source_session_id` | bead id | immutable | single-writer |

### 5h. Misc extmsg-adjacent

| Key | Type | Writers | Readers | Lifecycle | cc |
|---|---|---|---|---|---|
| `route` | string (e.g. `"thread-reply"`) — set on outgoing mail beads | `api/handler_mail.go` (not directly seen; observed in test at `extmsg_test.go:290`) | mail router | immutable | single-writer |
| `source` | string (external source; e.g. `"discord"`, `"startup"`, `"phase2"`) | `extmsg_test.go:26`, `worker/fake/spec_test.go:355,482` | attribution | immutable | single-writer |
| `step` | string (`"approval"`) | `worker/fake/spec_test.go:355` | worker phase | immutable | single-writer |

---

## 6. Molecule-as-consumer keys (unprefixed, non-gc)

Appear on molecule-attached beads to link them back to a workflow root / molecule root. These shadow the owner-owned `gc.workflow_id` and `gc.root_bead_id` but live unprefixed because they are **consumer-owned pointers on an ordinary bead** (usually a parent work-bead that happens to have a molecule attached).

| Key | Type | Writers | Readers | Lifecycle | cc |
|---|---|---|---|---|---|
| `molecule_id` | bead id (of the molecule root) | `sling/sling_core.go:171,229,661`, `sling/sling_attachment.go:162` (clear) | `sling/sling_attachment.go:56,162`, `api/handler_beads.go:25` | LWW per attach | controller-only |
| `workflow_id` | bead id (of the workflow root) | `sling/sling_core.go:360,523`, `sling/sling_attachment.go:157` (clear) | `sling/sling_attachment.go:57`, `api/handler_beads.go:25` | LWW per attach | controller-only |
| `merge_strategy` | enum (`direct`, `mr`, `local` — same shape as `convoy.merge`) | `sling/sling_core.go:289` | sling merge path | LWW | single-writer |
| `notes` | substituted string (from step template `step.Notes`) | `molecule.go:809` | UI / debugging | immutable | single-writer |
| `from` | actor id (who created the bead; surfaced if not on the bead struct directly) | `beads/bdstore.go:340,416` (gascity uses `from` as a metadata fallback when the `from` struct field is empty) | `beads/bdstore.go:340` | immutable | single-writer |
| `molecule_failed` | bool-as-string — marker that the molecule run failed | `molecule.go:918` | `molecule_test.go:1401`, reconcile | LWW | controller-only |

---

## 7. One-off / fixture / orphan keys

Keys that appeared in the scan but live in test fixtures, demo workflows, or otherwise are not store state. Listed here for completeness so the remapping file can dismiss them explicitly rather than missing them silently.

- `demo.*` (`demo.body`, `demo.done`, `demo.loop`, `demo.review`, `demo.survey`, `demo.work`) — pure test fixture step refs in `formula/compile_test.go` and `bootstrap/packs/core/formulas/*.toml`. Not real metadata keys at runtime; they appear inside test data as step IDs, not as bead metadata.
- `wf.body`, `wf.review` — same as above; step IDs in tests.
- `expansion.implement`, `expansion.worktree` — test expansion step IDs.
- `mol.loop`, `mol.review`, `mol.root`, `mol.step` — test molecule step IDs.
- `other.demo.survey` — test step id.
- `output.items` — JSON-pointer-ish expression used inside `gc.for_each`, not a metadata key itself.
- `agent.*`, `mail.*`, `session.*` (when bare event labels), `convoy.*` (when bare event labels), `convergence.*` (when bare event labels), `build.code`, `sling.dispatch` — these are all event-envelope `type` strings in the event stream (`events.go` / `convoy_event_stream.go` / various `_test.go`), not metadata keys.
- `provider_message_id` / `kind` / `provenance` / `source` — see §5 (extmsg core fields).
- `custom` / `test` / `target` / `from` literals inside molecule_test / sling_test fixtures that happen to match real key names — cross-referenced against real writers above.

---

## Concurrency summary

Keys whose current LWW-by-string-map behavior is actually wrong (they need an OR-set, an append-log, or a CAS):

- **OR-set required**: `alias_history`, `membership_owner_kinds`, `previous_session_id_pending_cleanup`.
- **Append-only log required**: `gc.attempt_log` (JSON blob read-modify-write is a known race).
- **Max required** (not LWW by stamp): `last_read_sequence`, `wake_attempts`/`churn_count`/`crash_count` when incremented (current code resets and increments, but two concurrent resets-then-increments lose the reset), `generation`, `continuation_epoch`, `next_sequence`, `gc.retry_count`, `gc.spawned_count`, `gc.closed_by_attempt`.
- **One-shot claim (CAS)**: `pending_create_claim` — the documented one-shot lease. Today its correctness depends on all writes going through a single controller.
- **Immutable-after-create** (a rewrite is a bug, not a merge): every `gc.kind`, `gc.root_bead_id`, `gc.step_ref`, `gc.step_id`, `gc.logical_bead_id`, `gc.idempotency_key`, `schema_version`, all `configured_named_*` keys, `joined_at`, `bound_at`, `binding_generation`.
- **Multi-replica reservation races**: `session_name`, `alias` — `EnsureSessionNameAvailable` is a pre-check, not a CAS. Two replicas can both reserve.

Everything else is LWW-per-write-step and is adequately served by beads-rs's `Lww<T>` semantics — assuming it gets a typed home and a real stamp.

---

## What's **not** in this inventory

Deliberately excluded:

- **Event envelope `type` fields** (`convergence.created`, `convoy.add`, `session.crashed`, `mail.sent`, etc.) — these are the `type` string on an event, not bead metadata. If a future remapping wants to type these, that's a separate exercise (and belongs under `primitives/events.md` or similar).
- **Bd CLI arg parsing** (`--set-metadata`, `--metadata-field`) — these are the wire shape. The keys passed via them are exactly the keys listed above.
- **Bd internal config keys** (`bd config set ...`) — not per-bead metadata; lives in bd's settings table.
- **Formula/molecule TOML fields** (`gc.kind = "scope"` inside a `.toml`) — these are the source from which the metadata keys are populated. The TOML fields are already typed via the TOML parser (see `formula/source_spec.go`); they flow into bead metadata at compile.

If a future pass finds a writer or reader that this inventory misses, the remediation is to add it here and then revisit the corresponding row in `metadata-remapping.md`.
