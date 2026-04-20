# Metadata remapping: gascity keys → beads-rs typed homes

**Pin:** beads v1.0.2 (c446a2ef) · companion to `metadata-inventory.md`. Read the inventory first.

This file is the **design decision log**. For every key (or coherent group of keys) from the inventory, it names a typed home in beads-rs. The decision vocabulary — one of seven options — is fixed:

1. **`TypedField`** — a typed field on a typed `BeadType` variant. Specifies the variant, the Rust type, and the CRDT wrapper (usually `Lww<T>`). Replaces a string metadata key with a compile-time-known field.
2. **`NamespaceProperty`** — belongs to the bead's namespace, not the bead itself. The namespace carries the flag; individual beads inherit it.
3. **`Label`** — fits label semantics (present/absent, no value payload, orthogonal to state). Confirms the key has no value content beyond "set".
4. **`DepEdgeState`** — belongs on a dependency edge, not a bead. Replaces "pointer from bead A to bead B via metadata" with "typed dep kind from A to B".
5. **`RuntimeState`** — does not belong in any persistent store. Lives in gascity's process table, tmux session tree, runtime provider cache, or other ephemeral substrate. Names the Go-side subsystem that would own it.
6. **`Derived`** — can be computed from other beads-rs state; needs no storage. Explains the derivation.
7. **`Revisit`** — no clean typed home was found; a first-class metadata primitive might be warranted after all. Each `Revisit` is evidence against the "no metadata" rule and carries a detailed rationale.

Design principles applied (from `docs/philosophy/type_design.md` and `docs/philosophy/scatter.md`):

- **Types should tell the truth, especially uncomfortable truths.** If gascity has 4 gate outcomes, beads-rs has 4 enum variants. If it has 27 convergence fields, beads-rs has a 27-field struct, not a `HashMap<String, String>`.
- **Information holds its shape.** An OR-set in reality gets an OR-set in types; a LWW scalar gets `Lww<T>`; a monotonic counter gets `Max<u64>` not `Lww<u64>`.
- **One canonical home per fact.** If state is derivable (routing, say) it's `Derived`, not a copy. If it's per-namespace (ephemeral-ness, protected prefix) it's a namespace property, not smeared across every bead.
- **Unrepresentable-wrong-state > validated-on-read.** A bead whose `kind` can only be `Gate` if the variant is `BeadType::Gate` cannot carry gate-only fields illegally.

A note on bead-type proliferation: the inventory has at least eight "kinds" of bead masquerading as `type=task` in gascity today (convergence-root, molecule-step, gate-control, retry-control, ralph-control, fanout-control, session, transcript-entry, membership, binding, group, participant, delivery-context, wait, nudge). Beads-rs's current `BeadType` enum (`Bug, Feature, Task, Epic, Chore`) is too narrow. The remapping below assumes beads-rs grows a richer `BeadType` enum — either directly or through the custom-types mechanism (`docs/go-parity/primitives/custom-types.md`). Where a proposal references a new variant (e.g. `BeadType::ConvergenceRoot`), that's the forcing function for the custom-types port; it's not inventing something out of thin air.

---

## A. TypedField (the majority)

The large `TypedField` section splits by owner subsystem — each subsystem becomes a typed bead variant plus its own mutable field struct. Rust types below assume the shared CRDT wrappers already present in `beads-core` (`Lww<T>`, plus hypothetical `Max<T>`, `OrSet<T>`, `AppendLog<T>`, `Cas<T>` — see note at the end about which already exist and which are new infrastructure).

### A.1 `BeadType::ConvergenceRoot` (replaces §1 of the inventory)

Source keys: `convergence.state`, `iteration`, `max_iterations`, `formula`, `target`, `gate_mode`, `gate_condition`, `gate_timeout`, `gate_timeout_action`, `active_wisp`, `last_processed_wisp`, `agent_verdict`, `agent_verdict_wisp`, `gate_outcome`, `gate_exit_code`, `gate_outcome_wisp`, `gate_retry_count`, `gate_stdout`, `gate_stderr`, `gate_duration_ms`, `gate_truncated`, `terminal_reason`, `terminal_actor`, `waiting_reason`, `retry_source`, `city_path`, `evaluate_prompt`, `pending_next_wisp`, plus the `var.<k>` prefix family.

**Chosen home**: `TypedField` on a new `BeadType::ConvergenceRoot` variant, modeled as three cohesive substructs: an immutable `ConvergenceSpec` set at creation, a mutable `ConvergenceLoopState` updated by the controller, and a per-gate-run `GateResult` nested inside the loop state.

**Rust type sketch** (illustrative; actual layout lands in `beads-core`):

```rust
// Creation-time, immutable; lives in BeadCore-adjacent immutable provenance.
struct ConvergenceSpec {
    formula: FormulaName,          // validated newtype; derived from convergence.formula
    target: String,                // convergence.target — treat as opaque identity
    max_iterations: u32,           // convergence.max_iterations
    gate: GateSpec,                // see below
    city_path: CityPath,           // convergence.city_path — validated newtype
    evaluate_prompt: String,       // convergence.evaluate_prompt
    retry_source: Option<BeadId>,  // convergence.retry_source; Some iff this was a retry-create
    template_vars: BTreeMap<VarName, String>, // var.<k> prefix — frozen at create
}

enum GateSpec {
    Manual,
    Condition { expr: GateExpr, timeout: Duration, on_timeout: TimeoutAction },
    Hybrid    { expr: GateExpr, timeout: Duration, on_timeout: TimeoutAction },
}
enum TimeoutAction { Iterate, Retry, Manual, Terminate } // convergence.gate_timeout_action

// Mutable, one Lww per cohesive unit — not per scalar. Lets the controller
// atomically advance loop state without a 10-field batch.
struct ConvergenceLoopState {
    phase: Lww<ConvergencePhase>,
    active_wisp: Lww<Option<BeadId>>,           // convergence.active_wisp
    last_processed_wisp: Lww<Option<BeadId>>,   // convergence.last_processed_wisp
    pending_next_wisp: Lww<Option<BeadId>>,     // convergence.pending_next_wisp
    iteration: Max<u32>,                        // convergence.iteration — monotonic
    verdict: Lww<Option<AgentVerdict>>,         // convergence.agent_verdict + wisp
    last_gate: Lww<Option<GateResult>>,         // bundles all 8 gate_* keys
    termination: Lww<Option<Termination>>,      // convergence.terminal_* + waiting_*
}

enum ConvergencePhase { Creating, Active, WaitingManual, Terminated }  // convergence.state

struct AgentVerdict {
    verdict: Verdict,            // convergence.agent_verdict
    wisp: BeadId,                // convergence.agent_verdict_wisp
}
enum Verdict { Approve, ApproveWithRisks, Block }   // from NormalizeVerdict at metadata.go:113-127

struct GateResult {
    outcome: GateOutcome,        // convergence.gate_outcome
    exit_code: Option<i32>,      // convergence.gate_exit_code
    retry_count: u32,            // convergence.gate_retry_count
    stdout: TruncatedBlob,       // gate_stdout + gate_truncated
    stderr: TruncatedBlob,       // gate_stderr
    duration: Duration,          // gate_duration_ms
    wisp: BeadId,                // gate_outcome_wisp
}
enum GateOutcome { Pass, Fail, Timeout, Error }  // from metadata.go:78-83

enum Termination {
    Approved       { actor: ActorId },                // convergence.terminal_reason = approved
    NoConvergence  { actor: ActorId },                // = no_convergence
    Stopped        { actor: ActorId },                // = stopped
    PartialCreation,                                  // = partial_creation (automatic)
    WaitingManual  { reason: WaitingReason },         // waiting_reason
}
enum WaitingReason { Manual, HybridNoCondition, Timeout, SlingFailure }  // metadata.go:86-91
```

**Rationale.** Convergence is one primitive with 27 fields and a well-defined state machine. Splitting it across 27 `Lww<String>` fields is the scatter failure mode. Bundling `GateResult` as one `Lww<Option<GateResult>>` means "all eight gate_* keys update atomically" becomes a compiler-enforced contract, not a `SetMetadataBatch` hope. `iteration` being `Max<u32>` instead of `Lww<u32>` kills the "reset-then-increment" race and makes beads-rs's guarantee stronger than Go's. `retry_source` as `Option<BeadId>` proves "retries are reachable from their origin"; turning that into a `discovered_from` dep is **also** correct, but duplication is scatter, so: keep `retry_source` as an immutable field pointing backward, and additionally emit the dep edge as a derivation. The `template_vars` `BTreeMap` is the only bag-of-string here, justified because the keys are user-supplied (not part of a closed vocabulary) and the values are not acted on by beads-rs — the consumer opens the bag.

**Protected prefix → ACL.** `convergence.*` protection (`acl.go:6`) becomes "only `BeadType::ConvergenceRoot`-writer code paths construct these updates"; ACL enforcement is structural, not string-prefix.

**Gascity impact.**
- `store.SetMetadata(beadID, FieldState, StateActive)` → `store.apply(Patch::Convergence(ConvergencePatch::SetPhase(Active)))` (via a typed patch surface).
- `parentBead.Metadata["gc.control_epoch"]` disappears — see §A.5 (`control_epoch` is derived from HLC, not from a metadata scalar).
- The big `SetMetadataBatch` after gate evaluation becomes one atomic `ConvergenceLoopState::last_gate.update(...)` call.

**CRDT merge behavior.** Every `Lww<...>` carries a stamp from the writing actor; two concurrent updates deterministically merge by stamp order. `Max<u32>` for `iteration` preserves "larger wins". `Termination` is a one-shot state (once set, never cleared), so `Lww<Option<Termination>>` is safe — but structurally we can go further: encode "terminal is terminal" by making termination immutable once set (the `Workflow::Closed` pattern in `composite.rs`). Flagged as a next-step refinement; LWW is adequate correctness today.

---

### A.2 `BeadType::Molecule*` family — workflow topology (§2a)

Source keys: `gc.kind`, `gc.formula_contract`, `gc.formula_name`, `gc.workflow_id`, `gc.root_bead_id`, `gc.step_ref`, `gc.step_id`, `gc.logical_bead_id`, `gc.root_store_ref`, `gc.source_store_ref`, `gc.source_bead_id`, `gc.idempotency_key`, `idempotency_key` (unprefixed), `gc.on_fail`, `gc.on_exhausted`, `gc.original_kind`, `gc.scope_ref`, `gc.scope_role`, `gc.scope_name`, `gc.scope_kind`, `gc.continuation_group`, `gc.template`, `gc.ralph_step_id`, `gc.spec_for`, `gc.spec_for_ref`, `gc.source_step_spec`, `gc.partial_retry`, `gc.check_mode`, `gc.check_path`, `gc.step_timeout`, `gc.check_timeout`, `gc.terminal`, `gc.output_json_required`, `gc.dynamic_fragment`.

**Chosen home**: `TypedField` across a family of bead-type variants that replace the `gc.kind` string:

```rust
enum BeadType {
    // ...existing variants...
    WorkflowRoot(WorkflowSpec),                     // gc.kind == "workflow"
    WorkflowFinalize,                               // gc.kind == "workflow-finalize"
    Scope(ScopeSpec),                               // gc.kind == "scope"
    ScopeCheck,                                     // gc.kind == "scope-check"
    ScopeCleanup,                                   // gc.kind == "cleanup" (rewritten to "retry" on compile — see original_kind)
    Ralph(RalphSpec),                               // gc.kind == "ralph"
    RalphAttempt(AttemptSpec),                      // gc.kind == "run" under ralph
    RalphCheck,                                     // gc.kind == "check" (under ralph control)
    Retry(RetrySpec),                               // gc.kind == "retry"
    RetryRun(AttemptSpec),                          // gc.kind == "retry-run"
    RetryEval,                                      // gc.kind == "retry-eval"
    FanoutControl(FanoutSpec),                      // (implicit — a control bead that fans out)
    Spec(SpecTarget),                               // gc.kind == "spec"
}
```

Every structural field that today lives in `gc.*` becomes a field on the appropriate variant's spec type. Illustrative:

```rust
struct WorkflowSpec {
    formula_contract: FormulaContract,   // gc.formula_contract — enum { GraphV2 }
    formula_name: FormulaName,           // gc.formula_name
    idempotency_key: Option<IdempotencyKey>, // gc.idempotency_key (root only) — validated newtype
    root_store_ref: StoreRef,            // gc.root_store_ref — validated newtype ("rig:<name>" shape)
    source_bead_id: Option<BeadId>,      // gc.source_bead_id
    source_store_ref: Option<StoreRef>,  // gc.source_store_ref
    scope_kind: ScopeKind,               // gc.scope_kind — enum
}

struct ScopeSpec {
    step_ref: StepRef,          // gc.step_ref — validated newtype
    scope_name: Option<String>, // gc.scope_name
    role: ScopeRole,            // gc.scope_role — enum { Body, Member, Teardown }
    on_fail: OnFail,            // gc.on_fail — enum { AbortScope, ... }
    continuation_group: Option<String>, // gc.continuation_group
}
enum ScopeRole { Body, Member, Teardown }

struct RalphSpec {
    step_ref: StepRef,
    check: CheckSpec,           // gc.check_mode + gc.check_path (only `Exec` today, validated)
    step_timeout: Duration,
    check_timeout: Duration,
    max_attempts: u32,
    on_exhausted: OnExhausted,  // enum { HardFail, SoftFail }
    ralph_step_id: StepId,      // gc.ralph_step_id
}
enum CheckSpec { Exec { path: PathBuf } }  // closed enum; new modes require a variant

struct AttemptSpec {
    step_ref: StepRef,
    attempt: u32,
    logical_bead_id: BeadId,    // gc.logical_bead_id
    // output/telemetry lives in AttemptResult (see A.4)
}

struct RetrySpec { /* same shape as RalphSpec minus check */ }

struct FanoutSpec {
    control_for: StepRef,       // gc.control_for
    for_each: FanoutSource,     // gc.for_each — a parsed selector, not an opaque string
    bond: FormulaFragmentRef,   // gc.bond
    bond_vars: FanoutVars,      // gc.bond_vars — parsed at compile, not a JSON blob at runtime
    mode: FanoutMode,           // gc.fanout_mode — enum
}
```

`gc.root_bead_id` and `gc.workflow_id` become a single typed parent pointer on every step-kind bead:

```rust
// On every workflow-member bead variant:
struct MembersInWorkflow {
    root: WorkflowRootId,       // newtype wrapping BeadId, typed by construction
}
```

**Rationale.** "Five domain states, one string field" is the translation failure from `scatter.md`. The moment `gc.kind` becomes an enum variant, every key that is valid only for that kind becomes unrepresentable on any other variant — that is the Unforgivable Error pattern inverted: wrong combinations cannot be constructed. `RalphSpec` cannot be created without `check`; `AttemptSpec` cannot be created without `attempt`. The pre-compile `gc.original_kind` debug field (e.g. `"cleanup"` stashed before rewrite to `"retry"`) is not state the engine needs; see §F.2 (`Revisit`) — it belongs in a compile-side audit log, not on the bead.

**CRDT merge behavior.** Most of these are compile-time immutable (`single-writer` in the inventory), so the `Lww<T>` wrapper is structurally wrong — they should be in `BeadCore`-adjacent immutable territory, not `BeadFields`. Concretely: the bead's `BeadType::Ralph(RalphSpec)` is set at creation and never changes. Matches `Creation` in `bead.rs:23-49`.

**`gc.idempotency_key` vs `idempotency_key` (unprefixed)**: two keys doing approximately the same job, per inventory §2a. The unprefixed one is set on non-root children by `molecule.go:428`. Collapse into **one** `idempotency_key: Option<IdempotencyKey>` on each child, plus the root also carries it. Scatter fixed in the port.

**`gc.source_step_spec` (JSON blob)**: proposed `Revisit` candidate — see §F.1.

**Gascity impact.** `bead.Metadata["gc.kind"]` switch statements become Rust `match` on the typed bead variant. Every "is this bead a scope?" check becomes `matches!(bead.typ(), BeadType::Scope(_))`.

---

### A.3 Routing (§2b)

Source keys: `gc.routed_to`, `gc.execution_routed_to`, `gc.run_target`, `gc.deferred_assignee`, `gc.deferred_routed_to`, `gc.deferred_execution_routed_to`, `gc.deferred_type`.

**Chosen home for `gc.routed_to` / `gc.execution_routed_to`**: `Derived` — computed by graphroute from `gc.run_target` (the compile-time declaration) plus the current agent-binding table. Today gascity stores the derived result back on the bead; that is the duplicate-data case from `scatter.md`. Beads-rs should compute routing at dispatch time.

Rationale: `graphroute.go:133-143` shows that `routed_to` is literally assigned from the result of resolving `run_target` against routes vars + agent bindings. The stored copy exists because Go-beads's `bd list --metadata-field gc.routed_to=worker` is the cheap index. Beads-rs has typed queries; a typed "find all steps whose derived route is `worker`" query replaces the string lookup without storing the derivation.

**Chosen home for `gc.run_target`**: `TypedField` on the owning step variant:

```rust
struct RouteDeclaration {
    run_target: TargetExpr,  // gc.run_target — parsed at compile, may contain {{vars}}
}
```

Every step bead variant (§A.2) that is routable carries a `RouteDeclaration` on its spec.

**Chosen home for the `gc.deferred_*` family**: `TypedField` as a nested enum on the owning step's routing state:

```rust
enum Routing {
    Bound { assignee: ActorId, exec: Option<ActorId> },  // normal state
    Deferred { original: Box<Routing>, reason: DeferReason }, // speculative create
}
```

Rationale: the entire "deferred_*" prefix family is encoding "this field's value is stashed because we're in a speculative create and haven't committed yet." That is one state transition, not four independent fields. The `Deferred` variant holds what was shadowed; undeferring swaps back. Scatter dies.

**Gascity impact.** `deferBeadMetadataValue(b, "gc.routed_to", DeferredRoutedToMetadataKey)` becomes `routing.defer()`. The constant names `DeferredAssigneeMetadataKey`, `DeferredRoutedToMetadataKey`, etc. disappear.

**CRDT merge behavior.** `Routing` as a single `Lww<Routing>` — defer and undefer are one atomic state transition. Concurrent writers collapse to last-wins, which matches today's behavior.

---

### A.4 Ralph attempt output / worker-result contract (§2d) + `gc.output_json`

Source keys: `gc.output_json`, `gc.output_json_required`, `gc.stdout`, `gc.stderr`, `gc.duration_ms`, `gc.truncated`, `gc.exit_code`, plus the `clearRetryEphemera` set from `ralph.go:852-883`.

**Chosen home**: `TypedField` as an `AttemptResult` on `BeadType::RalphAttempt` / `BeadType::RetryRun`:

```rust
struct AttemptResult {
    outcome: Outcome,                     // gc.outcome — enum { Pass, Fail, Skipped, Pending }
    exit_code: Option<i32>,               // gc.exit_code
    duration: Duration,                   // gc.duration_ms
    stdout: TruncatedBlob,                // gc.stdout + gc.truncated
    stderr: TruncatedBlob,
    output_json: Option<WorkerOutput>,    // gc.output_json — validated, not opaque
    failure: Option<Failure>,             // gc.failure_class + gc.failure_reason
}

enum Outcome { Pending, Pass, Fail, Skipped }
struct Failure { class: FailureClass, reason: String }
enum FailureClass { Hard, Transient }

struct WorkerOutput { body: serde_json::Value }  // intentionally validated only by shape
struct TruncatedBlob { data: Vec<u8>, truncated: bool }
```

**`gc.output_json_required`**: moves to the compile-time `RalphSpec` or `CheckSpec` — it's a contract declaration, not runtime state.

**`clearRetryEphemera`**: becomes a typed method `AttemptSpec::fresh_clone()` that constructs a new `RalphAttempt` with `AttemptResult::pending()` and no failure/output. The long literal list of keys in `ralph.go:856-881` disappears because a typed clone cannot carry old values.

**Cross-attempt propagation**: `propagateRetrySubjectMetadata` (`dispatch/retry.go:278-290`) currently copies every non-`gc.` metadata key from a subject to the logical bead. Under the typed model, this becomes a typed propagation of exactly the fields that are cross-attempt-persistent (review verdicts, etc.) — which requires `review.verdict`, `design_review.verdict`, `code_review.verdict` to have typed homes too (§A.9 below).

**CRDT merge behavior.** `AttemptResult` as a single `Lww<AttemptResult>` — the whole result is one atomic write. Current code writes it as 5–7 separate `SetMetadata` calls via `ralph.go:210-220`, which is semantically "these are one unit" pretending to be scalars.

---

### A.5 Retry/ralph control bead state (§2c)

Source keys: `gc.attempt`, `gc.max_attempts`, `gc.retry_state`, `gc.next_attempt`, `gc.retry_session_recycled`, `gc.controller_error`, `gc.closed_by_attempt`, `gc.final_disposition`, `gc.last_failure_class`, `gc.retry_count`, `gc.retry_from`, `gc.failed_attempt`, `gc.control_epoch`.

**Chosen home**: `TypedField` on the appropriate control variant (`BeadType::Ralph`, `BeadType::Retry`, `BeadType::FanoutControl`):

```rust
struct ControlState {
    spawn: Lww<SpawnPhase>,                 // gc.retry_state — enum { Initial, Spawning, Spawned }
    next_attempt: Max<u32>,                 // gc.next_attempt — monotonic
    session_recycled: Lww<bool>,            // gc.retry_session_recycled
    controller_error: Lww<Option<String>>,  // gc.controller_error
    final_disposition: Lww<Option<FinalDisposition>>, // gc.final_disposition
    closed_by_attempt: Max<u32>,            // gc.closed_by_attempt
    retry_count: Max<u32>,                  // gc.retry_count
    last_failure_class: Lww<Option<FailureClass>>, // gc.last_failure_class
    failed_attempt: Max<u32>,               // gc.failed_attempt
}

enum SpawnPhase { Initial, Spawning, Spawned }
enum FinalDisposition { Pass, HardFail, SoftFail, ControllerError }
```

**`gc.attempt` on an attempt bead**: moves to `AttemptSpec::attempt: u32` (§A.2), immutable per attempt bead.

**`gc.max_attempts`**: moves to `RalphSpec`/`RetrySpec`, compile-time immutable (§A.2).

**`gc.retry_from`**: `DepEdgeState` — an inter-attempt dep edge with kind `replaces-attempt`, not metadata. See §D.1.

**`gc.control_epoch`** (`molecule.go:247, 292`, `control_integration_test.go:103` etc.): **`Derived`** — this is an epoch counter used for dispatch idempotency. It is derivable from either (a) the HLC `WriteStamp` of the last "reset" event on the control bead, or (b) the cardinality of retry-run children. Storing it back on the bead is scatter.

**`gc.attempt_log` (JSON blob, read-modify-write)**: the strongest typing pressure in the entire inventory. See §C.1 — the correct home is a typed append-only log at the dep-traversal layer, not a JSON blob on a scalar field. Flagged both as `Derived` **and** as a spot where a real append-log primitive is warranted.

**Rationale.** The entire retry/ralph control surface is a state machine with 4–6 fields that today masquerade as 13 independent scalars. The group is small enough to keep as one `ControlState` per control variant. `next_attempt` is monotonic (not LWW) — the Go code has an implicit assumption that concurrent writes never happen because one controller owns the bead; beads-rs can encode that assumption as `Max<u32>` and be correct even under controller fault-over.

**Gascity impact.** `SetMetadataBatch(bead.ID, {"gc.retry_state": "spawned", "gc.next_attempt": "3"})` → `control.spawn.set(Spawned); control.next_attempt.bump(3);` or — better — one method `control.record_spawn_complete(3)` that updates both atomically.

---

### A.6 Fanout (§2e)

Source keys: `gc.fanout_state`, `gc.fanout_mode`, `gc.for_each`, `gc.bond`, `gc.bond_vars`, `gc.control_for`, `gc.spawned_count`, `gc.partial_fragment`, `gc.dynamic_fragment`.

**Chosen home**: `TypedField` on `BeadType::FanoutControl` (`FanoutSpec` compile-time, `FanoutState` runtime):

```rust
// compile-time, on FanoutControl's spec:
struct FanoutSpec { /* see A.2 */ }

// runtime mutable:
struct FanoutState {
    phase: Lww<FanoutPhase>,   // gc.fanout_state — enum { Initial, Spawning, Spawned }
    spawned_count: Max<u32>,   // gc.spawned_count
}
enum FanoutPhase { Initial, Spawning, Spawned }
```

**`gc.partial_fragment`**: `TypedField` as `enum FragmentOrigin { Complete, PartialSpawn }` on every member bead — it's a per-bead marker used during cleanup of aborted fanouts. `Lww<FragmentOrigin>` because the marker is set then cleared.

**`gc.dynamic_fragment`**: `TypedField` as a boolean-ish field on member beads, immutable at compile. Actually better modeled as a **NamespaceProperty** of the fragment's temporary namespace — the whole fragment is "dynamic" or not, not each bead separately. See §B.2.

---

### A.7 `convoy.*` (§3)

Source keys: `convoy.owner`, `convoy.notify`, `convoy.molecule`, `convoy.merge`, `target`.

**Chosen home**: `TypedField` on `BeadType::Convoy`:

```rust
struct ConvoyFields {
    owner: Lww<ActorId>,
    notify: Lww<NotifyTarget>,          // enum { Human, ... } — closed set from observed usage
    molecule: Lww<Option<BeadId>>,
    merge: Lww<MergeStrategy>,          // enum { Direct, Mr, Local }
    target_branch: Lww<BranchName>,     // unprefixed "target" key
}
```

Gascity already treats these as a struct via `convoy/convoy_fields.go:12-33`. This port makes the wire shape catch up with the Go-side type.

`convoy.molecule` as `Option<BeadId>` pointing at the attached molecule root is a candidate for `DepEdgeState` (a `tracks` or `owns-molecule` dep edge). See §D.2. Keeping both a typed field and a dep-edge is scatter; the field probably wins because it's a single-value pointer and the existing readership is a bead-by-id lookup, not a graph traversal.

**`target` (unprefixed)**: same typed home. The fact that it's unprefixed in Go today is exactly the "implicit" failure mode of `scatter.md`: "conventions that aren't in the structure." In beads-rs it's `ConvoyFields::target_branch`, typed and unambiguous.

---

### A.8 Session lifecycle (§4)

This is where the "types tell the truth" pressure is highest. Inventory §4 lists roughly 50 keys across identity, state machine, worker profile, and wait-bead concerns, plus three keys with flagged concurrency hazards (`pending_create_claim`, `alias_history`, multi-replica name reservation).

**Chosen home**: split into four typed bead variants plus typed substructs.

```rust
enum BeadType {
    // ...
    Session(SessionSpec),     // LabelSession in manager.go:57
    WorkerProfile(ProfileFields), // sits either on Session or as a sidecar
    Wait(WaitSpec),           // wait bead per session/nudge
    NudgeQueue(NudgeSpec),
}

struct SessionSpec {
    template: TemplateName,                   // template
    provider: ProviderId,                     // provider / provider_kind / builtin_ancestor
    work_dir: PathBuf,                        // work_dir
    command: LaunchCommand,                   // command + resume_*
    origin: SessionOrigin,                    // session_origin — enum { Manual, Named, Worker, Ephemeral }
    configured_named: Option<NamedConfig>,    // configured_named_* family — presence = true
    // session_key / instance_token / generation / continuation_epoch are mutable — see SessionRuntime
}
struct NamedConfig { identity: String, mode: NamedMode }

struct SessionRuntime {
    lifecycle: Lww<Lifecycle>,                // state + state_reason + all the transition keys
    runtime_start: Lww<Option<RuntimeStart>>, // started_config_hash + live_hash + core_hash_breakdown
    create_claim: Cas<Option<ClaimToken>>,    // pending_create_claim — CAS, not LWW
    names: NameBinding,                       // session_name + alias + alias_history
    keys: Lww<SessionKeys>,                   // session_key + instance_token
    counters: Counters,                       // generation + continuation_epoch
    transcripts: DialogState,                 // startup_dialog_verified + continuation_reset_pending + restart_requested
}

enum Lifecycle {
    Creating   { reason: String },
    Active     { entered_at: WriteStamp, reason: String },
    Asleep     { at: WriteStamp, reason: SleepReason, intent: Option<String> },
    Suspended  { at: WriteStamp },
    Draining   { since: WriteStamp, reason: String },
    Drained    { since: WriteStamp },
    Quarantined { until: WriteStamp, cycle: u32, crash_count: u32 },
    Archived   { at: WriteStamp, reason: String, continuity_eligible: bool,
                 retired_named_identity: Option<String> },
    Closed     { at: WriteStamp, reason: String, synced_at: WriteStamp },
    Orphaned, Closing, Stopped,  // compatibility states from lifecycle_projection.go:30-39
}

struct Counters {
    generation: Max<u32>,
    continuation_epoch: Max<u32>,
    wake_attempts: Counter,     // resettable; see §C.2
    churn_count: Counter,
    crash_count: Counter,
}

struct NameBinding {
    session_name: Cas<Option<SessionName>>,   // multi-replica reservation — needs CAS
    alias: Cas<Option<Alias>>,
    alias_history: OrSet<Alias>,              // or-set; see §C.3
}
```

The `MetadataPatch` functions in `lifecycle_transition.go` become typed transitions on `Lifecycle`:
- `RequestWakePatch(reason)` → `Lifecycle::Creating { reason }` + `Counters::reset_wake()` + `create_claim.request()`
- `ConfirmStartedPatch(now)` → `Lifecycle::Active { entered_at: now, reason: "creation_complete" }`
- `BeginDrainPatch(now, reason)` → `Lifecycle::Draining { since: now, reason }`
- etc.

Each patch is an atomic method on a typed state machine. The "merge two lifecycles" rule becomes deterministic by stamp on the `Lww<Lifecycle>`.

Where the `MetadataPatch` had to clear a dozen keys simultaneously (e.g. `ClearWakeBlockersPatch`), the state-machine transition naturally discards fields belonging to the prior variant — no explicit "clear these keys" list survives. That's the scatter-kill win.

**Unclear-from-source keys** (`last_woke_at`, `common_name`): these appear as reads in the gascity code but the grep did not surface a concrete writer in non-test files. Before the port lands, read `session/manager.go` + `session/names.go` end-to-end for `last_woke_at` and `session/names.go:375` for `common_name` to confirm their writers. Likely both are writer-in-tests-only or in a code path I scoped out. Mark as TODO; likely merges into `SessionRuntime::keys` / `SessionSpec`.

**Worker profile keys** (`worker_profile`, `worker_profile_provider_family`, `worker_profile_transport_class`, `worker_profile_compatibility_version`, `worker_profile_certification_fingerprint`, `mc_session_kind`): bundle into one `ProfileFields` typed struct attached to the session:

```rust
struct ProfileFields {
    profile: Lww<WorkerProfile>,
    provider_family: Lww<ProviderFamily>,
    transport_class: Lww<TransportClass>,
    compat_version: Lww<SemVer>,
    certification: Lww<CertificationFingerprint>,
    session_kind: Lww<SessionKind>,
}
```

Scatters into `SessionRuntime`; the fingerprint / semver are validated newtypes, not strings.

**Wait / nudge beads** (`session_id`, `nudge_id`, wait-`state`, `terminal_reason`, `commit_boundary`, `terminal_at`): become `BeadType::Wait(WaitSpec)` with a `WaitLifecycle` state machine mirroring the session one, and dep edges to the parent session and the upstream nudge. The `session_id` and `nudge_id` metadata keys become **`DepEdgeState`** — the `waits-for-session` dep and the `waits-for-nudge` dep (see §D.3).

---

### A.9 Non-gc propagated keys: review verdicts

Source keys: `review.verdict`, `design_review.verdict`, `code_review.verdict` (from `ralph.go:877-879`, the `clearRetryEphemera` set).

These are per-attempt worker outputs that the retry propagator explicitly propagates to the logical bead. Today they are bare unprefixed metadata scalars; gascity has no central definition of what values they take.

**Chosen home**: `TypedField` as a `ReviewVerdicts` struct on `AttemptResult` (§A.4):

```rust
struct ReviewVerdicts {
    review: Lww<Option<Verdict>>,
    design_review: Lww<Option<Verdict>>,
    code_review: Lww<Option<Verdict>>,
}
```

Where `Verdict` is the same enum from §A.1 (`Approve`, `ApproveWithRisks`, `Block`). Scatter fixed: one verdict enum shared by convergence and by review.

---

### A.10 Molecule-as-consumer pointers (§6)

Source keys: `molecule_id`, `workflow_id`, `merge_strategy`, `notes`, `from`, `molecule_failed`.

**`molecule_id` / `workflow_id` (on non-molecule beads)**: **`DepEdgeState`**. These are pointers from a consumer bead (e.g. a source bead that commissioned the workflow) to the attached root. That's a dep edge, not a scalar. See §D.4.

**`merge_strategy`**: `TypedField` on the consumer bead's spec — same `MergeStrategy` enum as `ConvoyFields::merge`. One enum, one definition, used in two places.

**`notes`**: this overlaps with beads-rs's existing `Note` composite (see `beads-core::composite::Note`). Already typed. The `notes` key's job is to carry `step.Notes` from the formula template into the bead at compile time; the typed home is "use the `Note` type with an author = controller," not a metadata scalar.

**`from`**: the "who originated this" field. Gascity uses `Metadata["from"]` as a fallback when the `from` struct field is empty (`bdstore.go:340,416`). Beads-rs already has `BeadCore::created().by` which is exactly this. **`Derived`** — no separate storage.

**`molecule_failed`**: `TypedField` — add a `MoleculeFailed` variant (or a `failed: bool` field) to `BeadType::WorkflowRoot` / `BeadType::Scope`. Or reuse the existing `Workflow::Closed` with a reason field (`bead.rs:341-349`): a "molecule failed" is a close-reason, not an orthogonal flag. Latter is cleaner.

---

### A.11 `extmsg` (§5)

Every extmsg record type is already a typed-struct-in-disguise: `ConversationTranscriptRecord`, `ConversationMembershipRecord`, `SessionBindingRecord`, `ConversationGroupRecord`, `ConversationGroupParticipant`, `DeliveryContextRecord`. Each has a single writer and a single reader, and each `encodeMetadataFields` call enumerates the full key set.

**Chosen home**: `TypedField` on dedicated bead variants:

```rust
enum BeadType {
    // ...
    ExtmsgTranscript(TranscriptFields),
    ExtmsgTranscriptState(TranscriptStateFields),
    ExtmsgMembership(MembershipFields),
    ExtmsgBinding(BindingFields),
    ExtmsgGroup(GroupFields),
    ExtmsgParticipant(ParticipantFields),
    ExtmsgDelivery(DeliveryFields),
}
```

Each variant's field struct mirrors §5a–5g of the inventory, with the following load-bearing typing decisions:

- **`ConversationRef`** as a validated struct (`scope_id`, `provider`, `account_id`, `conversation_id`, `parent_conversation_id`, `conversation_kind`) — seven keys become one validated newtype. Shared across transcript/membership/binding/group.
- **`MembershipFields::owners: OrSet<MembershipOwner>`** — this is the clearest OR-set case; current code round-trips through a comma-joined string (`encodeMembershipOwners`/`decodeMembershipOwners`). See §C.4.
- **`ParticipantFields::pending_session_cleanup: OrSet<SessionId>`** — same OR-set treatment for `previous_session_id_pending_cleanup`. See §C.5.
- **`TranscriptFields::actor: Option<Actor>`** and **`attachments: Vec<Attachment>`** — stored directly as typed Rust values, not as `actor_json`/`attachments_json` opaque blobs.
- **`TranscriptStateFields::next_sequence: Max<u64>`** — monotonic; not LWW.
- **`MembershipFields::last_read_sequence: Max<u64>`** — same.
- **`BindingFields::last_touched_at: Lww<WriteStamp>`** — LWW of a timestamp. The existing beads-rs `WriteStamp` handles this already.

**`BindingFields::last_touched_at`** is also a candidate for `Derived` (from the most recent write stamp on this bead), but the Go code reads it as a separate field for ordering decisions, which beads-rs can expose through `BeadView::updated_stamp` directly. Hold as `Derived`.

**Schema version** (`schema_version`): `TypedField`, but as a **wire-format** version attached to the namespace or the crate version, not per-bead. See §B.3.

**Other misc extmsg**: `route` (string) on outgoing mail beads — `TypedField` as an enum `MailRoute { ThreadReply, ... }`; `source` — `TypedField` `enum MailSource { Discord, Startup, Phase2, ... }` if the set is closed, else `String` with a runtime-checked closed vocab. Needs a closed-set inspection pass.

---

## B. NamespaceProperty

### B.1 Ephemeral / wisp-ness

Gascity distinguishes "real workflow beads" from "wisps" (ephemeral speculative beads). Today the distinction is encoded via the `Ephemeral=true` flag on a molecule plus the `gc:extmsg-*` label family — not as metadata keys per se, but functionally similar: a per-bead flag to mark "this lives in a throwaway container."

**Chosen home**: `NamespaceProperty`. A namespace carries `ephemeral: bool` (or equivalently, namespaces come in two flavors: persistent and ephemeral). Every bead in an ephemeral namespace inherits the flag; no per-bead marker needed. This is the canonical case from `scatter.md` — derive, don't retrieve. The `bd purge` behavior (`bdstore.go:149-184`) already treats "ephemeral" as a whole-container property, since it purges all closed beads under the ephemeral root.

**Rationale.** Today `Ephemeral` is a column on the molecule table, not a bead metadata key. Keep the namespace-level home; do not introduce per-bead metadata to represent it.

### B.2 `gc.dynamic_fragment`

Every bead in a dynamically-generated fragment (i.e. a fanout fragment at a retry boundary, per inventory §2e) has `gc.dynamic_fragment = "true"`. Consumers use this to exclude the whole fragment from retry-scope-walks.

**Chosen home**: `NamespaceProperty` on a per-fragment sub-namespace. A fanout fragment spawns its members under a namespace whose property `dynamic_origin: FragmentId` makes "exclude from retry scope" a query on the namespace, not a per-bead metadata check.

Alternative: keep as a `TypedField` boolean on each member bead (§A.6). Namespace is cleaner because the "dynamic" flag applies to a group of beads generated together.

### B.3 `schema_version` (extmsg)

Every extmsg record carries `schema_version` as a scalar int. This is wire-format versioning, not runtime state.

**Chosen home**: `NamespaceProperty` — the extmsg namespace declares its wire schema version; individual beads inherit. Failing that, declare at the crate level: beads-rs's `beads-api` crate pins a single schema version for each record variant at compile time. The per-bead int becomes a compile-time constant.

---

## C. Concurrency-sensitive remaps (OR-set, Max, CAS, AppendLog)

These keys do not merge correctly under naïve LWW-on-string-map semantics. They need explicit CRDT types.

### C.1 `gc.attempt_log` — append-only log

Today: a JSON blob on the control bead, read-modify-write by `appendAttemptLog` at `dispatch/control.go:935-973`.

**Chosen home**: `TypedField` `attempt_log: AppendLog<AttemptEntry>` on `ControlState` (§A.5):

```rust
struct AttemptEntry {
    attempt: u32,
    outcome: Outcome,     // § A.4 enum
    reason: Option<String>,
    at: WriteStamp,
}
```

`AppendLog<T>` needs to be built in `beads-core`. The semantics are add-only with dedup by `(attempt, at)`; merge is set-union; iterating produces entries ordered by attempt. Beads-rs does not currently have this primitive (existing types: `Lww<T>`, `Labels` is an OR-set); adding it is the forcing function for this key.

**Rationale.** The current read-modify-write on a JSON blob at the `map[string]string` layer is a genuine race. Two concurrent appendAttemptLog calls with stale reads produce two writes — LWW picks one and silently drops entries from the other. The typed AppendLog lifts the merge rule up to where it's correct.

### C.2 Counter keys that get reset (`wake_attempts`, `churn_count`, `crash_count`)

Today: LWW-by-string-map. Reset-then-increment is safe because one controller owns the session; but two controllers during fault-over race.

**Chosen home**: `TypedField` as `Counter` (a new type pending, or reuse `(Max<u32> /*sum*/, Max<WriteStamp> /*epoch*/)` pattern). Increments compose, resets bump the epoch. Merge takes `max(sum)` within the same epoch; if epochs differ, later epoch wins and sum restarts.

This is a minor typing exercise; it is listed here because the current string-map representation is genuinely wrong-under-races and must not be ported as-is.

### C.3 `alias_history` — OR-set

Today: CSV-joined string. Two concurrent appends LWW into one that drops the other.

**Chosen home**: `TypedField` `alias_history: OrSet<Alias>` on `NameBinding` (§A.8). Beads-rs already has `Labels` which is an OR-set; extract the underlying primitive or reuse the `Labels`-like machinery for typed OR-sets.

### C.4 `membership_owner_kinds` — OR-set

Today: encoded via `encodeMembershipOwners`/`decodeMembershipOwners` as a comma-joined string. Multi-replica membership operations (a manual add + a binding add) race.

**Chosen home**: `TypedField` `owners: OrSet<MembershipOwner>` on `MembershipFields` (§A.11).

### C.5 `previous_session_id_pending_cleanup` — OR-set

Same shape as C.4; participant pending cleanup.

**Chosen home**: `TypedField` `pending_session_cleanup: OrSet<SessionId>` on `ParticipantFields` (§A.11).

### C.6 `pending_create_claim` — CAS

Today: `"true"`/`""` scalar that the controller treats as a one-shot lease. Two controllers trying to wake the same session both write `"true"`; LWW merges them into "one write won"; both think they own the claim.

**Chosen home**: `TypedField` `create_claim: Cas<Option<ClaimToken>>` on `SessionRuntime` (§A.8). `Cas<T>` supports "compare-and-set" at the CRDT layer: a write specifies the expected prior value; merge rejects if the expected does not match (or keeps the existing, deterministically). `ClaimToken` is a per-claim nonce so two concurrent claims with different tokens are distinguishable post-merge.

This is the forcing function for a `Cas<T>` primitive in beads-core. Today's `Lww<T>` is structurally wrong for leases.

### C.7 `session_name`, `alias` — multi-replica reservation

Today: `EnsureSessionNameAvailable` pre-checks then writes; two replicas can both pass the pre-check. LWW picks one; both think they reserved.

**Chosen home**: `TypedField` `Cas<Option<Name>>` on `NameBinding` (§A.8), with a separate typed reservation registry in the namespace to coordinate — because "this name is taken" is a namespace-global fact, not a per-bead flag. May need a **`NamespaceProperty`** backing store (a `names: OrSet<SessionName>` on the namespace) in addition to the per-bead field, with merge rule "if two beads both claim the same name in the OR-set, the later-stamped one wins and the earlier one gets `name = None`."

This is non-trivial CRDT design. Flagged here; the likely resolution is to keep gascity's pre-check but back it with a namespace-scoped uniqueness constraint at the store layer. Beads-rs's existing `NamespaceId` machinery (`crates/beads-core/src/identity.rs`) would carry the registry.

---

## D. DepEdgeState

### D.1 `gc.retry_from`

Today: bead-id scalar on a retry-run pointing at the previous attempt.

**Chosen home**: a typed dep edge `DepKind::ReplacesAttempt` from new attempt → previous attempt. Beads-rs already has `DepKind::{Blocks, Parent, Related, DiscoveredFrom}` (`crates/beads-core/src/domain.rs:54-59`); extend to `{Blocks, Parent, Related, DiscoveredFrom, ReplacesAttempt}`. Edge carries no payload; the presence of the edge IS the assertion.

### D.2 `convoy.molecule`

Today: bead-id scalar on a convoy pointing at the attached molecule root.

**Chosen home**: `DepKind::AttachesMolecule` (or reuse `DepKind::Parent` + a namespace distinguishing "convoy-parent-of-molecule" from "convoy-parent-of-work-bead"). Lean toward a new dep-kind because the convoy/molecule relationship is structurally different from parent/child task hierarchy.

Equally valid alternative: keep as `TypedField` `molecule: Option<BeadId>` on `ConvoyFields` (§A.7). Trade-off: dep-edge is queryable via graph walks (beads-rs's typed dep-graph API); typed-field is a single-indirection lookup. Either is fine; **defer the choice until the convoy graph walker lands and the query shape is concrete**.

### D.3 Wait bead pointers (`session_id`, `nudge_id` on a wait bead)

Today: two scalar pointers.

**Chosen home**: `DepKind::WaitsForSession` and `DepKind::WaitsForNudge`. The semantics are "this wait bead is blocked until the pointed-to entity fires."

### D.4 `molecule_id` / `workflow_id` (on a consumer source bead)

Today: scalar pointers on a non-molecule bead, used by `sling/sling_attachment.go` and `api/handler_beads.go:25` to attach a workflow/molecule to its source.

**Chosen home**: `DepKind::AttachesWorkflow` (or reuse `DepKind::Parent`), with edge direction source → attached-root.

### D.5 `gc.source_bead_id`, `gc.source_step_spec` — **partial dep-edge candidate**

`gc.source_bead_id` is a pointer from a workflow-root back to the source bead that commissioned it. That's `DepKind::DiscoveredFrom` (already in the enum). `gc.source_step_spec` is a blob of compile-input data; see §F.1 for a `Revisit` discussion.

### D.6 `convoy.notify` is NOT a dep edge

Even though `convoy.notify = "human"` looks like "notify this target," it is an enum tag declaring notification policy, not a pointer to a notified entity. Stays as `TypedField` (§A.7). Flagged here because it's the kind of key that looks deppable and isn't.

---

## E. RuntimeState

These keys do not belong in any persistent store. They live in gascity's process substrate.

### E.1 `transport` (session)

Today: `"tmux"` / `"acp"` normalized at `session/manager.go:153,162`.

**Chosen home**: `RuntimeState` — the transport is a property of the live runtime session, not the bead. The Go code already detects it at runtime (`detector.DetectTransport(sessName)` at `manager.go:162`) and caches it in metadata purely to avoid re-detection.

Subsystem that would own it: `session.ProcessSupervisor` (the `sp` field in `Manager`) or the runtime detector. Re-detect on every session attach; amortize with an in-memory cache keyed by session name.

However — **if it must persist** because cold-start reboots need it: `TypedField` on `SessionRuntime` as `Lww<Option<Transport>>`. Decide based on whether transport detection is expensive. If it's free, `RuntimeState`; if it requires starting a process, `TypedField`. Leaning `RuntimeState` because `DetectTransport` is an in-process inspection.

### E.2 Bed-process PIDs, tmux session names (when not user-assigned)

Gascity does not seem to persist PIDs today (I did not find any `pid` metadata key in the inventory), but it does persist `session_name`. For the non-user-assigned case (`sessionNameFor(b.ID)` at `manager.go:336`), the generated name is deterministic from the bead id and could be recomputed on the fly — making it `Derived` rather than stored state.

**Chosen home for auto-generated `session_name`**: `Derived`. User-assigned session names are a different story (see §C.7).

### E.3 `instance_token`

Today: per-process nonce set at session create/resume (`manager.go:297`, `chat.go:228,333`).

**Chosen home**: **`RuntimeState`** — this is a per-process identity token used to detect "is the runtime I'm looking at the same one I started?" It's stored today because the controller wants to persist across controller restart; but controller-restart should re-verify the token via the runtime, not re-read it from the bead. Whoever owns the live runtime-binding map (`session.Manager`) would own this token in memory.

If controller-restart-tolerance proves expensive to rebuild via runtime probing, fall back to `TypedField` as in §A.8. Starting position: `RuntimeState`.

---

## F. Revisit

Two keys where I cannot find a clean typed home. Both point at one real gap in beads-rs's type vocabulary (no "blob with declared shape but opaque content" primitive) rather than a need for freeform metadata in general.

### F.1 `gc.source_step_spec` (JSON blob)

Today: a full serialized compile-time step spec, stashed on the control bead so a later retry can re-expand fragments faithfully (`control_integration_test.go:41`).

**Why Revisit, not TypedField**: The spec's Rust type (`StepSpec` or similar) is already defined in the formula compiler. Storing it on the bead as a typed field `source_spec: Lww<StepSpec>` is in principle correct — but the spec carries nested types (expressions, substitution variables, agent resolvers) that are versioned and that the compiler may extend. If beads-rs pins a typed representation at the bead layer, every compiler change becomes a beads-schema migration. The Go code avoids this by serializing to JSON and re-parsing at expand-time.

**Proposed resolution**: keep as `TypedField` with a wrapper type `serde_json::Value` (or a shape-validated `StepSpecJson` newtype whose deserialize is "parse as current StepSpec, fall back to raw Value on version skew"). That is still typed at the boundary — the shape is "some JSON object known to be parseable by the formula compiler at this version" — but the content is opaque to the bead engine.

This is one rare `Revisit` where a genuine "validated opaque blob" primitive has a case. Not a general "add freeform metadata" primitive — a specific "here's a blob with a declared shape and a version tag." Belongs in `beads-api` or the formula crate, not `beads-core`.

**Equally valid alternative**: ship `source_step_spec` as an **attachment** (separate bead with `DepKind::DiscoveredFrom` and a `BeadType::CompilerInputs` variant). That avoids stuffing a large blob on the control bead. Trade-off: more beads, more deps, harder to debug. Prefer inline blob with versioned shape.

### F.2 Unclear-from-source keys

Listed in inventory §2a with "unclear from source": `gc.session`, `gc.work_dir`, `gc.city_path`, `gc.endpoint_origin`, `gc.endpoint_status`, `gc.retry_from` (writer-wise), `gc.terminal` (origin-wise), `common_name`, `last_woke_at` (writer-wise).

**These are not Revisit in the "needs-metadata-primitive" sense.** They are Revisit in the "needs one more pass through the code before assigning a home" sense. Each likely lands in one of A/B/D above once the writer path is read. Bundled here so they are not forgotten.

Minimum re-read set to close these:
- Grep `gc.session =` (equals sign) in `internal/`.
- Read `dispatch/ralph.go` in full around line 19 for `gc.terminal`.
- Read `session/manager.go` and `session/chat.go` for `last_woke_at` write site.
- Read `fanout.go` end-to-end for `gc.endpoint_*`.

---

## Summary counts

Each decision category maps one key group. Some groups contain multiple keys (e.g. "convergence" is 27 keys under one TypedField structural home). Counts below are per distinct inventory row (group-level, not per-scalar-key — because the design decision is "these 8 gate_* keys land as one `GateResult` struct" not 8 separate decisions).

| Home | Group count | Notes |
|---|---:|---|
| **TypedField** | ~30 | A.1 (convergence, 1 row = 27 keys); A.2 (molecule/workflow topology, 1 = ~25 keys); A.3 (routing, 3); A.4 (attempt result, 1 = 7 keys); A.5 (control state, 1 = 10 keys); A.6 (fanout, 1 = 7 keys); A.7 (convoy, 1 = 5 keys); A.8 (session, split into 4 = ~45 keys); A.9 (review verdicts, 1 = 3 keys); A.10 (consumer pointers minus molecule/workflow_id, 3); A.11 (extmsg, 7 = ~60 keys). Dominant by far. |
| **NamespaceProperty** | 3 | B.1 ephemeral/wisp; B.2 dynamic_fragment; B.3 schema_version. |
| **Label** | 0 | No key in the inventory has pure present/absent semantics without a value payload. gascity uses beads-rs labels heavily (`gc:session`, `nudge:<id>`, etc.) but those are already labels, not metadata. |
| **DepEdgeState** | 5 | D.1 retry_from; D.2 convoy.molecule (pending); D.3 wait pointers (2); D.4 molecule_id/workflow_id (1 row for both); D.5 source_bead_id (reuses DiscoveredFrom). |
| **RuntimeState** | 3 | E.1 transport; E.2 auto-generated session names (overlaps Derived); E.3 instance_token. |
| **Derived** | ~5 | gc.routed_to & gc.execution_routed_to (from run_target + bindings); gc.control_epoch (from HLC); from (from BeadCore.created.by); auto-generated session_name; BindingFields::last_touched_at (from BeadView.updated_stamp). |
| **Revisit** | 2 | F.1 gc.source_step_spec (real case for a versioned opaque blob); F.2 unclear-from-source bundle (not a primitive gap, just more reading needed). |

**Quality signal: 2 Revisit items, 0 of which argue for a generic freeform-metadata primitive.** F.1 is one specific primitive need (a versioned opaque blob) that is narrowly scoped; F.2 is not a primitive gap, just deferred analysis.

---

## Open infrastructure needed in `beads-core`

The remaps above lean on CRDT primitives beyond the current `Lww<T>` / `Labels`. Concrete additions the port requires:

1. `Max<T: Ord>` — monotonic counter. Used for: `iteration`, `next_sequence`, `next_attempt`, `generation`, `continuation_epoch`, `spawned_count`, `closed_by_attempt`, `retry_count`, `failed_attempt`, `last_read_sequence`.
2. `OrSet<T: Hash + Eq>` — add-wins set. Used for: `alias_history`, `membership_owner_kinds`, `pending_session_cleanup`, and (possibly) a namespace name-registry.
3. `AppendLog<T>` — add-only log with deterministic merge. Used for: `gc.attempt_log`.
4. `Cas<T>` — compare-and-set scalar. Used for: `pending_create_claim`, `session_name`/`alias` reservation.
5. `Counter` (resettable monotonic) — used for: `wake_attempts`, `churn_count`, `crash_count`.

All five already have standard CRDT definitions; none is research. Each will arrive with its own proof family in `beads-core::tests/`.

---

## How to use this doc

- When porting a gascity subsystem to beads-rs, look it up in the inventory, then find the corresponding row here.
- When beads-rs's `BeadType` / field shape is being designed (custom-types port, typed-bead epic, etc.), cross-check against the rows above: every typed bead variant landing in core should satisfy the `TypedField` target described here.
- When the gascity side wants to add a new metadata key, first consult this doc: if the key fits into one of the existing typed homes, extend the type instead. If it cannot fit, that's a signal to open a fresh bead and revisit the vocabulary — not to sneak in a `map[string]string` stash.
