# gate

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**See also:** `primitives/convergence-loops.md` (TBD),
`primitives/metadata-remapping.md` (TBD).

## Purpose

A gate is an async coordination primitive: a bead that represents "wait
until a condition is satisfied, then let downstream work proceed". Gates
combine a manual-approval channel (human says "go") with a condition
channel (script returns exit 0) and a timeout policy. Gates are the
primary convergence-loop checkpoint for gascity's review workflow.

Gascity's creation sites:

- `internal/convergence/gate.go` — gate config parsing and result struct.
- `internal/convergence/create.go` — gate bead creation (part of the
  convergence loop root).
- `internal/convergence/evaluate.go` — runs the condition script and
  writes the outcome back.
- Go's `Issue` struct has `AwaitType`, `AwaitID`, `Timeout`, `Waiters` at
  `types.go:93-96` — a second, older gate surface for GitHub-run /
  GitHub-pr / timer / human / mail gates.

Distinguishing feature vs adjacent:

- `molecule` — workflow container. `gate` — one checkpoint in a workflow.
- `message` — async communication. `gate` — async wait condition.
- `merge-request` — external VCS artifact. `gate` — internal condition.

## Typed field set

beads-rs models gates as the union of the convergence gate (gascity's
rich surface) and the await-gate (Go's `Issue.AwaitType`/`AwaitID`).

```rust
pub struct GateFields {
    /// What kind of gate this is. Discriminates the rest of the fields.
    pub gate_kind: Lww<GateKind>,

    /// Gate evaluation mode (manual | condition | hybrid).
    /// Source: `metadata["convergence.gate_mode"]`
    /// (gascity `internal/convergence/metadata.go:18`).
    pub gate_mode: Lww<GateMode>,

    /// Path to condition script (when gate_mode is Condition or Hybrid).
    /// Source: `metadata["convergence.gate_condition"]`
    /// (gascity `metadata.go:19`).
    pub gate_condition: Lww<Option<PathBuf>>,

    /// Condition-script timeout.
    /// Source: `metadata["convergence.gate_timeout"]` (`metadata.go:20`);
    /// default 5m (`gate.go:11`).
    pub gate_timeout: Lww<Duration>,

    /// What happens when the gate times out.
    /// Source: `metadata["convergence.gate_timeout_action"]`
    /// (`metadata.go:21`).
    pub gate_timeout_action: Lww<GateTimeoutAction>,

    /// Outcome of the most recent evaluation.
    /// Source: `metadata["convergence.gate_outcome"]` (`metadata.go:26`,
    /// values at `metadata.go:78-83`).
    pub gate_outcome: Lww<GateOutcome>,

    /// Exit code from the condition script, when applicable.
    /// Source: `metadata["convergence.gate_exit_code"]` (`metadata.go:27`).
    pub gate_exit_code: Lww<Option<i32>>,

    /// Retry count used before reaching the recorded outcome.
    /// Source: `metadata["convergence.gate_retry_count"]` (`metadata.go:29`);
    /// capped at `MaxGateRetries = 3` (`gate.go:14`).
    pub gate_retry_count: Lww<u32>,

    /// Captured stdout from last condition run (truncated).
    /// Source: `metadata["convergence.gate_stdout"]` (`metadata.go:36`).
    pub gate_stdout: Lww<Option<String>>,

    /// Captured stderr (truncated).
    /// Source: `metadata["convergence.gate_stderr"]` (`metadata.go:37`).
    pub gate_stderr: Lww<Option<String>>,

    /// Wall-clock duration of last evaluation.
    /// Source: `metadata["convergence.gate_duration_ms"]` (`metadata.go:38`).
    pub gate_duration: Lww<Option<Duration>>,

    /// Whether stdout/stderr were truncated.
    /// Source: `metadata["convergence.gate_truncated"]` (`metadata.go:39`).
    pub gate_truncated: Lww<bool>,

    /// ======== Await-style gates (Go bd, older surface) ========
    /// Condition source for external gates.
    /// Source: Go `Issue.AwaitType` at `types.go:93`.
    pub gate_await_type: Lww<Option<AwaitType>>,

    /// External condition identifier (PR number, run ID, timer handle).
    /// Source: Go `Issue.AwaitID` at `types.go:94`.
    pub gate_await_id: Lww<Option<String>>,

    /// Mail addresses to notify when gate clears.
    /// Source: Go `Issue.Waiters` at `types.go:96`.
    /// OR-set: notifications can accrete concurrently without a winner.
    pub gate_waiters: OrSet<MailAddress>,
}
```

| Field | Type | CRDT | Source | Default / Required |
|---|---|---|---|---|
| `gate_kind` | `GateKind` | `Lww` | discriminator synthesized from Go fields | required at creation |
| `gate_mode` | `GateMode` | `Lww` | `convergence.gate_mode` | `Manual` |
| `gate_condition` | `Option<PathBuf>` | `Lww` | `convergence.gate_condition` | `None` |
| `gate_timeout` | `Duration` | `Lww` | `convergence.gate_timeout` | `5m` |
| `gate_timeout_action` | `GateTimeoutAction` | `Lww` | `convergence.gate_timeout_action` | `Iterate` |
| `gate_outcome` | `GateOutcome` | `Lww` | `convergence.gate_outcome` | `Pending` (see below) |
| `gate_exit_code` | `Option<i32>` | `Lww` | `convergence.gate_exit_code` | `None` |
| `gate_retry_count` | `u32` | `Lww` | `convergence.gate_retry_count` | `0` |
| `gate_stdout` | `Option<String>` | `Lww` | `convergence.gate_stdout` | `None` |
| `gate_stderr` | `Option<String>` | `Lww` | `convergence.gate_stderr` | `None` |
| `gate_duration` | `Option<Duration>` | `Lww` | `convergence.gate_duration_ms` | `None` |
| `gate_truncated` | `bool` | `Lww` | `convergence.gate_truncated` | `false` |
| `gate_await_type` | `Option<AwaitType>` | `Lww` | `Issue.AwaitType` | `None` |
| `gate_await_id` | `Option<String>` | `Lww` | `Issue.AwaitID` | `None` |
| `gate_waiters` | `OrSet<MailAddress>` | `OrSet` (union) | `Issue.Waiters` | empty |

### CRDT merge behavior

- All scalar fields use `Lww`. Concurrent writes resolve by
  `(WriteStamp, ActorId)` tiebreak per `crdt.rs`.
- `gate_waiters` uses `OrSet` so two replicas adding different waiters
  converge to the union. This matches Go's Go-side use of `Waiters` as an
  accretive list (new watchers subscribe; there's no "winner" semantics).
- `gate_outcome` `Lww` is correct but reveals a subtle semantic: if two
  replicas evaluate concurrently, the later-stamped outcome wins and the
  other is lost. gascity's Go path serializes evaluation through a single
  dispatcher so this doesn't arise there; in beads-rs's CRDT world, the
  apply layer should log a warning when a stamp conflict drops a
  non-`Pending` outcome.

## Enum variants

```rust
pub enum GateKind {
    Convergence, // convergence-loop gate (gascity primary surface)
    Await,       // await-style gate (Go bd older surface)
}

pub enum GateMode {
    Manual,    // human approves
    Condition, // script determines
    Hybrid,    // script first, manual if script unavailable
}

pub enum GateOutcome {
    Pending,  // not yet evaluated
    Pass,     // condition satisfied
    Fail,     // condition rejected
    Timeout,  // evaluation exceeded gate_timeout
    Error,    // evaluation errored (distinct from Fail)
}

pub enum GateTimeoutAction {
    Iterate,   // re-enter convergence loop
    Retry,     // retry the gate up to MaxGateRetries
    Manual,    // escalate to human approval
    Terminate, // end the parent workflow with no_convergence
}

pub enum AwaitType {
    GhRun,  // GitHub Actions run
    GhPr,   // GitHub pull request
    Timer,  // wall-clock timer
    Human,  // explicit human close
    Mail,   // mail message arrival
    Bead,   // another bead reaching a state
}
```

Provenance:

- `GateMode` — gascity `internal/convergence/metadata.go:55-59`.
- `GateOutcome` — gascity `metadata.go:78-83`.
- `GateTimeoutAction` — gascity `metadata.go:62-67`.
- `AwaitType` — Go `internal/types/types.go:93` comment, refined by
  `cmd/bd/close_gate_test.go:32-72` which enumerates
  `{human, gh:pr, gh:run, timer, bead}`; the comment at `types.go:93`
  also lists `mail`.
- `GateKind` — synthesized; distinguishes the two gate surfaces so parsing
  rules and default fields depend on the kind.

## Lifecycle + invariants

- Workflow: `Open → InProgress → Closed`.
- Outcome transitions:
  - `Pending → { Pass | Fail | Timeout | Error }` via evaluation.
  - A `Fail` may retry up to `MaxGateRetries` before becoming terminal.
  - A `Timeout` triggers `gate_timeout_action`, which may transition the
    PARENT molecule, not the gate itself.
- **Invariant**: `gate_kind == Convergence` requires `gate_mode != None`
  and EITHER `gate_condition.is_some()` OR `gate_mode == Manual`. Enforced
  at the apply boundary.
- **Invariant**: `gate_kind == Await` requires `gate_await_type.is_some()`.
  Without it, the gate can never satisfy.
- **Invariant**: when `workflow == Closed`, `gate_outcome` must be one of
  `Pass | Fail | Timeout | Error`, never `Pending`.
- **Ready-exclusion**: YES. Excluded by gascity's `readyExcludeTypes`
  (`internal/beads/beads.go:86`). Gates are wait conditions, not work.
- **Container behavior**: none. A gate is a leaf in the work graph.

## Dependencies

- As target:
  - `DepKind::Blocks` from a step that should wait on this gate.
  - `DepKind::Parent → <molecule-id>` when the gate is a step inside a
    convergence molecule.
- As source:
  - Typically none. The gate's outcome affects parent molecule state via
    the convergence evaluator reading `gate_outcome`, not via an outbound
    dep.

## Wire shape

```json
{
  "id": "bd-gate3",
  "issue_type": "gate",
  "title": "tests must pass",
  "description": "...",
  "status": "closed",
  "priority": 2,
  "workflow": { "state": "closed", "reason": "pass" },
  "claim": { "state": "unclaimed" },

  "gate_kind": "convergence",
  "gate_mode": "condition",
  "gate_condition": "scripts/check-tests.sh",
  "gate_timeout": "5m",
  "gate_timeout_action": "retry",
  "gate_outcome": "pass",
  "gate_exit_code": 0,
  "gate_retry_count": 1,
  "gate_stdout": "ok.\n",
  "gate_stderr": "",
  "gate_duration": "1m23s",
  "gate_truncated": false,

  "gate_await_type": null,
  "gate_await_id": null,
  "gate_waiters": [],

  "labels": [],
  "dependencies": [
    { "kind": "parent", "target": "bd-mol-abc" }
  ],
  "created": { ... },
  "updated_stamp": { ... },
  "content_hash": "..."
}
```

Type-specific: every field prefixed `gate_*`. All convergence fields map
to `metadata["convergence.*"]` keys from gascity — see
`primitives/metadata-remapping.md`.

## Parity status

- **Rust source**: not implemented. Add `BeadType::Gate` plus
  `GateFields` and the enum types above.
- **Gap vs Go bd / gascity**:
  - Go bd stores `AwaitType`, `AwaitID`, `Timeout`, `Waiters` on the Issue
    struct itself. beads-rs stores them as gate-type-specific fields; a
    bead with `issue_type != "gate"` does not carry them.
  - gascity stores convergence gate state in metadata. beads-rs lifts to
    typed fields. Every `convergence.gate_*` key has a typed home above.
- **Migration**: import pass reads metadata keys and populates typed
  fields. The metadata keys are cleared post-migration.

## Revisit

- `gate_kind` as a discriminator is slightly awkward — it turns a single
  `Gate` type into a sum of two "kinds". The honest alternative is two
  types, `BeadType::ConvergenceGate` and `BeadType::AwaitGate`. Gascity's
  usage clearly tends toward convergence; Go's await-gate may be phasing
  out. Decision: keep as one type with a discriminator for now; if the
  two shapes diverge further, split.
- Should `gate_stdout`/`gate_stderr` be `Lww<Option<String>>` or
  content-hash-immutable with a stamp pointing at the run? Today Go
  overwrites them per evaluation. `Lww` is correct — evaluation is a
  single-author operation; stamp comparison decides the winner.
