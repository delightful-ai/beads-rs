# Bead types

Per-variant spec files for beads-rs, pinned to the tag in [`../README.md`](../README.md) (Go v1.0.2, c446a2ef).

Each file captures:

- The Go type string (the `issue_type` JSON field value), if any.
- Type-specific fields, enums, and invariants.
- Lifecycle states unique to the variant.
- Dependency-edge kinds the variant interacts with.
- The proposed Rust parity shape.

All variants are classified by **namespace placement** per [`../DECISION_LOG.md`](../DECISION_LOG.md) §D3. The namespace determines sync, retention, visibility, and cross-reference semantics (see [`../primitives/namespaces.md`](../primitives/namespaces.md)).

## Namespace: `core`

The mainline namespace. Synced to git, persistent retention, default visibility. Every baseline bead type and every workflow/orchestration type lives here.

### Baseline types (already in Rust)

- [`task`](task.md) — the default; canonical template
- [`bug`](bug.md)
- [`feature`](feature.md) — aliases: `enhancement`, `feat`
- [`epic`](epic.md)
- [`chore`](chore.md)

### Go v1.0.0 additions

- [`decision`](decision.md) — aliases: `dec`, `adr`
- [`spike`](spike.md) — investigation/exploration
- [`story`](story.md) — aliases: `user-story`, `user_story`
- [`milestone`](milestone.md) — date-marked milestones

### Orchestration (gascity-critical)

- [`molecule`](molecule.md) — workflow template instance; typed `mol_type` (swarm/patrol/work)
- [`convoy`](convoy.md) — container of children; dispatch fan-out
- [`gate`](gate.md) — async coordination gate; `GateKind` discriminator (convergence vs await)
- [`message`](message.md) — mail-style inter-agent communication with threading
- [`merge-request`](merge-request.md) — VCS-driven external integration
- [`event`](event.md) — append-only event-bus entries

### Identity / config

- [`role`](role.md) — agent role/persona identity
- [`agent`](agent.md) — agent runtime identity (NOTE: distinct from session-runtime state; see Sessions namespace below)
- [`rig`](rig.md) — city namespace / store boundary identity
- [`slot`](slot.md) — per-issue slot (claim-adjacent; see `primitives/metadata-remapping.md` for slot vs claim reconciliation)

### Workflow control (from Agent 1's gascity subsystem decomposition)

These are Agent 1's proposed decomposition of gascity's internal orchestration beads — not yet one-file-per-variant under `types/`. Each today shares `type: "task"|"molecule"` in Go and discriminates via `metadata["gc.kind"]` strings. The canonical spec for each is in [`../primitives/metadata-remapping.md`](../primitives/metadata-remapping.md) §A.2 and §A.5–A.6. Future work: each gets its own `types/*.md` file as it's promoted from the metadata-remapping discussion.

- `WorkflowRoot` — outermost latch; `gc.kind="workflow-finalize"`
- `Scope(ScopeSpec)` — coherent work unit; latch
- `ScopeCheck` — scope completion evaluator
- `ScopeCleanup` — scope teardown (`gc.kind="cleanup"`)
- `Ralph(RalphSpec)` — iterative-retry controller
- `RalphAttempt(AttemptSpec)` — one attempt within a Ralph loop
- `Retry(RetrySpec)` — full-reset retry controller
- `RetryRun(AttemptSpec)` — one execution within a retry chain
- `RetryEval` — retry outcome evaluator
- `FanoutControl` — parallel-fanout controller

### Coordination (durable, core-namespaced)

Per D3, these are durable coordination state, not runtime-adjacent — they stay in `core`.

- `Group` — a named group of agents/beads (pool: polecats, mayors)
- `Membership` — agent-to-group join record
- `Participant` — member of a specific session/conversation

## Namespace: `sessions`

Session-lifecycle state. Runtime-adjacent; the process itself lives in gascity's provider (tmux/k8s/subprocess), these beads hold identity and durable checkpoints. Needs [`../primitives/namespaces.md`](../primitives/namespaces.md) **cross-namespace DepKey fix** before beads here can reference `core` workflow beads.

Spec lives in [`../primitives/metadata-remapping.md`](../primitives/metadata-remapping.md) §A.8. Promote to individual `types/*.md` files when the sessions namespace ships.

- `Session` — running agent-process identity (awake/held/quarantined/dead)
- `Wait` — durable "session is paused waiting for X"
- `Nudge` — queued prompt to an agent
- `TranscriptEntry` — append-only session activity log line

**Open question:** should `TranscriptEntry` sync to the core remote, or stay local-to-machine? See `namespaces.md` §2a.

## Namespace: `extmsg`

External-messaging bookkeeping. Bindings and deliveries need durability + cross-clone sync but are semantically distinct from the workflow graph — they're the agent-to-bead registry. Needs the cross-namespace DepKey fix.

Spec lives in [`../primitives/metadata-remapping.md`](../primitives/metadata-remapping.md) §A.11.

- `Binding` — agent-to-bead work-assignment record (survives crashes)
- `Delivery` — proof-of-delivery for mail

## Namespace: `wisps`

Ephemeral/GC-eligible beads. Retention is a namespace-level policy driven by each bead's `RetentionClass`. **Ships after the GC sweeper lands** — see [`../DECISION_LOG.md`](../DECISION_LOG.md) Floor 4.

- [`wisp`](wisp.md) — NOT a type variant; `Ephemerality` field + `wisps` namespace placement. See the file for the decision record.

Any `BeadType` can land in the wisps namespace if its `Ephemerality = Ephemeral(RetentionClass)`. Common ephemeral bead types: `Event` (heartbeat/patrol reports), `Agent` (liveness pings), `Task` (one-shot recovery records).

## Ready-exclusion set

Types filtered from `bd ready` by default (matching gascity's `internal/beads/beads.go:84-93`):

- `merge-request` (automation-driven)
- `gate` (async wait conditions)
- `molecule` (workflow containers)
- `message` (communication, not work)
- `session` (runtime identity; also not in `core`)
- `agent` (identity tracking)
- `role` (persona definitions)
- `rig` (rig identity)

beads-rs mirrors this set. Tracked in `primitives/ready-filter.md` (TBD).

**Open question from `event.md`:** should `Event` be ready-excluded in beads-rs even though gascity doesn't exclude it today? beads-rs leans yes; gascity ports may need to opt out.

## Parity status legend

Each file carries a `**Parity status:**` line:

- `faithful` — beads-rs shape matches Go's equivalent.
- `extended` — beads-rs preserves Go's surface and adds typed fields.
- `simplified` — intentionally narrower than Go.
- `deferred` — on the port list, target shape documented, not yet implemented.
- `declined` — explicitly won't port (see per-file reason).

## Cross-references

- [`../DECISION_LOG.md`](../DECISION_LOG.md) — decision log; namespace assignments (D3); wisp GC-eligibility (D4); prereq graph.
- [`../primitives/namespaces.md`](../primitives/namespaces.md) — namespace feature state and gaps.
- [`../primitives/metadata-inventory.md`](../primitives/metadata-inventory.md) + [`../primitives/metadata-remapping.md`](../primitives/metadata-remapping.md) — every gascity metadata key, with proposed typed home. Authoritative spec for workflow-control, sessions, and extmsg variants above.
- [`../primitives/dep-kinds.md`](../primitives/dep-kinds.md) — dep-edge kind enumeration; which kinds each type uses.
- [`../cmds/gascity/`](../cmds/gascity/README.md) — CLI per-command backlog for the 14 commands gascity calls.
