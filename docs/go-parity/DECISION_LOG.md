# Go Parity — Decision Log

This file is the synthesis across the four research streams that run under `docs/go-parity/`. Read this before diving into the per-command, per-type, or per-primitive files. When this log conflicts with a detail file, fix the conflict — don't leave it implicit.

**Pin:** Go `bd` v1.0.2 (`c446a2ef`, 2026-04-18) vs. beads-rs at current HEAD (`bd 0.2.0-alpha`).
**Floor 1 replan:** [`FLOOR1_REPLAN.md`](FLOOR1_REPLAN.md) updates the Floor 1 execution direction against the later local vendor checkouts: Go `bd` v1.0.2 at `a3f834b3` and Gas City `main` at `d6011764`.
**Research date:** 2026-04-20.

## Research outputs

| Stream | File(s) | One-line summary |
|--------|---------|------------------|
| CLI surface + wire | [`cmds/gascity/`](cmds/gascity/README.md) (14 cmd files + envelope + summary) | Every command gascity calls, flag-by-flag + live JSON, with per-command backlog |
| Dependency kinds | [`primitives/dep-kinds.md`](primitives/dep-kinds.md) | Go has 19 dep kinds; Rust has 4; gascity uses 8 (+4 to add) |
| Metadata → typed homes | [`primitives/metadata-inventory.md`](primitives/metadata-inventory.md) + [`metadata-remapping.md`](primitives/metadata-remapping.md) | ~250 keys → ~30 typed homes; 2 Revisits; 0 freeform primitive needed |
| Bead type variants | [`types/*.md`](types/) (20 files) | One file per variant; Wisp is `Ephemerality` + namespace property, not a variant |
| Namespaces | [`primitives/namespaces.md`](primitives/namespaces.md) | Feature is skeleton-real: partitioning wired, policy enforcement mostly unenforced |

## Headline decisions (locked in)

### D1. No freeform metadata in beads-rs

The metadata inventory mapped ~250 distinct keys gascity uses. **Only 2 `Revisit` entries needed a "maybe freeform" fallback, neither arguing for a generic primitive.** Every other key has a typed home — typed field on a typed bead variant, namespace property, label, dep-edge state, runtime-state (not in store), or derivable from existing beads-rs primitives.

Implication: `bd create --metadata`, `bd update --set-metadata`, `bd list --metadata-field` all get typed homes per bead variant or edge type. The current Gas City checkout still calls those metadata-shaped CLI surfaces directly, so beads-rs needs an adapter projection that accepts and emits the map while preserving typed canonical state internally.

Refinement from the later Gas City checkout: the current `BdStore` still writes and reads metadata directly, and the values are not arbitrary baggage. They include durable workflow/control, convergence, session, graph-apply, idempotency, and query state. Until Gas City consumes typed fields, beads-rs needs a CLI/wire projection that accepts and emits the current metadata map while mapping known keys into typed domain fields. That projection is an adapter contract, not a domain-level `HashMap<String, String>`.

### D2. Adopt Go's CLI surface wholesale

User decision: rather than preserving beads-rs's existing flag shapes, match Go's. Concretely:

- Drop the `{"result":"issue","data":{...}}` wrapper for gascity-shaped commands; emit bare objects/arrays like Go.
- Serde-rename `"type"` → `"issue_type"` on the wire.
- Preserve current Gas City's `"assignee"` and `"from"` fields on the Gas City-shaped wire. Older notes that renamed this field to `"owner"` are stale for the latest local Gas City checkout.
- Emit timestamps as RFC3339 strings on the wire (keep HLC `WriteStamp` as the internal ordering primitive).
- Flatten `deps_incoming`/`deps_outgoing` into Go's `dependencies[]`/`dependents[]` hydrated-issue shape on the wire.
- Rename `bd dep add --kind` → `--type`; `bd dep rm` → `bd dep remove` (keep `rm` as alias).
- Suppress `receipt.durability_proof` on default mutation output; gate behind `--receipt` flag.

The wire-shape layer is where Go-compat lives. The domain model stays richly typed.

### D3. Namespaces hold cross-cutting state, not types of work

User decision, informed by `primitives/namespaces.md`:

- **Sessions namespace** hosts `Session, Wait, Nudge, TranscriptEntry` — session lifecycle is runtime-adjacent, not core workflow.
- **Extmsg namespace** hosts `Binding, Delivery` — external-messaging bookkeeping distinct from workflow graph.
- **Wisps namespace** hosts anything GC-eligible by policy. Retention is a namespace property, not a per-bead flag.
- Mainline (`core`) namespace hosts: all workflow control types (Workflow, Scope, ScopeCheck, ScopeCleanup, WorkflowFinalize, Ralph, RalphAttempt, Retry, RetryRun, RetryEval, FanoutControl), plus Convoy, Gate, Message, MergeRequest, Event, Group, Membership, Participant, and the baseline types (Task, Bug, Feature, Epic, Chore, Decision, Spike, Story, Milestone, Role, Agent, Rig, Slot).

### D4. Wisps are GC-*eligible*, not GC-*default*

User refinement to `types/wisp.md`: an ephemeral bead survives until the retention policy fires. The `Ephemerality` tag on common `BeadFields` is a hint to the GC sweeper, not a deletion gate at creation. Wisp namespace policy declares TTL/compaction behavior; individual beads inherit.

### D5. Bead-type proliferation is real and worth paying for

Agent 1's decomposition of gascity's internal subsystems ( `metadata-remapping.md` §A.2, A.5–A.11) identifies ~21 distinct workflow bead kinds that today share `type: "task"|"molecule"` and discriminate via `metadata["gc.kind"]` strings. Each deserves its own typed variant because their state structs genuinely differ — a `Scope` has body+check+cleanup refs; a `Ralph` has iteration counters + attempt log; a `FanoutControl` has spawn spec + gate mode. These aren't variants-of-one-thing; they're siblings.

### D6. Reconciliation: enum granularity decided variant-by-variant

Agent 1 and Agent 2 disagreed about some groupings (Agent 1 has `ConvergenceRoot` and 4 session variants; Agent 2 has one `Gate` with `GateKind` discriminator and one `Session` variant). Decision: pick per-case, not dogmatically.

- Orchestration (Scope, Ralph, Retry, Fanout): **Agent 1's multi-variant decomposition wins.** Their spec structs are genuinely different.
- Gate, Session: **Agent 2's single-variant-with-discriminator wins.** Common state outweighs per-kind specialization.

### D7. Several metadata keys just vanish, but control state does not

Things that don't need a home — they're derivable:

- `gc.routed_to` / `gc.execution_routed_to` → computed at dispatch time from `gc.run_target` + agent-binding table (Go literally does this, then writes the result back).
- `from` metadata fallback → `BeadCore::created.by`.
- Auto-generated `session_name` → deterministic from bead ID.
- `BindingFields::last_touched_at` → `BeadView::updated_stamp`.

`gc.control_epoch` is not in this bucket. Latest Gas City uses it as an explicit expected-epoch fence around molecule/control attachment; it belongs with typed workflow/control state until the store has an equivalent first-class fenced update primitive.

## Open questions

### OQ1. Namespace cross-references are structurally orphaned

`Bead` doesn't carry a namespace field (it's on the event); `DepKey` doesn't either. Cross-namespace deps are silently allowed but graph traversal in one namespace can't see beads in another. This is the **real blocker** for sessions + extmsg namespaces, not the policy-enforcement gaps.

Proposed fix (not yet specced): add `namespace: NamespaceId` to `DepKey`, propagate through `CanonicalState` traversal APIs. Probably a small change, but it's structural and load-bearing. Needs design work before any variant goes into a non-`core` namespace.

### OQ2. Namespace policy enforcement is ~90% skeleton

Only `replicate_mode` is consumed by runtime behavior. `persist_to_git`, `retention`, `visibility`, `ready_eligible`, `gc_authority`, `ttl_basis` are defined, parsed, diffed, and read by zero production paths. Enforcing each is a separate chunk of work that must land before the corresponding namespace policy means anything.

### OQ3. Wisps need a GC sweeper (new feature)

`primitives/namespaces.md` flags the retention GC sweeper as a hard prerequisite for the wisps namespace. It's a medium-sized new feature (periodic sweep, TTL evaluation, durable progress tracking, cross-replica coordination). Without it, wisps in their own namespace pile up forever.

### OQ4. Revisit items from metadata-remapping.md

Two keys didn't land a clean typed home:

- `gc.source_step_spec` — serialized formula step spec for retry re-expansion. Needs a versioned opaque blob with schema version. Likely a typed immutable field on `RetryRun`/`RalphAttempt`, not freeform — but the schema version concern is real.
- "Not enough source reading" bundle — agent flagged specific files that need another pass before a decision is safe.

## Prerequisite graph

Work order constraint — higher floors depend on lower floors.

```
Floor 0 (no prereqs, ship alongside anything):
  CLI Go-compat work
  ├── bd init: tolerate no-remote + accept gascity's flag set
  ├── bd dep add: rename --kind → --type (keep --kind alias)
  ├── bd dep rm: alias to bd dep remove
  ├── bd ready: emit bare array (fix envelope for this cmd specifically)
  ├── Default mutation output: suppress receipt.durability_proof (add --receipt opt-in)
  └── Wire envelope drop + field projection (issue_type, assignee/from, RFC3339)

Floor 1 (foundations; refined by FLOOR1_REPLAN.md):

  Actual BeadType variants
    prereq for: any new variant, --type=<known> accept
    work: add real Rust variants for known Go/Gas City types; normalize Go aliases (enhancement→feature, dec/adr→decision, investigation/timebox→spike, user-story/user_story→story, ms→milestone)
    note: types.custom is a Gas City doctor compatibility facade, not the canonical source of truth

  DepKind vocabulary
    prereq for: Session (waits-for), MergeRequest (tracks), several others
    work: add the full Go well-known vocabulary; readiness-affecting kinds are blocks, parent-child, conditional-blocks, waits-for

  Metadata projection inventory
    prereq for: typed variant fields and current Gas City drop-in use
    work: map create/update/list/show metadata inputs and outputs to typed field homes; keep metadata only as CLI/wire projection

  CRDT primitives driven by typed state machines
    prereq for: variants with concurrent state (Ralph, Retry, Scope, Session, ...)
    work: implement Max<T>, AppendLog<T>, Cas<T>, Counter as the merge laws required by concrete fields such as convergence iteration, control_epoch, attempt logs, pending_create_claim, and resettable session counters
    note: OrSet<T> exists; verify it fits the new callers

  Cross-namespace DepKey fix (OQ1)
    prereq for: sessions namespace, extmsg namespace
    work: add namespace to DepKey; propagate through CanonicalState traversal

  Namespace policy enforcement (OQ2)
    prereq for: wisps namespace (needs retention), sessions namespace (maybe needs visibility)
    work: wire persist_to_git, retention, visibility, ready_eligible into actual runtime paths

  Missing CLI commands (independent)
    ├── bd config family (set, get, get --json for types.custom)
    ├── bd purge (or explicitly decline)
    └── bd dep list (+ batch form)

Floor 2 (depends on Floor 1):

  Core-namespace workflow variants
    prereq: actual BeadType variants + typed-field-driven CRDT primitives
    variants: Workflow, Scope, ScopeCheck, ScopeCleanup, WorkflowFinalize,
              Ralph, RalphAttempt, Retry, RetryRun, RetryEval, FanoutControl,
              Convoy, Gate (w/ GateKind discriminator), Message, MergeRequest,
              Event, Group, Membership, Participant,
              Decision, Spike, Story, Milestone, Role, Agent, Rig, Slot
    per-variant design: see types/*.md
    metadata remapping: see primitives/metadata-remapping.md for which Go keys become which typed fields

  Ephemerality on common BeadFields
    prereq: actual BeadType variants (so the field lands alongside type variants cleanly)
    work: add Lww<Ephemerality> to BeadFields; wire GC-eligibility hint

  bd create --graph batch
    prereq: core variants (needs to create all the types)
    work: parse JSON plan file, atomic batch create + dep wiring

Floor 3 (depends on Floor 2 + cross-ns DepKey fix):

  Sessions namespace
    prereq: cross-namespace DepKey (Floor 1) + Session/Wait/Nudge/TranscriptEntry variants (Floor 2)
    work: non-core namespace with its own retention/visibility policy; variants land there

  Extmsg namespace
    prereq: cross-namespace DepKey + Binding/Delivery variants
    work: non-core namespace; variants land there

Floor 4 (depends on namespace policy enforcement fully landed):

  GC sweeper (new feature)
    prereq: namespace policy enforcement (Floor 1) + TTL semantics on retention policy
    work: periodic sweep; TTL evaluation; durable progress tracking; cross-replica coordination

  Wisps namespace
    prereq: GC sweeper + Ephemerality field (Floor 2)
    work: namespace with policy { retention=TTL, gc_authority=single-replica }; beads land there
```

### What this order means in practice

Floor 0 work (wire + small CLI fixes) unblocks gascity-Go **today** — no structural changes, days of work total. After Floor 0 lands, gascity can at least parse beads-rs JSON without a shim.

Floor 1 foundations are where the real design pressure sits. Especially:
- **Actual BeadType variants** — every downstream variant depends on this; it's a core-crate change with downstream propagation, probably a week of careful work.
- **Cross-namespace DepKey** — structural, under-specified, needs design before code.
- **CRDT primitives** — not a primitive-first project, but also not speculative. Latest Gas City already proves monotonic counters/epochs, append logs, claim/lease state, and resettable counters; implement them through the owning typed fields and their proof tests.

Floor 2 is the bulk of the porting effort — one variant at a time, driven by the per-type spec files.

Floor 3 + 4 gate on specific pieces of Floor 1 being *fully* done, not skeleton.

## Out of scope (explicit)

These are not beads-rs concerns:

- **Gascity's orchestration logic** — `internal/molecule/`, `internal/formula/`, `internal/convergence/`, `internal/dispatch/`, `internal/sling/`, `internal/mail/`. If gascity gets rewritten in Rust, those become a separate crate (call it `gascity-rs`) that sits **above** beads-rs.
- **Go bd's orchestration command families** — `bd mol`, `bd formula`, `bd cook`, `bd gate`, `bd mail`, `bd swarm`, `bd merge-slot`. Gascity doesn't call these; they're Go bd's standalone user tooling. beads-rs does not need to port them.
- **Dolt-specific features** — `bd dolt push/pull`, `bd compact` (Dolt-history-specific form), `bd flatten`, `bd dolt auto-commit`, `bd --as-of <commit>`. beads-rs's CRDT/git engine has its own answers.
- **Tracker integrations** — `bd jira`, `bd linear`, `bd ado`, `bd github`, `bd gitlab`, `bd notion`. Not gascity-facing; future work if anyone wants them.

## Ground rules for future edits

- When porting work lands on a cmd/primitive/variant, update the corresponding spec file's parity status AND this log's open-questions / prereq section in the same change.
- Refreshing the Go pin (next Go release): follow `README.md` refresh procedure. If a new v1.X.Y feature affects a decision here, add a dated entry in a `## Pin refresh log` section at the bottom of this file.
- When a "Revisit" item from `metadata-remapping.md` resolves, move it from OQ4 to a specific decision block above.
- This file is the synthesis, not the source of truth for any single topic. When citing a decision, cite the detail file too.
