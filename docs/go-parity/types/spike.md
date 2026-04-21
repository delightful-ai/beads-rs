# spike

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**Namespace:** core

**Aliases:** `investigation`, `timebox` (Go `types.go:593-594`, `Normalize()`).

## Purpose

A timeboxed investigation: "we don't know enough to commit to a design —
spend N hours exploring". The output is a finding, not a shipped behavior
delta. Enforced section hints (Go `types.go:635-638`):

- `## Goal` — what question does this spike answer?
- `## Findings` — what was learned?

Gascity does not emit `spike` beads programmatically; humans create them to
scope exploratory work before committing to a formula or feature.

Distinguishing feature vs adjacent:

- `decision` — choice made after the spike.
- `task`/`feature` — committed work with a defined outcome. `spike` has an
  open-ended outcome (a finding).

## Typed field set

A spike's defining trait is its timebox. Go has no typed timebox field
today (it leans on `estimated_minutes` and the recommended-sections hint).
beads-rs can be slightly stronger by making the timebox explicit:

```rust
pub struct SpikeFields {
    /// Wall-clock deadline the spike must not exceed. When the deadline
    /// passes, `bd ready` surface should flag overdue spikes.
    pub spike_deadline: Lww<Option<WallClock>>,

    /// Outcome state — filled in when the spike concludes.
    pub spike_outcome: Lww<SpikeOutcome>,
}
```

| Field | Type | CRDT | Source | Default |
|---|---|---|---|---|
| `spike_deadline` | `Option<WallClock>` | `Lww` | New in beads-rs. Go uses `estimated_minutes` loosely. | `None` (no timebox) |
| `spike_outcome` | `SpikeOutcome` | `Lww` | New; Go encodes in close-reason text. | `Pending` |

Concurrent-write behavior: `Lww` last-stamp-wins with actor tiebreak, same
as every other scalar field.

## Enum variants

```rust
pub enum SpikeOutcome {
    Pending,      // spike still open
    Answered,     // question resolved, findings in description
    Inconclusive, // timebox expired without resolution
    Abandoned,    // no longer worth pursuing
}
```

Provenance: these are synthesized from close-reason text currently
classified by `FailureCloseKeywords` (Go `types.go:881-910`). No Go
enum exists. Making this a typed enum is a beads-rs upgrade — the
alternative is parsing free-form close reasons, which violates
"types tell the truth".

## Lifecycle + invariants

- Workflow: `Open → InProgress → Closed`. `spike_outcome` transitions:
  - `Pending` while `workflow != Closed`.
  - On `Closed`, must be one of `Answered | Inconclusive | Abandoned`.
  - Enforce in apply layer: closing a spike without setting outcome emits
    a validation error.
- **Ready-exclusion**: NOT excluded. Spikes are actionable.
- **Container behavior**: none. A spike that needs sub-exploration should
  be a `molecule` or `epic` instead.
- **Overdue detection**: if `spike_deadline < now() && workflow != Closed`,
  surface via `bd ready --overdue`.

## Dependencies

- Source: rarely.
- Target: a `decision` or `feature` that followed from the spike should carry
  `DepKind::DiscoveredFrom → <spike-id>`.
- `DepKind::Related` across adjacent spikes.

## Wire shape

```json
{
  "id": "bd-sp12",
  "issue_type": "spike",
  "title": "Investigate CRDT merge perf",
  "description": "...",
  "status": "closed",
  "priority": 2,
  "workflow": { "state": "closed", "reason": "timebox exhausted" },
  "claim": { "state": "unclaimed" },
  "spike_deadline": 1713628800000,
  "spike_outcome": "inconclusive",
  "labels": [],
  "dependencies": [],
  "created": { ... },
  "updated_stamp": { ... },
  "content_hash": "..."
}
```

Type-specific: `spike_deadline`, `spike_outcome`.

## Parity status

- **Rust source**: not implemented. Add `BeadType::Spike` with aliases.
- **Gap vs Go bd**: Go has no typed deadline / outcome. beads-rs would be
  stricter. Interop: Go clients still read `issue_type: "spike"` correctly;
  the new fields appear as unknown keys (forward-compatible). Importing Go
  beads with `issue_type: "spike"` should default `spike_outcome = Pending`
  if open, infer from close-reason text if closed.
- **Migration**: existing custom-typed spikes migrate by setting
  `spike_outcome = Pending` (if open) or running the close-reason classifier
  (if closed). Explicit one-shot migration command.
