# milestone

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**Namespace:** core

**Aliases:** `ms` (Go `types.go:597-598`, `Normalize()`).

## Purpose

A checkpoint marker: "when this list of work is closed, we've hit this
milestone". A `milestone` does no work itself — it names a boundary.
Functionally similar to `epic` (container with derived progress), but
semantically a milestone is a schedule marker, not a work breakdown — it
doesn't imply its children were planned together.

Go's `RequiredSections` for `milestone` is empty (`types.go:640-643`,
falls through to `default`). Gascity does not create milestones
programmatically.

Distinguishing feature vs adjacent:

- `epic` — planned work breakdown. `milestone` — checkpoint across
  potentially unrelated work items.
- `decision` — record of a choice. `milestone` — record of a delivery point.
- `convoy` — group of items to merge together. `milestone` — group of items
  to complete by a date.

## Typed field set

```rust
pub struct MilestoneFields {
    /// Target date for this milestone. Nullable — milestones can be
    /// open-ended ("when we're ready to call v2.0").
    pub milestone_target: Lww<Option<WallClock>>,

    /// Whether hitting this milestone was declared complete. Derived in
    /// Go from workflow=closed, but storing it typed prevents confusion
    /// between "closed because abandoned" and "closed because hit".
    pub milestone_outcome: Lww<MilestoneOutcome>,
}
```

| Field | Type | CRDT | Source | Default |
|---|---|---|---|---|
| `milestone_target` | `Option<WallClock>` | `Lww` | New. Go uses `DueAt` at `types.go:49`. beads-rs can reuse `DueAt` if/when adopted as a generic scheduling field; until then, store here. | `None` |
| `milestone_outcome` | `MilestoneOutcome` | `Lww` | New; Go infers from close-reason. | `Pending` |

Concurrent-write: `Lww` stamp-based, standard resolution.

Note on `milestone_target` vs a global `due_at`: see
`primitives/scheduling.md` (TBD). If beads-rs adopts Go's `DueAt`/`DeferUntil`
as top-level `BeadFields` for ALL types, this field becomes redundant —
delete it then. Do not create two channels for the same concept.

## Enum variants

```rust
pub enum MilestoneOutcome {
    Pending,   // milestone not yet reached
    Hit,       // all children closed by the target date, milestone declared done
    Missed,    // target date passed, milestone closed as abandoned
    Slipped,   // target date moved; see updated milestone_target
}
```

Provenance: synthesized. No Go enum. Alternative is parsing close-reason,
which is lossier. "Slipped" is distinguishable from "pending" because the
target was reset, not simply unmet.

## Lifecycle + invariants

- Workflow transitions: `Open → Closed`. Rarely `InProgress` (a milestone
  doesn't have work of its own).
- Outcome transitions:
  - `Pending → Hit` when workflow closes "on time" (all children closed,
    `<= milestone_target`).
  - `Pending → Missed` when workflow closes after `milestone_target` with
    outstanding children.
  - `Pending → Slipped` when `milestone_target` is moved to a later date.
  - On close, outcome must be `Hit` or `Missed`. `Slipped` is a transient
    open-milestone state.
- **Ready-exclusion**: NOT excluded. A milestone is a valid planning item.
  (Though rarely surfaces in `bd ready` because there's no work to do
  on the milestone itself until closing time.)
- **Container behavior**: SEMI. Children can attach via `DepKind::Parent`,
  or the milestone can aggregate via `DepKind::Related` edges (members-of
  semantic). Prefer `DepKind::Parent` for consistency with `epic`.

## Dependencies

- Target: work items aimed at the milestone use `DepKind::Parent →
  <milestone-id>`.
- `DepKind::Related` cross-links to other milestones on the same timeline.

## Wire shape

```json
{
  "id": "bd-ms01",
  "issue_type": "milestone",
  "title": "v2.0",
  "description": "...",
  "status": "open",
  "priority": 2,
  "workflow": { "state": "open" },
  "claim": { "state": "unclaimed" },
  "milestone_target": 1735689600000,
  "milestone_outcome": "pending",
  "labels": [],
  "dependencies": [],
  "created": { ... },
  "updated_stamp": { ... },
  "content_hash": "..."
}
```

Type-specific: `milestone_target`, `milestone_outcome`.

## Parity status

- **Rust source**: not implemented. Add `BeadType::Milestone` with alias `ms`.
- **Gap vs Go bd**: Go has no typed target / outcome; beads-rs is stricter.
  Interop: Go callers see the extra fields as unknown and ignore; beads-rs
  sees `issue_type: "milestone"` from Go and defaults both new fields.
- **Migration**: existing custom-typed milestones initialize outcome from
  close-reason classification (fail keywords → `Missed`, else → `Hit`).

## Revisit

- Decide whether `milestone_target` should be folded into a shared
  `primitives/scheduling.md` `due_at` field. If yes, delete
  `milestone_target` here and cite the primitive. Do not duplicate.
