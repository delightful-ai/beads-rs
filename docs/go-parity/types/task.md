# task

**Pin:** v1.0.2, c446a2ef

**Parity status:** `faithful`

**Namespace:** core

## Purpose

The default issue type — a generic unit of actionable work. `task` has no
type-specific state: it exists so that the space of non-bug/non-feature/non-epic
work has a named home. In gascity, any bead written without an explicit
`Type` defaults to `task` (`internal/beads/beads.go` Store interface contract:
"default Type to 'task'"). `task` is the template every other variant in this
doc extends from.

Distinguishing feature vs adjacent variants:

- `feature` — user-visible capability delta. `task` is anything else.
- `chore` — repo hygiene with no behavior delta. `task` is behavior-bearing.
- `story` — user-story-shaped narrative. `task` has no narrative shape.
- `epic` — container of children. `task` is a leaf.

## Typed field set

No fields beyond the common `BeadFields`. `task` is the canonical shape of
"untyped work", so every type-specific extension is additive relative to this
one. The common field surface, reproduced for reference (see
`crates/beads-core/src/bead.rs::BeadFields`):

```rust
pub struct BeadFields {
    pub title:               Lww<String>,
    pub description:         Lww<String>,
    pub design:              Lww<Option<String>>,
    pub acceptance_criteria: Lww<Option<String>>,
    pub priority:            Lww<Priority>,
    pub bead_type:           Lww<BeadType>,
    pub external_ref:        Lww<Option<String>>,
    pub source_repo:         Lww<Option<String>>,
    pub estimated_minutes:   Lww<Option<u32>>,
    pub workflow:            Lww<Workflow>,
    pub claim:               Lww<Claim>,
}
```

All CRDT merges are `Lww` per-field. Concurrent writes deterministically
resolve via `(WriteStamp, ActorId)` comparison — see `crates/beads-core/src/crdt.rs`.

## Enum variants

None introduced. Relies only on pre-existing `BeadType::Task`.

## Lifecycle + invariants

- Workflow transitions: `Open → InProgress → Closed`, or `Open → Closed` directly.
  Governed by `crates/beads-core/src/composite.rs::Workflow`.
- No type-specific invariants beyond the workspace baseline.
- **Ready-exclusion**: NOT excluded. `task` is explicitly the default `bd ready`
  result shape (Go `readyExcludeTypes` in `gascity/internal/beads/beads.go:84-93`
  does not list `task`).
- **Container behavior**: does not contain children. A parent-child dep where
  target is a `task` is valid but the `task` does not change shape.

## Dependencies

As source or target, `task` interacts with every `DepKind`:

- `Blocks` — a task can block or be blocked by anything.
- `Parent` — a task can be a child of an `epic`, `story`, `convoy`, `molecule`.
- `Related` — informational.
- `DiscoveredFrom` — follow-up linkage.

No type-specific dep edges.

## Wire shape

```json
{
  "id": "bd-a3f2",
  "issue_type": "task",
  "title": "...",
  "description": "...",
  "design": null,
  "acceptance_criteria": null,
  "status": "open",
  "priority": 2,
  "workflow": { "state": "open" },
  "claim": { "state": "unclaimed" },
  "external_ref": null,
  "source_repo": null,
  "estimated_minutes": null,
  "labels": [],
  "dependencies": [],
  "created": { "at": { "wall_ms": 0, "counter": 0 }, "by": "actor" },
  "updated_stamp": { "at": { "wall_ms": 0, "counter": 0 }, "by": "actor" },
  "content_hash": "..."
}
```

All fields above are shared baseline. `task` adds nothing to the envelope.

## Parity status

- **Rust source**: `crates/beads-core/src/domain.rs::BeadType::Task` (ships).
- **Gap vs Go bd**: zero. Go's `TypeTask` is the default; beads-rs matches.
  Go additionally has top-level `SpecID`, `DueAt`, `DeferUntil`, `StartedAt`,
  `Owner`, `CreatedBy`, `ClosedBySession`, `SourceSystem`, `Pinned`, `IsTemplate`,
  and compaction metadata — those are cross-cutting additions, not
  task-specific. They belong in `primitives/` docs (scheduling, ownership,
  compaction) rather than here.
- **Migration**: no migration needed; every existing bead-typed `task` bead
  round-trips unchanged.
