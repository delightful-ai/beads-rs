# epic

**Pin:** v1.0.2, c446a2ef

**Parity status:** `extended` (ships today as `faithful`, will extend once

**Namespace:** core
molecule semantics land)

## Purpose

A container bead: groups related work items under a single umbrella. Children
attach via `DepKind::Parent`. In Go, `epic` gets special treatment in
`bd ready` (`IssueDetails.EpicTotalChildren/ClosedChildren/Closeable` at
Go `types.go:759-761`) — when all children are closed, the epic is
"eligible for closure".

gascity uses `epic` interchangeably with `molecule` for workflow roots when
the workflow is a hand-authored project plan rather than a formula
instantiation. The `molecule` type in gascity is reserved for formula-derived
containers (see `molecule.md`).

## Typed field set

Baseline `BeadFields`, plus derived counters surfaced in JSON but not stored:

```rust
// Derived, not a Lww field — computed from children at read time.
// See BeadProjection in crates/beads-core/src/bead.rs.
pub epic_total_children:  Option<u32>,   // count of parent-child children
pub epic_closed_children: Option<u32>,   // count where workflow=Closed
pub epic_closeable:       Option<bool>,  // total > 0 && closed == total
```

- **CRDT behavior**: these are projections. No merge — each replica computes
  them from the current dep + workflow view. No stamp.
- **Source (Go)**: `Issue.EpicTotalChildren/ClosedChildren/Closeable` at
  `types.go:759-761`.
- **Required at creation**: N/A (derived).

If gascity's epic-as-workflow-root needs grow to require a `MolType` (swarm |
patrol | work), that field is owned by `molecule.md`, not here. Keep `epic`
pure-container.

## Enum variants

None introduced. `MolType` stays in `molecule.md`.

## Lifecycle + invariants

- Workflow transitions identical to `task`.
- **Auto-closure rule** (Go, optional): when `epic_closeable == true`, CLI
  may offer to close. Not an invariant; a suggestion.
- **Ready-exclusion**: NOT excluded. An epic with no children is itself
  actionable ("plan this epic").
- **Container behavior**: YES. Children attach via `DepKind::Parent` where
  target = epic-id, source = child-id. Deep descendants are traversed via
  `WorkFilter.ParentID` with recursive semantics (Go `types.go:1296`).

## Dependencies

- Source: rarely; an epic can `Blocks` another (sequencing two phased epics).
- Target: children point at the epic via `DepKind::Parent`.
- `DepKind::Related` cross-links between epics.

## Wire shape

```json
{
  "id": "bd-epic1",
  "issue_type": "epic",
  "title": "Port CRDT substrate",
  "description": "...",
  "status": "open",
  "priority": 1,
  "workflow": { "state": "open" },
  "claim": { "state": "unclaimed" },
  "labels": [],
  "dependencies": [],
  "epic_total_children": 12,
  "epic_closed_children": 7,
  "epic_closeable": false,
  "created": { ... },
  "updated_stamp": { ... },
  "content_hash": "..."
}
```

Type-specific fields: `epic_total_children`, `epic_closed_children`,
`epic_closeable`. All other fields shared with `task`.

## Parity status

- **Rust source**: `crates/beads-core/src/domain.rs::BeadType::Epic` (ships).
  Derived counters NOT implemented.
- **Gap vs Go bd**:
  - Derived counters (`epic_total_children`/`_closed_children`/`_closeable`) —
    beads-rs should compute them in `BeadProjection::from_view` or a
    sibling helper, pulling children from the dep store. Not stored; not
    merged.
  - Auto-closure CLI hint: surface-layer concern, not core.
- **Migration**: existing epic beads round-trip; new derived fields just
  appear in output.
