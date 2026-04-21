# slot

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**Namespace:** core

**See also:** `primitives/slots.md` (TBD — the mechanism), `agent.md`.

## Purpose

A `slot` bead represents a per-bead assignment slot: a discrete,
nameable position that can hold a claim. Slots let a single work bead
model multi-party work (e.g., `code-review` with slots for
`author-response`, `primary-reviewer`, `secondary-reviewer`) without
subdividing the bead. When gascity's work-type upgrade from simple mutex
assignment to richer multi-assignee models completes, slots are the
atomic unit.

gascity registers `slot` as a required custom type
(`internal/doctor/checks_custom_types.go:21`). The sampled code does not
show programmatic slot creation — slot-as-bead-type coexists with
"Slots" as a generic mechanism on any bead. This file documents the type
(the bead variant); `primitives/slots.md` documents the mechanism (the
per-bead slot map).

Distinguishing feature vs adjacent:

- `primitives/slots.md` — per-bead slot map mechanism (slots as fields on
  any bead). `slot` — slot as its own bead type.
- `agent` — who fills a slot. `slot` — the position being filled.
- `task` — work to do. `slot` — an assignment cell.

A slot bead exists when the slot itself needs first-class identity
(e.g., "this specific reviewer seat on this specific PR is a thing I
want to reference from elsewhere"). The bead form is the minority path;
the per-bead mechanism (map stored on the host bead) is the common path.

## Typed field set

```rust
pub struct SlotFields {
    /// Parent bead this slot attaches to. Typically a task / feature /
    /// merge-request.
    pub slot_host: Lww<BeadId>,

    /// Slot name within the host bead (e.g., "reviewer", "committer").
    pub slot_name: Lww<SlotName>,

    /// What kind of slot this is.
    pub slot_kind: Lww<SlotKind>,

    /// Current holder of the slot. None = vacant.
    pub slot_holder: Lww<Option<ActorId>>,

    /// When the current claim expires, if bounded.
    pub slot_expires: Lww<Option<WallClock>>,

    /// Role bead that describes what the slot is for (e.g. pointer at
    /// `bd-role-reviewer`). Optional — slots can be named without a
    /// registered role.
    pub slot_role: Lww<Option<BeadId>>,
}
```

| Field | Type | CRDT | Source (synthesized) | Default / Required |
|---|---|---|---|---|
| `slot_host` | `BeadId` | `Lww` (write-once) | new | required |
| `slot_name` | `SlotName` | `Lww` (write-once) | new | required |
| `slot_kind` | `SlotKind` | `Lww` | new | `Manual` |
| `slot_holder` | `Option<ActorId>` | `Lww` | new | `None` |
| `slot_expires` | `Option<WallClock>` | `Lww` | new | `None` |
| `slot_role` | `Option<BeadId>` | `Lww` | new | `None` |

CRDT behavior: `slot_host`, `slot_name` immutable post-creation (pair is
the identity of the slot). Other fields `Lww` by stamp; concurrent
claims resolve deterministically.

This type is mostly forward-looking; gascity has not yet made slot beads
a live surface in the sampled code. The field set is derived from work-
assignment semantics and kept minimal.

## Enum variants

```rust
pub struct SlotName(String); // validated kebab-case

pub enum SlotKind {
    Manual,           // human claims and releases
    AutoAssigned,     // a dispatcher fills from a pool
    OpenCompetition,  // many agents can race to fill (see Go WorkType)
}
```

Provenance:

- `SlotKind::OpenCompetition` mirrors Go's `WorkTypeOpenCompetition` at
  `types.go:699`. The Go `WorkType` enum applies to whole beads; here,
  slots can have their own policy.

## Lifecycle + invariants

- Workflow: `Open → InProgress → Closed`. `Closed` when the slot is
  retired (host bead closes, or slot is explicitly removed).
- **Invariant**: `(slot_host, slot_name)` unique — no two slot beads
  share the same position on the same host.
- **Invariant**: `slot_expires > now()` implies `slot_holder.is_some()`.
  A vacant slot cannot have an expiry.
- **Ready-exclusion**: YES when vacant (no work to do on an empty slot).
  NOT in gascity's current `readyExcludeTypes` list — this is a beads-rs
  addition. Document divergence in `primitives/ready-filter.md`.
- **Container behavior**: none.

## Dependencies

- As target:
  - `DepKind::Parent → <host-bead-id>`.
  - `DepKind::AssignedTo` from the slot → the holder agent bead, when
    `slot_holder.is_some()`.
- As source:
  - `DepKind::Related → <role-bead-id>` when `slot_role.is_some()`.

## Wire shape

```json
{
  "id": "bd-slot-rev-1",
  "issue_type": "slot",
  "title": "primary-reviewer on bd-mr-42",
  "description": "...",
  "status": "in_progress",
  "priority": 2,
  "workflow": { "state": "in_progress" },
  "claim": { "state": "unclaimed" },

  "slot_host": "bd-mr-42",
  "slot_name": "primary-reviewer",
  "slot_kind": "manual",
  "slot_holder": "bob",
  "slot_expires": 1713628800000,
  "slot_role": "bd-role-reviewer",

  "labels": [],
  "dependencies": [
    { "kind": "parent", "target": "bd-mr-42" },
    { "kind": "related", "target": "bd-role-reviewer" }
  ],
  "created": { ... },
  "updated_stamp": { ... },
  "content_hash": "..."
}
```

Type-specific: `slot_host`, `slot_name`, `slot_kind`, `slot_holder`,
`slot_expires`, `slot_role`.

## Parity status

- **Rust source**: not implemented.
- **Gap vs Go bd / gascity**: gascity does not write slot beads
  programmatically in the sampled code. The field set here is
  forward-looking and minimal.
- **Migration**: none.

## Revisit

- Are per-bead slots (the mechanism) enough, or do we actually need slot
  beads? Decision may defer until a concrete call site for
  slot-as-first-class-bead emerges. See `primitives/slots.md` for the
  mechanism alternative.
- If slot beads remain as a type variant, consider collapsing with the
  "claim" concept on regular beads. The shapes are nearly identical:
  holder + expiry + assigned role. A slot bead may be just a `claim`
  given its own id. Revisit after mechanism spec is written.
