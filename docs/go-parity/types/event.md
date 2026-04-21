# event

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**Namespace:** core

**See also:** `primitives/events.md` (TBD), `message.md`,
`primitives/metadata-remapping.md` (TBD).

## Purpose

An `event` bead is an append-only record of something that happened:
"patrol.muted", "agent.started", "convoy.created", "bead.closed". Events
are immutable after creation. They exist to give downstream automation a
durable trigger surface AND to produce an audit trail.

Go bd promoted `event` from an orchestrator-only type to a built-in
internal type at `types.go:541` (`TypeEvent`) with the rationale:
"system-internal type used by set-state for audit trail beads". The
`Issue` struct carries event-specific fields `EventKind`, `Actor`,
`Target`, `Payload` at `types.go:109-112`. Go bd's `bd create --type=event`
accepts the `--event-category`, `--event-actor`, `--event-target`,
`--event-payload` flags (`cmd/bd/create.go:183-190`).

Gascity's `internal/events` package runs a parallel event bus
(`.gc/events.jsonl`) — that is NOT the bead event surface. The two
coexist: the JSONL bus is for high-volume tier-0 observability; `event`
beads are for durable, referenceable, queryable events that other beads
can point at.

Distinguishing feature vs adjacent:

- `message` — addressed communication; has a recipient. `event` — broadcast
  record; no recipient.
- `gate { await_type: Mail }` — waits for mail. An event-consuming gate
  waits for an event kind pattern; that is a distinct `await_type`
  (Await enum does not currently include `Event` — see Revisit).
- `task` — work to do. `event` — thing that happened.

## Typed field set

```rust
pub struct EventFields {
    /// Namespaced event kind, e.g. "patrol.muted", "agent.started",
    /// "bead.closed". This is the primary filter/routing key.
    /// Source: Go `Issue.EventKind` at `types.go:109`.
    pub event_kind: Lww<EventKind>,

    /// Entity URI that caused this event (agent id, session id, actor).
    /// Source: Go `Issue.Actor` at `types.go:110`.
    pub event_actor: Lww<Option<EntityUri>>,

    /// Entity URI or bead id affected by the event.
    /// Source: Go `Issue.Target` at `types.go:111`.
    pub event_target: Lww<Option<EntityUri>>,

    /// Event-specific JSON payload. Immutable post-creation. Validated as
    /// well-formed JSON at create time (Go `types.go:247-252`).
    /// Source: Go `Issue.Payload` at `types.go:112`.
    pub event_payload: Lww<EventPayload>,
}
```

| Field | Type | CRDT | Source | Default / Required |
|---|---|---|---|---|
| `event_kind` | `EventKind` | `Lww` (write-once) | Go `Issue.EventKind` | required at creation |
| `event_actor` | `Option<EntityUri>` | `Lww` (write-once) | Go `Issue.Actor` | `None` |
| `event_target` | `Option<EntityUri>` | `Lww` (write-once) | Go `Issue.Target` | `None` |
| `event_payload` | `EventPayload` | `Lww` (write-once, content-hashed) | Go `Issue.Payload` | empty JSON object |

### CRDT merge behavior

- All fields are write-once. `Lww` by stamp is correct per the CRDT
  substrate; the apply layer rejects updates post-creation (an event that
  mutates is no longer an event). Two replicas creating what looks like
  the same event race as two distinct events — that is correct behavior,
  because concurrent creation has separate `Stamp`s and typically
  separate `BeadId`s.

## Enum variants

```rust
pub struct EventKind(String); // e.g. "bead.created", "agent.started"

pub struct EntityUri(String); // e.g. "agent:alice", "bead:bd-xyz"

pub enum EventPayload {
    Empty,
    Json(serde_json::Value), // validated non-null JSON
}
```

`EventKind` is a newtype wrapper, not a sum. The namespace convention
(dot-separated: `category.verb`) is a validation rule, not an enum. The
kinds in gascity's `internal/events/events.go:19-58` form the expected
initial set:

- `session.*`: `woke`, `stopped`, `crashed`, `draining`, `undrained`,
  `quarantined`, `idle_killed`, `suspended`, `updated`.
- `bead.*`: `created`, `closed`, `updated`.
- `mail.*`: `sent`, `read`, `archived`, `marked_read`, `marked_unread`,
  `replied`, `deleted`.
- `convoy.*`: `created`, `closed`.
- `controller.*`: `started`, `stopped`.
- `city.*`: `suspended`, `resumed`.
- `order.*`: `fired`, `completed`, `failed`.
- `provider.swapped`, `worker.operation`.
- `extmsg.*`: `bound`, `unbound`, `group_created`, `adapter_added`,
  `adapter_removed`, `inbound`, `outbound`.

Keeping `EventKind` open (newtype around String) is intentional: new
categories accrete without type surgery. Validation enforces the
dot-separated shape.

## Lifecycle + invariants

- Workflow: `Open` on creation. Events may stay `Open` forever
  (they are records, not work); an audit-trail cleanup job closes them
  when they age out. No `InProgress` state.
- **Invariant**: all `event_*` fields are immutable after creation. Apply
  layer rejects updates.
- **Invariant**: `event_payload` must be valid JSON
  (Go `types.go:247-252`).
- **Invariant**: `event_kind` must be a dot-separated `category.verb` with
  `[a-z][a-z0-9_]*` on each side. Enforce at construction.
- **Ready-exclusion**: NOT currently in gascity's `readyExcludeTypes`. But
  events are purely records, not work. beads-rs SHOULD add `event` to its
  ready-exclusion list even if Go/gascity hasn't. Note this divergence in
  `primitives/ready-filter.md`.
- **Container behavior**: none.

## Dependencies

- As source: `DepKind::Related` from an event to the bead it records about
  is useful ("this event describes bead X").
- As target: `DepKind::CausedBy` (Go `types.go:796`) from a downstream
  action bead to the event that triggered it. beads-rs needs this DepKind
  variant.

## Wire shape

```json
{
  "id": "bd-ev-42",
  "issue_type": "event",
  "title": "agent.started: alice",
  "description": "...",
  "status": "open",
  "priority": 4,
  "workflow": { "state": "open" },
  "claim": { "state": "unclaimed" },

  "event_kind": "agent.started",
  "event_actor": "agent:alice",
  "event_target": "bead:bd-task-99",
  "event_payload": { "session_name": "alice-4", "generation": 1 },

  "labels": [],
  "dependencies": [
    { "kind": "related", "target": "bd-task-99" }
  ],
  "created": { ... },
  "updated_stamp": { ... },
  "content_hash": "..."
}
```

Type-specific: `event_kind`, `event_actor`, `event_target`,
`event_payload`.

## Parity status

- **Rust source**: not implemented. Add `BeadType::Event` plus
  `EventFields`.
- **Gap vs Go bd**:
  - Go stores the four event fields directly on the Issue struct. beads-rs
    stores them in the per-type field group — they only apply when
    `bead_type == Event`. Serde serialization for a `task` bead should
    omit them.
  - Go treats `event` as built-in internal (`IsBuiltIn() == true` via
    `types.go:565-567`); beads-rs promotes to a core variant. Same trust
    model.
- **Migration**: existing event beads round-trip. `EventKind`, `Actor`,
  `Target`, `Payload` fields on Go Issues map directly.

## Revisit

- Should `gate.gate_await_type` grow an `AwaitType::Event { kind: EventKind }`
  variant, so a gate can wait on event arrival? Today gascity uses mail
  (`convergence.handler.go`) for this. If event-driven gates become
  first-class, add the variant. Note here so the question is not lost.
- `event.priority` defaults: events are generally low-priority records.
  Consider `Priority::LOWEST` as default for events (not currently the
  `Priority::default() == MEDIUM`).
