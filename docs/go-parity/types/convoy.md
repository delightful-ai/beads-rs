# convoy

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**Namespace:** core

**See also:** `molecule.md`, `primitives/molecules.md` (TBD),
`merge-request.md`.

## Purpose

A convoy is a container that groups a set of work items so they can be
merged together as a unit. Children attach via `DepKind::Parent →
convoy-id`. Convoys are orchestration-layer: gascity creates them when
multiple work items share a target branch and a merge strategy, so the
dispatcher can batch-wire their merge behavior. Gascity's creation site is
`internal/convoy/convoy.go::ConvoyCreate` (lines 43-74).

Distinguishing feature vs adjacent:

- `epic` / `molecule` — workflow containers. `convoy` — merge container.
- `merge-request` — the vcs-side artifact. `convoy` — the grouping that
  produces it.
- `milestone` — delivery checkpoint. `convoy` — bundled delivery unit.

## Typed field set

```rust
pub struct ConvoyFields {
    /// Who manages this convoy. Maps to gascity's `convoy.owner`.
    /// Source: `internal/convoy/convoy_fields.go:26`.
    pub convoy_owner: Lww<Option<ActorId>>,

    /// Notification target on completion.
    /// Source: `internal/convoy/convoy_fields.go:27` (`convoy.notify`).
    pub convoy_notify: Lww<Option<MailAddress>>,

    /// Associated molecule ID when this convoy was auto-created from a
    /// molecule's children.
    /// Source: `internal/convoy/convoy_fields.go:28` (`convoy.molecule`).
    pub convoy_molecule: Lww<Option<BeadId>>,

    /// Merge strategy for child items.
    /// Source: `internal/convoy/convoy_fields.go:29` (`convoy.merge`).
    pub convoy_merge: Lww<Option<ConvoyMerge>>,

    /// Target branch inherited by child work beads.
    /// Source: `internal/convoy/convoy_fields.go:32` (metadata key
    /// intentionally UNPREFIXED as `"target"` — line 30-31: "target is
    /// intentionally unprefixed so work beads can read their own value
    /// directly, while still inheriting it from convoy ancestors during
    /// sling").
    pub convoy_target: Lww<Option<BranchName>>,
}
```

| Field | Type | CRDT | Source (gascity) | Default |
|---|---|---|---|---|
| `convoy_owner` | `Option<ActorId>` | `Lww` | `convoy.owner` metadata key | `None` |
| `convoy_notify` | `Option<MailAddress>` | `Lww` | `convoy.notify` | `None` |
| `convoy_molecule` | `Option<BeadId>` | `Lww` | `convoy.molecule` | `None` |
| `convoy_merge` | `Option<ConvoyMerge>` | `Lww` | `convoy.merge` | `None` (= `Direct`) |
| `convoy_target` | `Option<BranchName>` | `Lww` | `target` (intentionally unprefixed; shared with child work beads) | `None` |

Concurrent-write: all `Lww` by stamp. For `convoy_target`, note that
gascity's "unprefixed" convention means work beads inheriting the target
read a shared metadata key. In beads-rs, child work beads should read from
their convoy parent via the dep graph, not from a duplicated field — so the
shared-metadata trick goes away. See `primitives/metadata-remapping.md`
(TBD).

## Enum variants

```rust
pub enum ConvoyMerge {
    Direct, // merge children directly to convoy_target
    Mr,     // create a merge request (see merge-request.md)
    Local,  // materialize locally, no remote push
}
```

Provenance: gascity `internal/convoy/convoy.go` comment
(`// merge strategy: "direct", "mr", "local"`) at
`convoy_fields.go:16`; sling's `SlingOpts.Merge` at
`internal/sling/sling.go:47` (`"", "direct", "mr", "local"`). Empty string
maps to `Direct` by convention.

## Lifecycle + invariants

- Workflow: `Open → InProgress → Closed`.
- `Closed` emits `events.ConvoyClosed`
  (`internal/convoy/convoy.go:131-148`).
- Creation emits `events.ConvoyCreated`
  (`internal/convoy/convoy.go:66-70`). beads-rs's event bead emission is
  separate — see `event.md`.
- **Progress is derived**, not stored: gascity computes
  `(Total, Closed, Complete)` at query time
  (`internal/convoy/convoy.go:76-109`). beads-rs should do the same in
  `BeadProjection` rather than storing counters.
- **Ready-exclusion**: NOT in Go's `readyExcludeTypes`. A convoy is not in
  gascity's `readyExcludeTypes` list (`internal/beads/beads.go:84-93`);
  reviewers may actually want to act on convoys. Keep NOT excluded.
- **Container behavior**: YES. gascity's `IsContainerType("convoy") ==
  true` (`internal/beads/beads.go:57-65`). Children attach via
  `DepKind::Parent`.

## Dependencies

- Target: children attach `DepKind::Parent → convoy-id` (gascity
  `internal/convoy/convoy.go:58-62, 122-126` — `ConvoyAddItems`
  and `ConvoyCreate` update children's `ParentID`).
- `DepKind::Related` can cross-link between convoys sharing a target branch.
- Source: when a convoy closes, it emits a merge artifact. If that
  artifact is a `merge-request` bead, use `DepKind::Related` (not
  blocks/parent) — the MR is an external observation, not a child.

## Wire shape

```json
{
  "id": "bd-cv7",
  "issue_type": "convoy",
  "title": "v2 auth cleanup",
  "description": "...",
  "status": "open",
  "priority": 2,
  "workflow": { "state": "open" },
  "claim": { "state": "unclaimed" },
  "convoy_owner": "alice",
  "convoy_notify": "team-auth@...",
  "convoy_molecule": "bd-mol-abc",
  "convoy_merge": "mr",
  "convoy_target": "main",
  "progress": { "total": 8, "closed": 5, "complete": false },
  "labels": [],
  "dependencies": [],
  "created": { ... },
  "updated_stamp": { ... },
  "content_hash": "..."
}
```

Type-specific stored: `convoy_owner`, `convoy_notify`, `convoy_molecule`,
`convoy_merge`, `convoy_target`. Derived: `progress`.

## Parity status

- **Rust source**: not implemented. Add `BeadType::Convoy` plus
  `ConvoyFields`, `ConvoyMerge` enum.
- **Gap vs Go bd / gascity**:
  - gascity stores these as metadata. beads-rs lifts to typed fields.
  - `convoy_target` remapping: see `primitives/metadata-remapping.md`.
- **Migration**: existing convoy beads from gascity read metadata keys
  at import time, populate typed fields, clear metadata.

## Revisit

- Is `convoy_notify` a `MailAddress` or a more general `NotifyTarget` that
  could be mail / event-kind / webhook? gascity uses it only for mail
  today; keep as `MailAddress` until a second call site appears. If/when
  multiple targets matter, promote to `NotifyTarget` enum.
