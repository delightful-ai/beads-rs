# rig

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**See also:** `primitives/namespaces.md` (TBD), `agent.md`, `role.md`.

## Purpose

A rig bead names a namespace / store boundary / "city within a city". In
gascity's multi-rig setups, each rig is a separately-replicated bead
store with its own identity; rig beads persist that identity so
configuration is not lost across process restarts.

Gascity registers `rig` as a required custom type
(`internal/doctor/checks_custom_types.go:21`) and references rig names
throughout: `session.Info.Rig`, `Order.Rig`, etc. The `BeadRouter`
resolves an agent within a rig context
(`gascity/internal/sling/sling.go:60` conceptually, via
`AgentResolver.ResolveAgent(cfg, name, rigContext)` at line 60).

Distinguishing feature vs adjacent:

- `agent` — identity within a rig. `rig` — the boundary itself.
- `role` — persona. `rig` — scoping container.
- `epic` / `molecule` — work containers. `rig` — namespace container.

## Typed field set

```rust
pub struct RigFields {
    /// Canonical rig id (lowercase, kebab-case).
    pub rig_id: Lww<RigId>,

    /// Absolute filesystem path where the rig's .beads store lives on
    /// the local box, when hosted on this node. None when remote-only.
    /// Derived from city config; mirrored here for discovery without
    /// re-reading config.
    pub rig_beads_dir: Lww<Option<PathBuf>>,

    /// Display label used in prompts and UI.
    pub rig_display_name: Lww<Option<String>>,

    /// City this rig belongs to (parent namespace), when the rig is
    /// nested. Multi-tier cities are expected.
    pub rig_city: Lww<Option<CityId>>,
}
```

| Field | Type | CRDT | Source (gascity) | Default / Required |
|---|---|---|---|---|
| `rig_id` | `RigId` | `Lww` (write-once) | `cfg.Rigs` naming; see `internal/config` | required |
| `rig_beads_dir` | `Option<PathBuf>` | `Lww` | city config at `rig.beads_dir` | `None` |
| `rig_display_name` | `Option<String>` | `Lww` | city config | `None` |
| `rig_city` | `Option<CityId>` | `Lww` | city config | `None` |

Concurrent-write: `rig_id` write-once; others `Lww`. Two config-push
events updating `rig_beads_dir` resolve by stamp.

Why so thin: rigs are primarily config-backed identities. The bead exists
to persist the identity in the replicated store; most operational state
lives in config files under `city.toml` + `.gc/`. Over-modeling would
duplicate config state into beads.

## Enum variants

```rust
pub struct RigId(String);  // validated: `[a-z][a-z0-9-]*`
pub struct CityId(String); // same validation
```

Newtype wrappers only.

## Lifecycle + invariants

- Workflow: `Open` indefinitely; `Closed` when the rig retires.
- **Invariant**: `rig_id` immutable, globally unique within its city.
- **Ready-exclusion**: YES. Excluded by gascity's `readyExcludeTypes`
  (`internal/beads/beads.go:92`). Rig beads are identity records.
- **Container behavior**: SEMI. Rigs contain agents/roles via
  `DepKind::Parent` from agent/role → rig. But most gascity code reads
  rig membership via `agent_rig` field (Lww) or via config, not by
  traversing parent-child edges.

## Dependencies

- As target:
  - `DepKind::Parent` from agents, roles in the rig.
  - `DepKind::Related` from other rigs (sibling cities).
- As source: rarely.

## Wire shape

```json
{
  "id": "bd-rig-gastown",
  "issue_type": "rig",
  "title": "Gastown",
  "description": "...",
  "status": "open",
  "priority": 4,
  "workflow": { "state": "open" },
  "claim": { "state": "unclaimed" },

  "rig_id": "gastown",
  "rig_beads_dir": "/Users/darin/src/cities/gastown/.beads",
  "rig_display_name": "Gastown",
  "rig_city": null,

  "labels": [],
  "dependencies": [],
  "created": { ... },
  "updated_stamp": { ... },
  "content_hash": "..."
}
```

Type-specific: `rig_id`, `rig_beads_dir`, `rig_display_name`, `rig_city`.

## Parity status

- **Rust source**: not implemented.
- **Gap vs Go bd / gascity**: gascity doesn't write rig beads
  programmatically in the sampled code. The field surface is forward-
  looking.
- **Migration**: none; rigs today live in config files, not in bead store.
  Adopting the rig type is an opt-in migration from config-only to
  bead-tracked identity.

## Revisit

- Is `rig_beads_dir` really type-specific, or a generic "where do I live"
  field that belongs in `primitives/storage-identity.md`? Today only rigs
  need it. Revisit if cities also want it.
- Nested-city support via `rig_city` is speculative. If gascity stays
  single-tier, drop the field.
