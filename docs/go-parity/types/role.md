# role

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**Namespace:** core

**See also:** `agent.md`, `session.md` (TBD at `primitives/sessions.md`),
`rig.md`.

## Purpose

A role bead names a persona/identity that agents can inhabit: `mayor`,
`reviewer`, `dispatcher`, etc. Roles are the "what kind of agent" half of
the identity triple; an `agent` bead is the "which agent" half; a
`session` bead is the "which incarnation".

Roles are registered by gascity as required custom types in every
bd store (`internal/doctor/checks_custom_types.go:19-22`). gascity does
not currently emit role beads programmatically in the sampled code — the
type is reserved for human/config-driven identity declarations.

Distinguishing feature vs adjacent:

- `agent` — a specific agent instance (e.g. "alice", "bob"). `role` — the
  persona that agent is playing (e.g. "reviewer").
- `rig` — namespace/city boundary. `role` — identity within.
- `slot` — per-bead assignment metadata. `role` — reusable persona.

## Typed field set

```rust
pub struct RoleFields {
    /// Canonical role slug (lowercase, kebab-case, alphanumeric).
    /// Human-facing title goes in the shared `title` field.
    pub role_slug: Lww<RoleSlug>,

    /// Free-form responsibilities / persona description. Typically
    /// authored in `description`; this field is a pointer at the prompt
    /// template file for agent hydration.
    pub role_prompt_template: Lww<Option<PathBuf>>,
}
```

| Field | Type | CRDT | Source (gascity) | Default / Required |
|---|---|---|---|---|
| `role_slug` | `RoleSlug` | `Lww` (write-once) | derived from title / explicit at creation | required |
| `role_prompt_template` | `Option<PathBuf>` | `Lww` | convention; points at a `prompts/<slug>.md` file under the city root (`gascity/internal/citylayout/layout.go:17` `PromptsRoot`) | `None` |

Concurrent-write: `role_slug` is write-once, enforced at apply. Other
fields `Lww` by stamp.

Why so thin: gascity does NOT programmatically write role beads with
structured metadata in the sampled source. The role bead today is a stub
that the identity system references. Over-structuring it before real
consumers exist would be speculation.

## Enum variants

```rust
pub struct RoleSlug(String); // validated: `[a-z][a-z0-9-]*`
```

Not a sum — any number of roles can exist. Validation rule only.

## Lifecycle + invariants

- Workflow: `Open → Closed`. Closing a role retires the persona.
- **Invariant**: `role_slug` immutable post-creation.
- **Invariant**: `role_slug` unique within a namespace (rig). Enforced via
  an index during apply.
- **Ready-exclusion**: YES. Excluded by gascity's `readyExcludeTypes`
  (`internal/beads/beads.go:91`). A role bead is not work.
- **Container behavior**: none.

## Dependencies

- As target:
  - `DepKind::AssignedTo` from agent → role (Go `types.go:788`
    `DepAssignedTo`). beads-rs needs this DepKind.
  - `DepKind::Related` cross-links between adjacent roles.
- As source: rarely.

## Wire shape

```json
{
  "id": "bd-role-reviewer",
  "issue_type": "role",
  "title": "Reviewer",
  "description": "...",
  "status": "open",
  "priority": 4,
  "workflow": { "state": "open" },
  "claim": { "state": "unclaimed" },

  "role_slug": "reviewer",
  "role_prompt_template": "prompts/reviewer.md",

  "labels": [],
  "dependencies": [],
  "created": { ... },
  "updated_stamp": { ... },
  "content_hash": "..."
}
```

Type-specific: `role_slug`, `role_prompt_template`.

## Parity status

- **Rust source**: not implemented.
- **Gap vs Go bd / gascity**: gascity does not write role beads
  programmatically in the sampled code. The field surface here is minimal
  by design.
- **Migration**: existing custom-typed role beads read `title` and derive
  `role_slug` via lowercasing / kebab-case normalization; prompt template
  path unset unless a `role.prompt_template` metadata key already exists.

## Revisit

- As gascity grows out role-driven behavior (permissions, prompt layering,
  capability gating), this struct will accrete fields. Do not preemptively
  model them; add when concrete call sites exist.
