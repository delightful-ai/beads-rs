# agent

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**Namespace:** core

**See also:** `role.md`, `rig.md`, `primitives/sessions.md` (TBD).

## Purpose

An `agent` bead is the runtime identity of an agent: its alias, current
state, and the session (if any) it is currently inhabiting. Distinct from
a `session` bead (the runtime incarnation) and a `role` bead (the
persona). Agents are long-lived; sessions come and go.

Gascity registers `agent` as a required custom type
(`internal/doctor/checks_custom_types.go:21`). The sampled code does not
show programmatic creation of agent beads; identity fields live in
`gascity/internal/agent/` (session naming, hints) and session metadata.

Distinguishing feature vs adjacent:

- `session` — runtime incarnation with a tmux/process handle.
  `agent` — the persistent identity across sessions.
- `role` — persona. `agent` — the entity playing roles.
- `rig` — namespace. `agent` — entity within a rig.
- `slot` — per-bead assignment metadata. `agent` — the assignee.

## Typed field set

```rust
pub struct AgentFields {
    /// Canonical agent alias (lowercase, kebab-case or dotted, e.g.
    /// "alice", "gastown.mayor"). Source: gascity's session alias
    /// validation (`session/names.go:109`).
    pub agent_alias: Lww<AgentAlias>,

    /// Current availability state.
    pub agent_state: Lww<AgentState>,

    /// Current session bead id when active.
    pub agent_current_session: Lww<Option<BeadId>>,

    /// Current role assignments — an agent can play multiple roles
    /// concurrently (reviewer + committer for this rig, say).
    /// OrSet: roles accrete; removal is a dedicated dep-remove event.
    pub agent_roles: OrSet<BeadId>,

    /// Rig this agent belongs to.
    pub agent_rig: Lww<RigId>,

    /// Default provider (claude-code, codex, gemini, …) if configured.
    pub agent_default_provider: Lww<Option<ProviderKind>>,
}
```

| Field | Type | CRDT | Source (gascity) | Default / Required |
|---|---|---|---|---|
| `agent_alias` | `AgentAlias` | `Lww` (write-once) | `session/names.go:109` | required at creation |
| `agent_state` | `AgentState` | `Lww` | synthesized from session lifecycle | `Offline` |
| `agent_current_session` | `Option<BeadId>` | `Lww` | tracked via session beads | `None` |
| `agent_roles` | `OrSet<BeadId>` | `OrSet` union | convention | empty |
| `agent_rig` | `RigId` | `Lww` (write-once) | city/rig config | required at creation |
| `agent_default_provider` | `Option<ProviderKind>` | `Lww` | session metadata `provider` (`session/manager.go:289`) | `None` |

### CRDT merge behavior

- `agent_alias`, `agent_rig`: write-once post-creation; apply-layer
  rejects updates (a different alias is a different agent).
- `agent_state`: `Lww` — most recent observation wins. Concurrent
  observations typically come from the same controller, but across
  tailnet replicas, a later-stamped "offline" overrides an earlier
  "active" correctly.
- `agent_current_session`: `Lww`. Setting to a new session replaces the
  old. Explicitly nullable for "no current session".
- `agent_roles`: `OrSet` because role assignments are additive and
  concurrent adds should union. Role removal is a separate `OrSet`
  remove, which gascity today does not model — document as a TBD.

## Enum variants

```rust
pub struct AgentAlias(String); // validated per session/names.go

pub enum AgentState {
    Offline,      // no runtime
    Starting,     // provisioning a session
    Active,       // session live, accepting work
    Drained,      // session completed drain, dormant
    Quarantined,  // crash-loop lockout
}

pub enum ProviderKind {
    ClaudeCode,
    Codex,
    Gemini,
    Custom(String),
}
```

Provenance:

- `AgentState` — synthesized from gascity's session states
  (`session/manager.go:25-51`):
  - `Active`/`Awake` → `Active`
  - `Creating`/`Starting` → `Starting`
  - `Suspended`/`Asleep`/`Drained` → `Drained`
  - `Quarantined` → `Quarantined`
  - No session → `Offline`
  This collapses the session-centric state machine into the agent-level
  availability dimension. The session's own finer-grained states live on
  the session bead.
- `ProviderKind` — from gascity's provider metadata
  (`session/manager.go:289`). `Custom(String)` for non-builtin providers.

## Lifecycle + invariants

- Workflow: `Open` indefinitely; `Closed` only when the agent retires
  permanently. An agent taking a break does NOT close the bead; it
  transitions `agent_state` instead.
- **Invariant**: `agent_alias` unique within the containing rig. Enforced
  by an index during apply.
- **Invariant**: `agent_alias` and `agent_rig` immutable post-creation.
- **Invariant**: `agent_current_session` must reference a bead with
  `bead_type == Session` and `session_agent == this.id`. Enforced at
  apply via the dep graph.
- **Ready-exclusion**: YES. Excluded by gascity's `readyExcludeTypes`
  (`internal/beads/beads.go:90`). Agent beads are identity tracking,
  not actionable work.
- **Container behavior**: none.

## Dependencies

- As target:
  - `DepKind::AssignedTo` from a session bead → this agent (Go
    `types.go:788`). beads-rs needs this DepKind.
  - `DepKind::AuthoredBy` from work beads → this agent (Go `types.go:787`,
    `DepAuthoredBy`). beads-rs needs this DepKind too.
- As source:
  - `DepKind::Related` to current role beads. The `agent_roles` field
    duplicates this for fast read access; edges ARE the source of truth
    and the OrSet is a projection.

### CRDT projection question

If `agent_roles` is a projection of `DepKind::Related → role beads`,
storing it as its own OrSet creates two sources of truth. Decision: store
`agent_roles` as OrSet AND emit the dep edges; apply layer enforces
consistency (adding a role adds both; removing a role removes both). Under
concurrent add-from-A / add-from-B, both converge via OrSet union AND both
dep edges exist, which converges via the dep-store's OR-set merge. They
do not drift.

Alternative: drop the field, always read from dep store. Chose
field-plus-edge for query latency, but the alternative is valid and
should be measured. See Revisit.

## Wire shape

```json
{
  "id": "bd-agent-alice",
  "issue_type": "agent",
  "title": "Alice",
  "description": "...",
  "status": "open",
  "priority": 4,
  "workflow": { "state": "open" },
  "claim": { "state": "unclaimed" },

  "agent_alias": "alice",
  "agent_state": "active",
  "agent_current_session": "bd-sess-42",
  "agent_roles": ["bd-role-reviewer", "bd-role-dispatcher"],
  "agent_rig": "gastown",
  "agent_default_provider": "claude-code",

  "labels": [],
  "dependencies": [
    { "kind": "related", "target": "bd-role-reviewer" },
    { "kind": "related", "target": "bd-role-dispatcher" }
  ],
  "created": { ... },
  "updated_stamp": { ... },
  "content_hash": "..."
}
```

Type-specific: `agent_alias`, `agent_state`, `agent_current_session`,
`agent_roles`, `agent_rig`, `agent_default_provider`.

## Parity status

- **Rust source**: not implemented.
- **Gap vs Go bd / gascity**: gascity does not write agent beads
  programmatically in the sampled code. The field surface here is
  forward-looking, grounded in session-metadata shapes at
  `session/manager.go:286-298`.
- **Migration**: any existing agent beads from configured named sessions
  carry metadata keys; import hoists them. If none exist, no migration.

## Revisit

- `agent_roles` vs dep edges: pick one source of truth after measuring
  read path. See CRDT section.
- `agent_default_provider::Custom(String)` leaks arbitrary strings into
  a typed enum. Reasonable for now (providers are plug-in). If the custom
  string space grows nuanced, promote to a `ProviderKind::Named(ProviderId)`
  with its own newtype.
