# Bead types

Per-type spec for the issue-type variants in Go beads, pinned to the tag recorded in `../README.md` (v1.0.2, c446a2ef).

Each type file captures:

- The Go type string (the `issue_type` JSON field value).
- Type-specific fields, metadata conventions, and labels.
- Lifecycle states unique to the type (e.g. gate outcomes, message read/archive).
- Dependency semantics (e.g. convoy-as-container, molecule-as-root).
- Exclusions (e.g. which types are filtered from `bd ready` by default).
- Parity status for beads-rs.

## Index — built-in types

- [task](task.md) — _faithful_ baseline. Default type.
- [bug](bug.md) — _faithful_ baseline.
- [feature](feature.md) — _faithful_ baseline.
- [epic](epic.md) — _faithful_ baseline (with `mol_type` extensions — see `primitives/molecules.md`).
- [chore](chore.md) — _faithful_ baseline.
- [decision](decision.md) — _deferred_. Aliases: `dec`, `adr`.

## Index — v1.0.0 additions

- [spike](spike.md) — _deferred_. Investigation/exploration work.
- [story](story.md) — _deferred_. User-story-shaped work.
- [milestone](milestone.md) — _deferred_. Milestone markers.

## Index — orchestration types

- [molecule](molecule.md) — _deferred_. Workflow-template instance. See also `primitives/molecules.md`.
- [wisp](wisp.md) — _deferred_. Ephemeral molecule instance. See also `primitives/wisps.md`.
- [convoy](convoy.md) — _deferred_. Container of children; expanded during dispatch.
- [gate](gate.md) — _deferred_. Async coordination gate with `convergence.*` metadata.
- [message](message.md) — _deferred_. Mail-style inter-agent communication with threading.
- [merge-request](merge-request.md) — _deferred_. VCS-driven external integration.
- [event](event.md) — _deferred_. Append-only event-bus entries (with `--event-*` create flags).
- [role](role.md) — _deferred_. Agent role/persona identity.
- [agent](agent.md) — _deferred_. Agent runtime identity.
- [rig](rig.md) — _deferred_. City namespace / store boundary identity.
- [slot](slot.md) — _deferred_. Per-issue metadata slot (distinct from the Slots primitive — this is the type, `primitives/slots.md` is the mechanism).

## Ready-exclusion set

Types that `bd ready` filters out by default (Go `internal/beads/beads.go:readyExcludeTypes`):

- `merge-request` (automation-driven)
- `gate` (async wait conditions)
- `molecule` (workflow containers)
- `message` (communication, not work)
- `session` (runtime identity)
- `agent` (identity tracking)
- `role` (persona definitions)
- `rig` (rig identity)

beads-rs should mirror this list. Track it in `primitives/ready-filter.md` when written.
