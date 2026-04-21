# feature

**Pin:** v1.0.2, c446a2ef

**Parity status:** `faithful`

**Namespace:** core

## Purpose

A user-visible capability delta: new behavior, new API surface, new UI path.
`feature` is the canonical "positive work" variant — it adds something the
system previously didn't do.

Normalized from alias `enhancement` and `feat` per Go
`types.go:587-590` (`Normalize()`). beads-rs should accept the same aliases.

Distinguishing feature vs adjacent variants:

- `task` — generic work; `feature` is specifically capability-bearing.
- `story` — same intent but user-perspective narrative shape.
- `epic` — container of features/stories/tasks.

## Typed field set

No fields beyond common `BeadFields`.

## Enum variants

None introduced.

## Lifecycle + invariants

- Workflow identical to `task`.
- Convention: features should carry an `## Acceptance Criteria` section
  (Go `types.go:620-623`). Same lint enforcement path as `bug`.
- **Ready-exclusion**: NOT excluded.
- **Container behavior**: none; but features frequently appear as children of
  an `epic` via `DepKind::Parent`.

## Dependencies

Same as `task`. `DiscoveredFrom` flows features discovered mid-work into the
backlog.

## Wire shape

Identical to `task` with `issue_type: "feature"`.

## Parity status

- **Rust source**: `crates/beads-core/src/domain.rs::BeadType::Feature` (ships).
- **Gap vs Go bd**: parsing — beads-rs should alias `"enhancement"` and
  `"feat"` to `BeadType::Feature` (Go `types.go:588-590`). The current
  `enum_str!` invocation in `domain.rs:29-36` only accepts the canonical form;
  add `Feature => ["feature", "enhancement", "feat"]`.
- **Migration**: none.
