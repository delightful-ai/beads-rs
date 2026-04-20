# chore

**Pin:** v1.0.2, c446a2ef

**Parity status:** `faithful`

## Purpose

Repo hygiene with no behavior delta: dependency bumps, lint fixes, renames,
formatting passes, CI tweaks. Distinguished from `task` by user expectation
("this shouldn't change what the app does") and from `bug` by absence of a
defect report.

Gascity does not create chores programmatically; they originate from humans
and occasionally from tool-driven orders (e.g. a scheduled dependency
updater order could fire `bd create --type=chore`).

## Typed field set

No fields beyond common `BeadFields`.

## Enum variants

None.

## Lifecycle + invariants

- Workflow identical to `task`.
- **Ready-exclusion**: NOT excluded.
- **Container behavior**: none.

## Dependencies

Same as `task`. Chores sometimes carry `DepKind::Related` to the feature
whose build-out required the cleanup.

## Wire shape

Identical to `task` with `issue_type: "chore"`.

## Parity status

- **Rust source**: `crates/beads-core/src/domain.rs::BeadType::Chore` (ships).
- **Gap vs Go bd**: zero.
- **Migration**: none.
