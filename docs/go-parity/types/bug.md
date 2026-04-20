# bug

**Pin:** v1.0.2, c446a2ef

**Parity status:** `faithful`

## Purpose

A defect in shipped behavior: the system does X when it should do Y. `bug`
distinguishes itself from `task`/`feature`/`chore` in two observable ways:

1. It has canonical recommended sections `## Steps to Reproduce` and
   `## Acceptance Criteria` (Go `types.go:615-619`, `RequiredSections` on
   `TypeBug`). beads-rs can enforce these via lint.
2. It is conventionally higher-priority than tasks; no built-in rule enforces
   this, but `bd ready` sort policies (`SortPolicyPriority`) surface it first.

Gascity does not write bugs programmatically; they come from human or
agent-authored `bd create --type=bug` calls.

## Typed field set

No fields beyond common `BeadFields`. `bug` shares the full `task` shape;
the difference is semantic ("is this a defect?"), not structural.

## Enum variants

None introduced.

## Lifecycle + invariants

- Workflow transitions identical to `task`.
- Convention: close-reason should identify resolution ("fixed by bd-xyz",
  "cannot reproduce", "wontfix"). Go treats certain close reasons as "failure"
  via `types.FailureCloseKeywords` at `types.go:881-910`; this affects
  `DepConditionalBlocks` semantics (a `bug` closed with "wontfix" is treated
  as a failed attempt).
- **Ready-exclusion**: NOT excluded. Bugs are actionable work.
- **Container behavior**: none.

## Dependencies

Same as `task`. `DiscoveredFrom` is load-bearing for bug: a bug bead
discovered while implementing a feature should carry
`DiscoveredFrom → <feature-id>`.

## Wire shape

Identical to `task`. `issue_type: "bug"` is the only delta.

## Parity status

- **Rust source**: `crates/beads-core/src/domain.rs::BeadType::Bug` (ships).
- **Gap vs Go bd**: zero for persisted state. Go's `RequiredSections` lint
  hints are a CLI/surface-layer concern; if beads-rs ports them, they belong
  in `beads-cli` or a lint crate, not the core type.
- **Migration**: none.
