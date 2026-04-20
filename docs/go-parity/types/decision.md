# decision

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**Aliases:** `dec`, `adr` (Go `types.go:588-591`, `Normalize()`).

## Purpose

A durable record of an architectural/design choice. Shape mirrors an ADR
(Architecture Decision Record): what was decided, why, what alternatives
were rejected. Unlike `task`/`feature`, a `decision` bead does not represent
work to do — it represents work already reasoned, to be preserved.

Go enforces recommended sections (`types.go:629-633`):

- `## Decision`
- `## Rationale`
- `## Alternatives Considered`

gascity does not emit `decision` beads programmatically; humans and agents
create them via `bd create --type=decision`.

Distinguishing feature vs adjacent:

- `spike` — investigation producing a finding. `decision` records the choice
  that follows.
- `story`/`feature` — work to do.
- `epic` — container.

## Typed field set

No fields beyond common `BeadFields`. The ADR structure lives in
`description` / `design` / `acceptance_criteria`; the type is a classifier.

Rationale for not adding typed fields: "decision" content is narrative
markdown. Adding `Lww<DecisionOutcome>` enum would force a false choice
structure (approved/rejected/superseded) that ADRs already encode via
supersession links.

## Enum variants

None introduced. Supersession is expressed via `DepKind` (existing
`Related` for cross-reference; Go uses `DepSupersedes` for explicit version
chains at `types.go:782` — beads-rs needs a `DepKind::Supersedes` variant
to represent this honestly). See `primitives/dep-kinds.md` (TBD).

## Lifecycle + invariants

- Workflow: `Open` (draft) → `Closed` (finalized). Rarely `InProgress`.
- **Supersession**: a newer `decision` can point at an older one via
  `DepKind::Supersedes` (when that variant ships). The superseded decision
  is not deleted — both beads persist. This respects "information holds its
  shape" from `docs/philosophy/type_design.md`.
- **Ready-exclusion**: NOT excluded by Go's `readyExcludeTypes`. A draft
  decision IS actionable ("finish this ADR").
- **Container behavior**: none.

## Dependencies

- `DepKind::Supersedes` (new, when added): `decision(new) → decision(old)`.
- `DepKind::Related`: cross-link between ADRs covering adjacent domains.
- `DepKind::DiscoveredFrom`: an ADR authored during a spike carries
  `DiscoveredFrom → <spike-id>`.

## Wire shape

Identical to `task` with `issue_type: "decision"`.

## Parity status

- **Rust source**: not implemented. Add `BeadType::Decision` with aliases
  `["decision", "dec", "adr"]`.
- **Gap vs Go bd**: aliases + `DepKind::Supersedes` (latter tracked in
  `primitives/dep-kinds.md` not here).
- **Migration**: users with existing custom-typed "decision"/"adr" labels
  migrate naturally once the variant lands; a migration pass can rewrite
  beads whose `bead_type == Chore && labels contains "adr"` to
  `bead_type == Decision`. Not automatic — opt-in via a `bd migrate`
  command.
