# story

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**Namespace:** core

**Aliases:** `user-story`, `user_story` (Go `types.go:595-596`, `Normalize()`).

## Purpose

A user-story-shaped narrative: "As a <role>, I want <capability>, so that
<outcome>." Functionally equivalent to `feature` for ready-filtering and
workflow, but preserved as a distinct type because the authoring convention
differs — stories drive acceptance criteria authoring. Go enforces the
recommended section `## Acceptance Criteria` (`types.go:620-623`, shared
with `task` and `feature`).

gascity does not emit `story` beads programmatically.

Distinguishing feature vs adjacent:

- `feature` — capability-first framing ("add OAuth").
- `story` — user-first framing ("as a returning user, I want...").
- `epic` — container holding multiple stories.
- `task` — no narrative shape.

## Typed field set

No fields beyond common `BeadFields`. The story structure is textual; the
type is a classifier.

Rationale for not adding typed fields (e.g. `story_role`,
`story_capability`, `story_outcome`): user stories are expressed as a single
grammatical sentence whose pieces are only meaningful together. Splitting
into three `Lww<String>` fields would either force every reader to
reconstruct the sentence, or admit inconsistency between the three parts.
Keep it in `description` where it can't get out of sync with itself.

## Enum variants

None.

## Lifecycle + invariants

- Workflow identical to `task`/`feature`.
- **Ready-exclusion**: NOT excluded.
- **Container behavior**: none; stories frequently live under an `epic`.

## Dependencies

Same as `feature`. Stories often carry `DepKind::Parent → <epic-id>`.

## Wire shape

Identical to `task` with `issue_type: "story"`.

## Parity status

- **Rust source**: not implemented. Add `BeadType::Story` with aliases
  `["story", "user-story", "user_story"]`.
- **Gap vs Go bd**: aliases only.
- **Migration**: users with existing custom-typed "story" or feature beads
  with a `user-story` label can migrate opt-in.
