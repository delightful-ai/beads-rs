# merge-request

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**See also:** `convoy.md`, `primitives/vcs-integration.md` (TBD).

## Purpose

A merge-request bead represents an external VCS-side merge request (GitHub
PR, GitLab MR, Gitea PR). It is the bead that automation keeps in sync
with the remote VCS system; when the remote MR merges, the MR bead closes.

Gascity's sling machinery triggers MR creation via a `Merge = "mr"`
strategy (`internal/sling/sling.go:47`, `sling_core.go:287-289`) which
stamps `merge_strategy=mr` metadata; an external actor (a CI hook or a
gascity order wired to `merge-request` events) is what actually creates the
`merge-request` bead. Gascity itself rarely emits `Type="merge-request"`
beads; it observes them. The `merge-request` custom type is registered by
`internal/doctor/checks_custom_types.go:19-22` as a required type in every
gascity-managed store.

Distinguishing feature vs adjacent:

- `convoy` — grouping of work producing the MR. `merge-request` — the MR
  itself, mirrored from the VCS.
- `gate { await_type: GhPr }` — a wait-condition ON an MR. The
  `merge-request` bead is the tracking record; the `gate` consumes its
  state.
- `event` — one-shot factual record. `merge-request` — long-lived, has
  open/merged/closed lifecycle.

## Typed field set

```rust
pub struct MergeRequestFields {
    /// VCS provider hosting the MR.
    pub mr_provider: Lww<VcsProvider>,

    /// Repo slug the MR lives in (e.g. "gastownhall/beads").
    pub mr_repo: Lww<String>,

    /// Provider-local MR number (PR 42 on GitHub, MR !42 on GitLab).
    pub mr_number: Lww<u64>,

    /// Source branch containing the changes.
    pub mr_source_branch: Lww<BranchName>,

    /// Target branch the MR wants to merge into.
    pub mr_target_branch: Lww<BranchName>,

    /// Author handle on the VCS provider.
    pub mr_author: Lww<Option<String>>,

    /// Current remote state.
    pub mr_state: Lww<MergeRequestState>,

    /// Merge-commit SHA when merged.
    pub mr_merge_sha: Lww<Option<CommitSha>>,

    /// Wall-clock when the MR state last observed.
    pub mr_last_observed: Lww<Option<WallClock>>,

    /// Required reviewers / approvals received.
    /// OrSet: approvals accrete; revocation is a separate event, not a
    /// set-remove (Go would model that via a superseding approval).
    pub mr_approvals: OrSet<ApprovalRef>,
}
```

| Field | Type | CRDT | Source | Default / Required |
|---|---|---|---|---|
| `mr_provider` | `VcsProvider` | `Lww` | inferred from URL or explicit at creation | required |
| `mr_repo` | `String` | `Lww` | explicit | required |
| `mr_number` | `u64` | `Lww` | provider payload | required at creation |
| `mr_source_branch` | `BranchName` | `Lww` | provider payload | required |
| `mr_target_branch` | `BranchName` | `Lww` | provider payload | required |
| `mr_author` | `Option<String>` | `Lww` | provider payload | `None` |
| `mr_state` | `MergeRequestState` | `Lww` | observed from provider | `Open` |
| `mr_merge_sha` | `Option<CommitSha>` | `Lww` | provider payload at merge | `None` |
| `mr_last_observed` | `Option<WallClock>` | `Lww` | poller wall-clock | `None` |
| `mr_approvals` | `OrSet<ApprovalRef>` | `OrSet` union | provider approvals | empty |

### CRDT merge behavior

- Scalar fields: `Lww` — the poller that observes the most recent provider
  state wins.
- `mr_approvals` as `OrSet`: approvals are additive. Two pollers observing
  different approvals converge to the union. Revocation is not modeled
  as a remove; instead, a superseding `ApprovalRef` with a newer timestamp
  is added (see `ApprovalRef` below), and UI surfaces the latest per
  reviewer.

Gascity today stamps only `merge_strategy` metadata
(`sling_core.go:287-289`). The full field surface above does not exist in
gascity or Go bd — this is a beads-rs design proposal grounded in what the
merge-request type needs to be useful. Provenance for the concept is
gascity's required-types list at
`internal/doctor/checks_custom_types.go:19-22` and the reserved
`ExcludeTypes` registration at `beads-rs readyExcludeTypes`.

## Enum variants

```rust
pub enum VcsProvider {
    GitHub,
    GitLab,
    Gitea,
}

pub enum MergeRequestState {
    Draft,    // WIP / draft PR
    Open,     // ready for review
    Closed,   // rejected without merging
    Merged,   // merged into target
}

pub struct ApprovalRef {
    pub reviewer: String,       // provider handle
    pub state:    ApprovalState,
    pub at:       WallClock,
}

pub enum ApprovalState {
    Approved,
    ChangesRequested,
    Commented,        // review without decision
    Dismissed,        // approval revoked by action
}
```

Provenance: synthesized. Go has no typed merge-request surface; these
mirror GitHub's REST API review states. GitLab approval model maps cleanly.

## Lifecycle + invariants

- Workflow: `Open → InProgress → Closed`. Bead `workflow` tracks internal
  state; `mr_state` tracks the remote observation. Close the bead only
  when the MR reaches `Merged` or `Closed` on the remote.
- **Invariant**: when `workflow == Closed`, `mr_state ∈ {Merged, Closed}`.
- **Invariant**: `mr_number` is immutable after creation (apply-layer
  rejects updates).
- **Ready-exclusion**: YES. Excluded by gascity's `readyExcludeTypes`
  (`internal/beads/beads.go:85`) — "processed by automation". Humans don't
  claim an MR bead; automation updates it.
- **Container behavior**: none.

## Dependencies

- As target:
  - `DepKind::Blocks` from a `task`/`story` that wants to land via this MR.
  - `DepKind::Related` from the `convoy` that produced it (see
    `convoy.md` — convoy → MR is Related, not Parent, because the MR is
    an observation).
- As source:
  - Rarely; could block a `milestone` awaiting the merge.

## Wire shape

```json
{
  "id": "bd-mr-42",
  "issue_type": "merge-request",
  "title": "Add CRDT audit trail",
  "description": "...",
  "status": "in_progress",
  "priority": 2,
  "workflow": { "state": "in_progress" },
  "claim": { "state": "unclaimed" },

  "mr_provider": "github",
  "mr_repo": "gastownhall/beads-rs",
  "mr_number": 42,
  "mr_source_branch": "feat/crdt-audit",
  "mr_target_branch": "main",
  "mr_author": "alice",
  "mr_state": "open",
  "mr_merge_sha": null,
  "mr_last_observed": 1713628800000,
  "mr_approvals": [
    { "reviewer": "bob", "state": "approved", "at": 1713620000000 }
  ],

  "labels": [],
  "dependencies": [
    { "kind": "related", "target": "bd-cv7" }
  ],
  "created": { ... },
  "updated_stamp": { ... },
  "content_hash": "..."
}
```

Type-specific: every `mr_*` field.

## Parity status

- **Rust source**: not implemented.
- **Gap vs Go bd / gascity**: Go bd has no typed MR surface; MR handling
  lives in external adapters (`internal/github/`, `internal/gitlab/`).
  gascity registers the type string and excludes from ready, but doesn't
  prescribe fields. beads-rs's proposed field set is new design.
- **Migration**: existing `merge-request` beads in the wild likely carry
  URL-like `external_ref` and free-form metadata. Migration is a provider
  re-poll: given `external_ref`, hit the provider API and backfill typed
  fields. One-shot command.

## Revisit

- Is `mr_approvals` really type-specific, or should reviews be a generic
  `review` dep-kind + metadata? Today gascity/Go do not have a separate
  review primitive; keeping approvals here is simpler. If review becomes
  a first-class bead (Codex-style review bead), revisit.
- `mr_last_observed` is polling-state, not merge-request-state. Consider
  moving to `primitives/external-sync.md` as a generic "external ref last
  observed" stamp that any externally-synced bead can carry. If yes,
  remove from here.
