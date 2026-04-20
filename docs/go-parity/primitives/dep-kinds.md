# Dependency Kinds

**Pin:** Go `bd` v1.0.2 (`c446a2ef`), gascity `main` (read 2026-04-20), beads-rs HEAD.
**Status:** _partial_ — beads-rs's `DepKind` enum covers 4 of Go's 19+ variants. Gascity actively uses 7 of those variants as edge data on the wire, so the enum surface is a real user of this gap, not a hypothetical.

Source of truth for Go's enumeration: `~/vendor/github.com/gastownhall/beads/internal/types/types.go:764-801`. Source of truth for beads-rs: `crates/beads-core/src/domain.rs:51-92`.

---

## Inventory

Every value documented here is backed by a specific constant in Go's source or a specific gascity call site (or both). Hypothetical variants are not included.

### `blocks`

- **Go const:** `DepBlocks` (`types.go:770`). Default for `--deps`, `dep add`, `--type`.
- **Semantics:** `A blocks B` (conventional wire: `from=B, to=A, type=blocks`, i.e., "B depends on A"). B is not ready until A closes.
- **Direction:** Asymmetric. `bd dep list B --direction=down` returns A (what B depends on); `bd dep list A --direction=up` returns B (what depends on A).
- **Go orchestration use sites:**
  - `internal/dispatch/ralph.go:637, 1062` — ralph dispatcher checks for `blocks` when computing readiness.
  - `internal/graphroute/graphroute.go:409`, `cmd/gc/cmd_convoy_dispatch.go:317`, `cmd/gc/cmd_sling.go:948` — graph routing treats `blocks` as a readiness-gating edge.
  - `internal/beads/memstore.go:256`, `internal/beads/caching_store.go:415, 442` — in-process stores treat `blocks` as a blocker when deriving `ready` work.
  - Test evidence: `internal/beads/memstore_test.go:290,396`, `cmd/gc/build_desired_state_test.go:81`, `cmd/gc/cmd_graph_test.go:22,62,160,161,228,277,278,312,356`.
- **Transitive closure:** Yes, and it is directly load-bearing. Readiness is a transitive-closure question over `blocks` edges; a single open ancestor blocks all descendants.
- **CRDT merge:** Concurrent `dep add (A,B,blocks)` from two actors converges via OR-set semantics — the presence of the edge is the LWW winner. Concurrent add+remove on the same edge resolves via write-stamp order in the canonical state (see `crates/beads-core/src/state/canonical.rs:508+`). `DepKind::Blocks` participates in DAG cycle checking (`domain.rs:89-91`).

### `parent-child`

- **Go const:** `DepParentChild` (`types.go:771`).
- **Semantics:** `A parent-child B` (wire: `from=B, to=A, type=parent-child`, i.e., "B's parent is A"). B is a structural child of A. Unlike `blocks`, parent-child does not require A to close before B is ready **by itself** — but it does cause B's readiness to cascade when A's whole subtree is expanded (e.g., molecule dispatch).
- **Direction:** Asymmetric, and a tree: each bead has at most one parent; any number of children.
- **Go orchestration use sites:**
  - `cmd/gc/wisp_gc.go:164` — garbage collection walks `parent-child` edges to find descendants of a closed root.
  - `cmd/gc/wisp_gc_test.go:136,139,172,211,214,241,291,294,297,368` — wisp GC tests assume parent-child hierarchy.
  - `internal/molecule/*.go` — molecule instantiation emits parent-child edges from every step to the root.
  - `cmd/gc/cmd_sling_routevars_test.go:62`, `cmd/gc/session_model_phase0_workflow_spec_test.go:71,123,221,222` — session/workflow models rely on parent-child for scoping.
  - Gascity's `ApplyGraphPlan` uses `parent_key`/`parent_id` on graph nodes to emit implicit parent-child edges (see `internal/beads/beads.go:37-38`).
- **Transitive closure:** Yes, forms a tree per molecule. Readiness traversal walks up the parent chain to find containing molecules.
- **CRDT merge:** Parent-child is special-cased in gascity's `MemStore.DepAdd` (`internal/beads/memstore.go:388-400`) — a parent-child edge does not collapse a same-direction non-parent-child edge. beads-rs's `DepKind::Parent` participates in DAG cycle checking (`domain.rs:89-91`). Concurrent parent-child additions on the same child (from two actors trying to reparent) converge via LWW on the edge write-stamp.

### `conditional-blocks`

- **Go const:** `DepConditionalBlocks` (`types.go:772`, _"B runs only if A fails"_).
- **Semantics:** B is ready only when A transitions to `closed` with a failure-class close reason. Inverse of `blocks`.
- **Direction:** Asymmetric; dependent on the closer's reason code.
- **Go orchestration use sites:**
  - `internal/dispatch/ralph.go:637, 1062` — ralph's readiness computation treats `conditional-blocks` as a readiness gate.
  - `cmd/gc/cmd_convoy_dispatch.go:317`, `cmd/gc/cmd_sling.go:948`, `internal/graphroute/graphroute.go:409` — graph routing considers `conditional-blocks` alongside `blocks` and `waits-for` as ready-affecting.
  - `internal/beads/memstore.go:256`, `caching_store.go:415,442` — in-process stores filter on it.
  - `types.go:826` — `AffectsReadyWork()` returns true.
- **Transitive closure:** Partial — the conditional-blocks edge only gates on the terminal state of the immediate predecessor, but that predecessor itself may be gated by further edges.
- **CRDT merge:** Same OR-set + write-stamp rules as `blocks`. **Rust does not have this variant.**

### `waits-for`

- **Go const:** `DepWaitsFor` (`types.go:773`, _"Fanout gate: wait for dynamic children"_).
- **Semantics:** A `waits-for` B with metadata `WaitsForMeta{Gate: "all-children"|"any-children", SpawnerID: "..."}` — A is ready when the gate over B's children is satisfied. Used to express "wait for all steps spawned by B's invocation to finish."
- **Direction:** Asymmetric. `from` waits for `to`'s children (or `to` itself if no spawner is set).
- **Go orchestration use sites:**
  - `internal/formula/compile.go:550` — formula compilation emits `waits-for` when a step fans out to dynamic children.
  - `internal/dispatch/ralph.go:637, 1062`, `internal/graphroute/graphroute.go:409`, `cmd/gc/cmd_convoy_dispatch.go:317`, `cmd/gc/cmd_sling.go:948` — readiness computation respects waits-for gates.
  - `internal/beads/memstore.go:256`, `caching_store.go:415,442` — in-process stores surface blocking status.
  - `internal/formula/recipe.go:95` — recipe deps use `waits-for` as a first-class edge kind.
  - `types.go:826` — `AffectsReadyWork()` returns true.
- **Transitive closure:** Depends on gate semantics. `all-children` gates only pass when the entire children set has closed; `any-children` gates pass on first close.
- **CRDT merge:** OR-set + write-stamp. The edge carries `WaitsForMeta` (JSON) as metadata today; typed replacement is a per-edge struct (see `types/gate.md`).

### `related`

- **Go const:** `DepRelated` (`types.go:776`, _"Association types"_).
- **Semantics:** Informational cross-reference. "These two beads are related" — no readiness implication.
- **Direction:** Symmetric semantically, but stored asymmetrically (gascity stores one-directional edges).
- **Go orchestration use sites:** _Not used by gascity in production code_. Grep hits show `related` only in MCP integration docs and beads' own tests. Gascity's code uses `relates-to` instead (see below).
- **Transitive closure:** No.
- **CRDT merge:** OR-set, no readiness impact.

### `discovered-from`

- **Go const:** `DepDiscoveredFrom` (`types.go:777`).
- **Semantics:** "This bead was discovered while working on that one." Audit-trail edge.
- **Direction:** Asymmetric. `from` was discovered while working on `to`.
- **Go orchestration use sites:** Used by agent workflows (e.g., `beads/examples/python-agent/agent.py:61` invokes `bd dep add <new> <parent> --type discovered-from`). Gascity does **not** emit `discovered-from` edges itself but must preserve them when they show up on existing beads.
- **Transitive closure:** No readiness impact. Useful for post-hoc auditing.
- **CRDT merge:** OR-set.

### `replies-to`

- **Go const:** `DepRepliesTo` (`types.go:780`, _"Conversation threading"_).
- **Semantics:** Message threading — bead A replies to bead B. Used by the beadmail subsystem.
- **Direction:** Asymmetric, thread-oriented.
- **Go orchestration use sites:** Not used in gascity's active subsystems per grep. Appears in `beads/format/format_test.go` and MCP tool docs. Reserved.
- **Transitive closure:** Chain-forming, but no readiness implication.
- **CRDT merge:** OR-set.

### `relates-to`

- **Go const:** `DepRelatesTo` (`types.go:781`, _"Loose knowledge graph edges"_).
- **Semantics:** Informational, same spirit as `related`.
- **Direction:** Effectively symmetric; stored directionally.
- **Go orchestration use sites:** `internal/api/handler_beads_graph_test.go:324` adds a `relates-to` edge in a test fixture. Gascity's production code does not emit `relates-to` on its own; preserves it when present.
- **Transitive closure:** No.
- **CRDT merge:** OR-set.

### `duplicates`

- **Go const:** `DepDuplicates` (`types.go:782`, _"Deduplication link"_).
- **Semantics:** A duplicates B (A is the dup, B is the canonical).
- **Direction:** Asymmetric.
- **Go orchestration use sites:** None in gascity's tree per grep. Reserved for dedup flows.
- **Transitive closure:** No readiness implication. Useful for merge/close-with-reason cascades.
- **CRDT merge:** OR-set.

### `supersedes`

- **Go const:** `DepSupersedes` (`types.go:783`, _"Version chain link"_).
- **Semantics:** A supersedes B (A is the newer version).
- **Direction:** Asymmetric, forms version chains.
- **Go orchestration use sites:** None in gascity per grep. Reserved for version/document supersession.
- **Transitive closure:** Chain-forming.
- **CRDT merge:** OR-set.

### `authored-by`, `assigned-to`, `approved-by`, `attests`

- **Go consts:** `DepAuthoredBy`, `DepAssignedTo`, `DepApprovedBy`, `DepAttests` (`types.go:786-789`). Marked "HOP foundation — Decision 004" in comments.
- **Semantics:** Entity/attestation edges. `A authored-by B` means "B is the author of A." `A attests B` means "A attests that B has some property" (typically used with `metadata` carrying skill name).
- **Direction:** Asymmetric.
- **Go orchestration use sites:** Not directly used by gascity's production code (per grep). Reserved for identity/HOP subsystem.
- **Transitive closure:** No readiness implication.
- **CRDT merge:** OR-set. `attests` edges carry metadata (attested property), which is a honor-no-metadata concern — typed replacement is a per-edge struct (see `primitives/metadata-remapping.md`).

### `tracks`

- **Go const:** `DepTracks` (`types.go:792`, _"Convoy → issue tracking (non-blocking)"_).
- **Semantics:** A `tracks` B — A monitors B's state without blocking on it. Convoys use this to aggregate progress across many beads without creating a DAG dependency.
- **Direction:** Asymmetric.
- **Go orchestration use sites:**
  - `internal/molecule/graph_apply.go:223, 365` — molecule apply emits `tracks` edges from body steps to the convoy/molecule root.
  - `internal/molecule/molecule_test.go:354, 357, 1205` — tests confirm convoy/molecule tracks semantics.
  - `internal/beads/memstore_test.go:293, 308` — memstore conformance tests.
  - `internal/beads/beadstest/conformance.go:810, 817` — public store conformance test.
  - `cmd/gc/cmd_graph_test.go:357` — "`tracks` is non-blocking — gc-2 should still be ready" confirms the readiness contract.
- **Transitive closure:** No — readiness does not propagate over `tracks` edges.
- **CRDT merge:** OR-set. Non-blocking, so `AffectsReadyWork()` is false.

### `until`

- **Go const:** `DepUntil` (`types.go:795`, _"Active until target closes"_).
- **Semantics:** A is active/valid until B closes. Used for muting, snoozing, time-boxed references.
- **Direction:** Asymmetric.
- **Go orchestration use sites:** Not grepped in gascity's production code. Reserved.
- **Transitive closure:** No.
- **CRDT merge:** OR-set.

### `caused-by`

- **Go const:** `DepCausedBy` (`types.go:796`, _"Triggered by target (audit trail)"_).
- **Semantics:** A was caused by B. Incident/event causality.
- **Direction:** Asymmetric.
- **Go orchestration use sites:** Not grepped in gascity's active code.
- **Transitive closure:** Chain-forming for audit.
- **CRDT merge:** OR-set.

### `validates`

- **Go const:** `DepValidates` (`types.go:797`, _"Approval/validation relationship"_).
- **Semantics:** A validates B (e.g., a test bead validates a feature bead).
- **Direction:** Asymmetric.
- **Go orchestration use sites:** Not grepped in gascity.
- **Transitive closure:** No.
- **CRDT merge:** OR-set.

### `delegated-from`

- **Go const:** `DepDelegatedFrom` (`types.go:800`, _"Work delegated from parent; completion cascades up"_).
- **Semantics:** A is work delegated out of B; when A closes, the cascade up to B should close B (or mark it progressed).
- **Direction:** Asymmetric, with a _closure-cascade_ rule.
- **Go orchestration use sites:** Not grepped in gascity's production code but the cascade semantics are real and load-bearing when present.
- **Transitive closure:** Yes via cascade.
- **CRDT merge:** OR-set; the cascade itself is an apply-time rule, not a merge rule.

---

## beads-rs current support

`crates/beads-core/src/domain.rs:54-70`:

```rust
pub enum DepKind {
    Blocks,
    Parent,
    Related,
    DiscoveredFrom,
}
```

Parse aliases (`domain.rs:66-69`):

| `DepKind::parse("blocks" | "block")` | `Blocks` |
| `DepKind::parse("parent" | "parent_child" | "parentchild" | "parent-child")` | `Parent` |
| `DepKind::parse("related" | "relates")` | `Related` |
| `DepKind::parse("discovered_from" | "discoveredfrom" | "discovered-from")` | `DiscoveredFrom` |

`requires_dag()` (`domain.rs:89-91`): `Blocks` and `Parent` return true (DAG-enforced, cycles rejected). `Related` and `DiscoveredFrom` are acyclic-tolerant informational.

### Gap table

| Go `DependencyType` | Gascity uses it? | Rust `DepKind` | Status |
|---------------------|:-:|-------------|--------|
| `blocks` | **yes** (many) | `Blocks` | faithful |
| `parent-child` | **yes** (many) | `Parent` | faithful |
| `conditional-blocks` | **yes** (`ralph.go`, `graphroute`, `sling`, `convoy_dispatch`, `memstore`, `caching_store`) | missing | **MUST ADD** |
| `waits-for` | **yes** (formula/compile, ralph, graphroute, sling, convoy_dispatch, memstore, caching_store) | missing | **MUST ADD** (plus gate-metadata design; see `types/gate.md`) |
| `related` | no (gascity doesn't emit) | `Related` | tolerated |
| `discovered-from` | no (beads agents do; gascity preserves) | `DiscoveredFrom` | tolerated |
| `replies-to` | no (beadmail reserved) | missing | deferred |
| `relates-to` | yes (`handler_beads_graph_test.go` sets it; gascity must round-trip) | missing | **MUST ADD** |
| `duplicates` | no | missing | deferred |
| `supersedes` | no | missing | deferred |
| `authored-by` | no (HOP foundation) | missing | deferred |
| `assigned-to` | no (HOP foundation) | missing | deferred |
| `approved-by` | no (HOP foundation) | missing | deferred |
| `attests` | no (HOP foundation) | missing | deferred |
| `tracks` | **yes** (molecule/graph_apply, cmd_graph_test) | missing | **MUST ADD** |
| `until` | no | missing | deferred |
| `caused-by` | no | missing | deferred |
| `validates` | no | missing | deferred |
| `delegated-from` | no (cascade reserved) | missing | deferred |

### Required work in beads-rs

Load-bearing for gascity:

1. **Add `DepKind::ConditionalBlocks`** with parse aliases `["conditional_blocks", "conditionalblocks", "conditional-blocks"]`. Mark `requires_dag() = true` (cycles forbidden). Include in `affects_readiness()` (new method, per Go's `AffectsReadyWork`).
2. **Add `DepKind::WaitsFor`** with parse aliases `["waits_for", "waitsfor", "waits-for"]`. Mark `requires_dag() = true`. Include in `affects_readiness()`. Needs per-edge `WaitsForMeta`-equivalent struct — typed, not a JSON blob (see `types/gate.md`).
3. **Add `DepKind::Tracks`** with parse aliases `["tracks", "track"]`. Mark `requires_dag() = false`. Does NOT affect readiness.
4. **Add `DepKind::RelatesTo`** with parse aliases `["relates_to", "relatesto", "relates-to"]`. Mark `requires_dag() = false`. Informational.
5. **Add `affects_readiness()` method on `DepKind`.** Returns true for `Blocks | Parent | ConditionalBlocks | WaitsFor`. This is the predicate gascity's readiness computation and Rust's equivalent both need.

Deferred (not blocking gascity today, but add as soon as the owning subsystem lands in Rust):

6. **`DepKind::Duplicates`**, `DepKind::Supersedes`, `DepKind::RepliesTo` — reserve variants with aliases; zero-impact on readiness; zero-impact on DAG enforcement.
7. **`DepKind::Until`**, `DepKind::CausedBy`, `DepKind::Validates`, `DepKind::DelegatedFrom` — add when cascade/incident/approval subsystems are designed.
8. **Entity-edges**: `DepKind::AuthoredBy`, `DepKind::AssignedTo`, `DepKind::ApprovedBy`, `DepKind::Attests` — deferred pending HOP decision. Currently `authored-by` and `assigned-to` semantics are encoded in the bead's structured fields (`created_by`, `assignee`), so promoting them to edges is a design call.

Validation surface:

9. **`DepKind::parse` should accept any kebab-case or snake-case form of the known variants.** The parser already lower-cases and `-`→`_`. Extend the match list to cover all 19 variants.
10. **Unknown kinds: error or preserve?** Go's `DependencyType.IsValid` accepts any non-empty ≤50-char string — custom dep kinds are allowed. Rust's current `DepKind::parse` errors on unknown. Decision: mirror Go by adding a `DepKind::Custom(ValidatedCustomDepKind)` variant, OR keep the closed enum and reject custom kinds at parse time. Recommended: closed enum (types-tell-truth principle) + an explicit policy doc. Gascity does not emit custom dep kinds today.

Ordering invariant:

11. **`DepKind`'s `Ord` impl uses `as_str()` for lexical comparison** (`domain.rs:95-97`). Adding variants is safe — canonical CBOR ordering is string-keyed so new variants slot in correctly without breaking byte-stable output.

---

## Queries on deps

### Direction semantics

Gascity's `DepList(id, direction)` (`bdstore.go:784-821`):

- **`direction=down` (default):** "what does `id` depend on." Returns beads `id` points to via outgoing edges. Each returned `Dep` has `IssueID=id`, `DependsOnID=<returned bead>`, `Type=<edge kind>`.
- **`direction=up`:** "what depends on `id`." Returns beads pointing AT `id` via incoming edges. Each returned `Dep` has `IssueID=<returned bead>`, `DependsOnID=id`, `Type=<edge kind>`.

The "up" label in Go's CLI corresponds to querying the **reverse adjacency list**. Rust's canonical state exposes both via `deps_out` and `deps_in` lookups (`state/canonical.rs:788+`, `:806+`), so the raw data is there; it's the `bd dep list` subcommand that needs wiring.

### Batch form

`bd dep list id1 id2 id3 --json` returns a flat array of raw dependency records — different JSON shape from single-id (which returns bdIssue-shaped rows). Both shapes must be supported. Gascity's single and batch parsers require this dual shape; see `GASCITY_SURFACE.md` command 14.

### Required work in beads-rs (for `bd dep list` subcommand)

1. **Implement `bd dep list <id>...` subcommand** in `crates/beads-cli/src/commands/dep.rs`.
2. **Accept `--direction=up|down`** (default `down`). Maps to querying `deps_out` (down) or `deps_in` (up) on `CanonicalState`.
3. **Accept `--type <kind>` filter.** Pre-filter the edge set by `DepKind` before serializing.
4. **Single-id output shape (compat mode):** array of bdIssue-shaped records with a top-level `dependency_type` field carrying the kind. Requires co-joining the issue's core fields with the edge kind — probably a `IssueWithDepKind` projection struct in `beads-api`.
5. **Batch output shape (compat mode):** array of `{issue_id, depends_on_id, type, created_at, created_by, metadata}` raw records. Edge metadata field is a JSON-encoded string (Go's actual shape) or — preferred — a typed struct per edge kind (see honor-no-metadata in `GASCITY_SURFACE.md`).
6. **Both directions for both modes** — gascity's `DepListBatch` always queries "down" (outgoing) but `DepList` supports both. Round-trip correctness is load-bearing.

### CRDT behavior during queries

`bd dep list` is a read operation, so it uses the standard query path (apply watermarks via `require-min-seen` if requested). No CRDT divergence concerns here beyond the usual HLC ordering. The underlying edge set is the canonical OR-set of live (non-tombstoned) edges keyed by `DepKey(from, to, kind)` (`state/canonical.rs:508+`). Concurrent adds converge via presence; concurrent add+remove converges via write-stamp LWW on the edge.
