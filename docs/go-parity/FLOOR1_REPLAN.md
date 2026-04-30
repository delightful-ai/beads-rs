# Floor 1 replan: typed foundations plus Gas City compatibility

**Pin:** Go `bd` v1.0.2 (`a3f834b3`, local tag checkout) and Gas City `main` (`d6011764`), both read from `~/vendor` on 2026-04-30.

This file supersedes the original Floor 1 tracker draft where it conflicts. The draft treated custom strings, config, metadata, and CRDT primitives as independent work. The current vendor trees show a different shape:

- Go beads still uses string-backed issue and dependency types, but it has a real known vocabulary and aliases in `internal/types/types.go`.
- Gas City currently writes most orchestration state through bead metadata in `internal/beads.Store`.
- Gas City doctor still checks `types.custom`, but that is a Go compatibility affordance, not a good canonical model for beads-rs.

## D0. Gas City metadata is durable state-machine projection

Gas City is not using metadata as an arbitrary user key/value stash. The latest checkout uses beads as the persistence substrate for several concrete state machines:

- workflow/control runtime: `gc.kind`, `gc.control_epoch`, `gc.retry_state`, `gc.fanout_state`, `gc.attempt`, `gc.attempt_log`, `gc.outcome`;
- convergence loop: `convergence.state`, `iteration`, `active_wisp`, `pending_next_wisp`, `last_processed_wisp`, gate result fields, terminal fields;
- session lifecycle: typed projections around base state, desired state, wake/drain/claim/quarantine/archive transitions;
- graph creation: `metadata`, `metadata_refs`, `assign_after_create`, edge kinds, and dependency metadata are part of an atomic plan contract.

That means Floor 1 is not "maybe add CRDT primitives later if a typed field asks for them." The source has already proven several merge-law needs: LWW typed enums and structs, monotonic epochs/counters, append-only attempt logs, claim/lease state, and exact metadata-field query projections. The implementation order should still start from owned typed fields, not generic primitive-first infrastructure, but the state-machine work is real Floor 1 work.

## D1. `types.custom` is compatibility, not domain truth

Rust should not make `types.custom` the source of truth for known Gas City types.

Canonical behavior:

- `BeadType` grows actual variants for known domain types, not a plain validated string.
- Go aliases are normalized at the boundary: `feat`/`enhancement`, `dec`/`adr`, `investigation`/`timebox`, `user-story`/`user_story`, `ms`.
- Known Go/Gas City types are accepted because the Rust domain knows them, not because a config key happened to list them.
- Unknown type acceptance is a separate policy decision. It should not be smuggled in by implementing `types.custom` as an unbounded domain extension.

Compatibility behavior:

- `bd config get --json types.custom` exists because Gas City doctor calls it.
- For the Gas City path, it may return a virtual comma-joined list containing the required Gas City type names.
- `bd config set types.custom <csv>` should accept the doctor/fix flow and validate syntax, but it does not have to expand the canonical Rust domain model.
- If a future consumer truly needs arbitrary custom types, add a first-class `Custom(ValidatedTypeName)` policy bead with explicit acceptance criteria. Do not let the Gas City doctor check create that feature by accident.

## D2. Metadata is a compatibility projection, not a no-op

Current Gas City is metadata-first. `BdStore.Create` sends `--metadata <json>`, `Update` and `SetMetadataBatch` send `--set-metadata`, `List` sends `--metadata-field`, and `bdIssue` parses a `metadata` object. Current Gas City also expects `assignee` and `from`; an older `owner`-rename plan is stale for this vendor checkout.

Rust should still keep the hard-cutover domain model:

- typed bead variants;
- typed variant fields;
- typed dependency edge payloads;
- typed query filters.

But the current Gas City bridge needs a projection layer:

- accept current metadata flags at the CLI/store boundary where Gas City calls them;
- map known metadata keys into typed fields;
- emit a `metadata` object on Gas City-shaped JSON responses until Gas City reads typed fields directly;
- preserve exact string-map behavior at the adapter boundary where Gas City relies on it, including empty string vs absent and exact conjunctive `--metadata-field` filters;
- reject or explicitly quarantine unknown metadata keys rather than storing an untyped permanent bag.

This is not a backward-compatibility shim inside the domain model. It is the live external adapter contract for the current Gas City checkout.

## D3. Typed state machines drive the CRDT work

The original Floor 1 draft listed `Max`, `AppendLog`, `Cas`, and resettable `Counter` as independent foundations. The corrected framing is:

- do not build abstract primitives before assigning ownership to real fields;
- do not demote proven Gas City state machines to speculative "candidate" work;
- add CRDT helpers as the implementation tool for typed fields whose current string-map encoding has a real merge race.

New order:

1. Add actual `BeadType` variants and parse/serialize behavior.
2. Add the full `DepKind` vocabulary and readiness semantics.
3. Add the Gas City CLI/wire projection for metadata read/write/filter behavior.
4. Assign every load-bearing metadata key to a typed field, derived value, runtime state, label, or edge payload.
5. Implement the merge law required by those assigned fields.

Examples:

- `convergence.iteration`, `gc.control_epoch`, `gc.next_attempt`, `gc.closed_by_attempt`, `gc.retry_count`, and `gc.spawned_count` need monotonic semantics, not LWW strings.
- `gc.attempt_log` needs append-only semantics because the current read-modify-write JSON blob can lose concurrent entries.
- `pending_create_claim`, wake/drain transitions, and session name/alias reservation need claim/lease or reservation semantics; external locks are not a replacement for a correct store model.
- `gc.kind`, convergence states, retry/fanout states, gate outcomes, and session lifecycle states need typed enums with invalid states rejected at the boundary.

## D4. Dependency kinds should mirror the full known vocabulary

Rust currently has four dep kinds. Go beads has a well-known vocabulary for workflow, graph, entity, tracking, reference, and delegation edges, while still accepting bounded custom strings.

Floor 1 should add the full known vocabulary, not just the four names that appeared in the first draft. Readiness semantics must match Go:

- readiness-affecting: `blocks`, `parent-child`, `conditional-blocks`, `waits-for`;
- non-readiness: the rest, including `tracks`, `relates-to`, `duplicates`, `supersedes`, `authored-by`, `assigned-to`, `approved-by`, `attests`, `until`, `caused-by`, `validates`, `delegated-from`.

`waits-for` edge metadata should become a typed edge payload. Gas City-compatible JSON may still expose the old metadata string where its parser requires it.

## D5. Refined Floor 1 order

1. Vendor truth lock and smoke contract.
   Record the exact Go beads and Gas City commits, plus the current command/JSON expectations. This prevents stale docs from steering implementation.

2. Actual `BeadType` variants.
   Add known variants and aliases. Do not base known type acceptance on `types.custom`.

3. Full `DepKind` vocabulary.
   Add all known Go dep kinds, readiness semantics, `dep add --type`, `dep remove`, `dep list`, and typed `waits-for` payload shape.

4. Gas City config facade.
   Implement only the `types.custom` surface Gas City doctor needs. Treat it as compatibility state or a virtual view.

5. CLI bridge commands and wire projection.
   Implement `bd dep list`, `bd create --graph`, `bd purge --json [--dry-run]`, list metadata filters, create metadata input, update metadata input, bare Go JSON output, and `assignee`/`from` projection as external adapter surfaces.

6. Gas City state-machine projection inventory.
   Turn Gas City's current `gc.*`, `convergence.*`, session, and graph-plan metadata into an explicit mapping to typed homes. This is the main prerequisite for safe per-state implementation.

7. Typed workflow/control state.
   Type `gc.kind`, control epochs, attempt counters, retry/fanout phases, outcomes, attempt logs, idempotency keys, and graph-apply references in the owning variants.

8. Typed convergence state.
   Type the convergence root state machine, including active/pending/last wisp pointers, iteration, gate outcome/retry fields, waiting state, and terminal state.

9. Typed session lifecycle.
   Type session lifecycle projection, wake/create/drain/sleep/archive/quarantine transitions, claim/lease fields, counters, and alias/name reservation.

10. Namespaces.
   Keep cross-namespace `DepKey` in scope for sessions/extmsg, but do not block basic typed bead acceptance on full namespace policy enforcement.

11. Reusable CRDT helper extraction.
   Factor `Max`, append log, claim/lease, OR-set, or resettable counter helpers from the typed field work once each owner and proof family is explicit.

## D6. Current tracker implications

- `beads-rs-12u7.2.1` should become actual typed `BeadType` variants, not "open enum first".
- `beads-rs-12u7.2.8` should become a `types.custom` compatibility facade, likely P2 after known variants are accepted.
- `beads-rs-12u7.2.2` should cover the full dep vocabulary, not only four additions.
- `beads-rs-12u7.2.3` through `.2.6` should be rewritten as real typed state-machine work, not demoted "candidate primitive" beads.
- `beads-rs-12u7.2.11` should become the Gas City state-machine projection inventory, not just a metadata-key spreadsheet.
- `beads-rs-12u7.2.10` must use Gas City's current batch shape: a flat array of raw dependency records, not a map by id.
