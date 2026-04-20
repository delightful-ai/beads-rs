# wisp

**Pin:** v1.0.2, c446a2ef

**Parity status:** `declined as a type variant` — promoted to a namespace/flag
primitive. See `primitives/ephemeral-namespaces.md` (TBD) for the canonical
design.

## Decision

Do NOT introduce `BeadType::Wisp`. Instead, ephemerality is a per-bead
property orthogonal to type. gascity itself treats wisp-ness as a classifier
(the `ID-prefix` is `wisp` vs `mol`; the bead type under the hood can be
`molecule`, `task`, `agent`, `patrol`, whatever the underlying work is).

### Evidence from gascity

- `gascity/internal/beads/beads.go:69-72` classifies the molecule family as
  `{"molecule": true, "wisp": true}` — treating them as distinct types.
  This is a gascity-side convenience.
- Go's `IDPrefixMol = "mol"` and `IDPrefixWisp = "wisp"` constants at
  `types.go:1406-1409` are ID-generation prefixes, NOT type classifiers. A
  wisp at its creation site is stamped with `IDPrefixWisp` to get an ID
  like `bd-wisp-xxx`, but the `IssueType` field still reflects the actual
  work type.
- The real ephemerality concerns are three independent flags on the Issue
  struct:
  - `Ephemeral` (Go `types.go:79`) — "not synced via git", the primary
    wisp marker.
  - `NoHistory` (Go `types.go:80`) — "stored in wisps table but NOT
    GC-eligible"; mutually exclusive with `Ephemeral`.
  - `WispType` (Go `types.go:81`) — TTL classification for
    compaction: `heartbeat | ping | patrol | gc_report | recovery | error |
    escalation` (Go `types.go:668-681`).
- These three fields together, plus the storage-layer wisps table, ARE the
  wisp machinery. Type variant would be redundant and would also lie —
  `Ephemeral` beads come in many underlying types (agent heartbeat,
  patrol cycle report, GC report, etc.).

### Forcing function from beads-rs philosophy

From `docs/philosophy/type_design.md`:

> Two states are the same type iff no sequence of operations can distinguish
> them.

A `BeadType::Wisp` with `ephemeral=false` is already distinguishable from a
`BeadType::Wisp` with `ephemeral=true` — which means they are different
states carried by the same type. The ephemerality flag is the real
distinguisher. Making it a type variant would duplicate the flag into the
type and keep it outside, violating:

> Every behavior in your system should emerge from exactly one type
> configuration.

### What beads-rs DOES need

A typed ephemeral seam with three pieces:

1. **`Lww<Ephemerality>` field** on common `BeadFields`:

   ```rust
   pub enum Ephemerality {
       Persistent,            // default; synced via git
       Ephemeral { ttl: Ttl }, // not synced; GC-eligible per ttl
       NoHistory,             // stored but not GC-eligible
   }

   pub enum Ttl {
       Heartbeat,    // 6h  — liveness pings
       Ping,         // 6h  — health check ACKs
       Patrol,       // 24h — patrol cycle reports
       GcReport,     // 24h — garbage collection reports
       Recovery,     // 7d  — force-kill, recovery actions
       Error,        // 7d  — error reports
       Escalation,   // 7d  — human escalations
   }
   ```

   This replaces Go's `(Ephemeral bool, NoHistory bool, WispType string)`
   triple with a single sum type. The Go invariant "ephemeral and
   no_history are mutually exclusive" (`types.go:254-256`) becomes
   unrepresentable here — the enum enforces it.

2. **Storage seam**: ephemeral beads live in a separate ref/namespace so
   they do not pollute `refs/heads/beads/store`. This is the
   daemon/git-layer concern — see `primitives/ephemeral-namespaces.md`
   (TBD).

3. **ID-prefix convention**: beads-rs can optionally stamp `bd-wisp-…` on
   ephemeral beads at creation, mirroring Go `IDPrefixWisp`. This is a
   human affordance, not a type classifier.

## Parity status

- **Rust source**: not implemented. The `Ephemerality` enum above goes in
  `crates/beads-core/src/domain.rs` alongside `BeadType`.
- **Gap vs Go bd**: beads-rs shape is stricter and stores fewer fields.
  Go-authored wisps import by collapsing `(ephemeral, no_history,
  wisp_type)` into `Ephemerality`:
  - `(false, false, "")` → `Persistent`
  - `(true, false, "")` → `Ephemeral { ttl: <default> }` (caller chooses;
    absence in Go is a bug the beads-rs import catches).
  - `(true, false, wt)` → `Ephemeral { ttl: wt.into() }`
  - `(false, true, _)` → `NoHistory`
  - `(true, true, _)` → validation error; Go enforces mutual exclusion
    at create/update time, so this should never arrive.
- **Migration**: existing Go-authored wisps migrate via the mapping above.
  Existing beads-rs data has no wisps (feature unimplemented), so forward
  migration is clean.

## Cross-reference

See `primitives/ephemeral-namespaces.md` (TBD) for:

- The ref/namespace storage design.
- GC behavior per TTL class.
- The relationship to `refs/heads/beads/store` sync.
- CLI shape for `bd wisp purge-closed`.

See `primitives/id-prefixes.md` (TBD) for:

- ID-generation convention (`bd-wisp-xxx` vs `bd-mol-xxx` vs `bd-xxx`).
