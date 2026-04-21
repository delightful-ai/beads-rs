# wisp

**Pin:** v1.0.2, c446a2ef

**Parity status:** `declined as a type variant` — promoted to an `Ephemerality` field on common `BeadFields` + placement in the `wisps` namespace. See [`../primitives/namespaces.md`](../primitives/namespaces.md) and [`../DECISION_LOG.md`](../DECISION_LOG.md#d4-wisps-are-gc-eligible-not-gc-default).

## Decision

Do NOT introduce `BeadType::Wisp`. Ephemerality is:

1. A **typed field** on every bead (`Lww<Ephemerality>`), orthogonal to `BeadType`.
2. A **namespace placement**: beads with `Ephemerality::Ephemeral(_)` live in the `wisps` namespace, which has retention policy declared at the namespace level.

The bead type underneath can be `Molecule`, `Task`, `Event`, `Agent`, or whatever the underlying work actually is. A wisp is a classification, not a category.

### What "ephemeral" means in beads-rs

> **Wisps are GC-*eligible*, not GC-*default*.** A wisp survives until the namespace's retention sweeper runs and decides to reap it. Creating a wisp does not schedule its deletion; it declares the bead's retention class.

This is deliberately different from a "soft delete" or "TTL on creation" model. Ephemeral beads are full, queryable, replicated-within-their-namespace beads until the GC sweeper fires.

### Evidence from gascity

- `gascity/internal/beads/beads.go:69-72` classifies molecule + wisp as distinct types — a gascity-side convenience, not a storage distinction.
- Go's `IDPrefixMol = "mol"` and `IDPrefixWisp = "wisp"` at `types.go:1406-1409` are ID-generation prefixes, not type classifiers. A wisp is stamped with `IDPrefixWisp` to get an ID like `bd-wisp-xxx`, but the `IssueType` field still reflects the actual work type.
- Go's three orthogonal flags on every Issue:
  - `Ephemeral` (Go `types.go:79`) — "not synced via git"
  - `NoHistory` (Go `types.go:80`) — "stored but not GC-eligible"; mutually exclusive with `Ephemeral`
  - `WispType` (Go `types.go:81`) — TTL classification (`heartbeat | ping | patrol | gc_report | recovery | error | escalation` per Go `types.go:668-681`)
- Taken together plus the storage-layer wisps table, these ARE Go's wisp machinery. Type variant would duplicate the flag into the type.

### Forcing function from beads-rs philosophy

From `docs/philosophy/type_design.md`:

> Two states are the same type iff no sequence of operations can distinguish them.

A hypothetical `BeadType::Wisp` with `ephemeral=false` is already distinguishable from the same type with `ephemeral=true` — which means `ephemeral` is the real distinguisher. Making it a type variant would duplicate the flag.

## The typed Ephemerality field

```rust
pub enum Ephemerality {
    /// Default — synced via git, retained forever.
    /// Lives in a persistent namespace (typically `core`).
    Persistent,

    /// Not synced via git, GC-eligible per retention class.
    /// Lives in the `wisps` namespace.
    /// Retention class determines the TTL the namespace sweeper applies.
    Ephemeral(RetentionClass),
}

pub enum RetentionClass {
    Heartbeat,   //   6h  — liveness pings, worker health
    Ping,        //   6h  — health-check ACKs
    Patrol,      //  24h  — patrol cycle reports
    GcReport,    //  24h  — garbage-collection reports
    Recovery,    //   7d  — force-kill, recovery actions
    Error,       //   7d  — error reports
    Escalation,  //   7d  — human escalations
    Never,       //   ∞   — stored ephemerally (in `wisps` namespace) but exempt from TTL-based GC.
                 //        Subsumes Go's `NoHistory=true, Ephemeral=true` combination.
}
```

This replaces Go's `(Ephemeral bool, NoHistory bool, WispType string)` triple:

| Go flags                      | `Ephemerality` in beads-rs                        |
|-------------------------------|---------------------------------------------------|
| `(false, false, *)`           | `Persistent`                                      |
| `(true, false, "")`           | `Ephemeral(RetentionClass::Heartbeat)` (default)  |
| `(true, false, wt)`           | `Ephemeral(wt.into())`                            |
| `(false, true, *)`            | `Ephemeral(RetentionClass::Never)`                |
| `(true, true, *)`             | validation error in Go; impossible in beads-rs    |

Go's invariant "`Ephemeral` and `NoHistory` are mutually exclusive" (`types.go:254-256`) becomes **unrepresentable** in beads-rs — the enum makes it structurally impossible.

## Namespace placement

Beads are **routed to the `wisps` namespace at creation time** based on their `Ephemerality` field:

- `Ephemerality::Persistent` → core namespace (or wherever the caller explicitly targets).
- `Ephemerality::Ephemeral(_)` → `wisps` namespace.

The `wisps` namespace has policy properties (see [`../primitives/namespaces.md`](../primitives/namespaces.md)):

- `persist_to_git = false` — not synced as part of the core store ref.
- `retention = ttl-by-class` — sweeper reads each bead's `RetentionClass` and applies the matching TTL.
- `gc_authority = <specific replica or policy>` — who runs the sweep.

**Prerequisite:** the `wisps` namespace requires a retention GC sweeper (see [`../DECISION_LOG.md`](../DECISION_LOG.md) Floor 4, OQ3). Without the sweeper, ephemeral beads accumulate indefinitely, which is worse than keeping them in `core` with a flag. **Wisps namespace ships after the sweeper lands**, not before.

## What goes in the wisps namespace

Following gascity's usage pattern:

- Heartbeat/ping beads written by runtime session providers
- Patrol cycle reports
- GC reports
- Recovery action records
- Error beads flushed from agents
- Human escalation records that are meant to be ephemeral

A bead's `BeadType` is still whatever fits the underlying work (`Task`, `Event`, `Agent`, etc.). The `Ephemerality` field is what routes it to `wisps`.

## CLI

- `bd create --ephemeral [--retention-class <class>]` sets `Ephemerality::Ephemeral(class)` and routes to `wisps`.
  - `--retention-class` defaults to `heartbeat` if omitted when `--ephemeral` is set.
- `bd list` filters by namespace via existing `--namespace` flag. `bd list --namespace=wisps` shows ephemeral beads.
- `bd purge` (see [`../cmds/gascity/03-purge.md`](../cmds/gascity/03-purge.md)) may be the manual force-sweep; the automatic sweeper runs on a schedule.

## Cross-references

- [`../primitives/namespaces.md`](../primitives/namespaces.md) — namespace feature state, cross-ns dep fix, policy enforcement gaps.
- [`../DECISION_LOG.md`](../DECISION_LOG.md) — D3 (namespaces hold cross-cutting state), D4 (wisps are GC-eligible, not GC-default).
- [`../primitives/metadata-remapping.md`](../primitives/metadata-remapping.md) — the broader "no metadata" design; ephemerality is one example of the remapping.
