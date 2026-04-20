# Primitives

Per-concept design notes for the structural ideas in Go beads, pinned to the tag recorded in `../README.md` (v1.0.2, c446a2ef).

Where `cmds/` documents user-facing command surface, `primitives/` documents the underlying concepts those commands operate on. Each primitive file captures:

- What the concept is in Go (with source pointers).
- How it's represented in storage (tables, columns, constraints).
- How it interacts with the `Issue` data model (extra fields, derived views).
- The chosen beads-rs realization, including forcing functions when divergent.
- Parity status: `faithful`, `extended`, `simplified`, `deferred`, or `declined`.

## Index

- [slots](slots.md) — _deferred_. `SlotSet`/`SlotGet`/`SlotClear` on the storage interface; per-issue metadata for consumer-owned scratch. The canonical answer to "how does beads-rs hold gascity's `gc.idempotency_key`, `convergence.gate_*` etc."
- [molecules](molecules.md) — _deferred_. Workflow templates with `mol_type` (swarm/patrol/work), bonding, and phase transitions (proto → mol → wisp).
- [formulas](formulas.md) — _deferred_. Declarative workflow definitions parsed from TOML/JSON, cooked into molecules.
- [wisps](wisps.md) — _deferred_. Ephemeral molecules via `Ephemeral=true` flag; excluded from JSONL export; garbage-collected.
- [hooks](hooks.md) — _deferred_. `HookFiringStore` decorator pattern; event-driven extensibility at the storage layer.
- [custom-types](custom-types.md) — _deferred_. Normalized `custom_types` / `custom_statuses` tables (v1.0.0); per-project type extension beyond the built-in variants.
- [dependency-types](dependency-types.md) — _deferred_. `blocks`, `discovered-from`, `parent-child`, `waits-for`, `relates-to`, `duplicates`, `supersedes`, `replies-to`, `tracks`. Each with edge semantics and graph implications.
- [ids-and-prefixes](ids-and-prefixes.md) — _deferred_. Hash-based IDs (`bd-a3f8`), hierarchical extensions (`bd-a3f8.1`), prefix-based routing, `rename-prefix`.
- [namespaces-and-federation](namespaces-and-federation.md) — _deferred_. Go's shared-server / federation model vs. beads-rs's first-class `NamespaceId`.
- [sync-and-history](sync-and-history.md) — _deferred_. Dolt commit history, `--as-of`, compaction/flattening — and the beads-rs CRDT/git analogues.
