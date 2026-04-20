# molecule

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**See also:** `primitives/molecules.md` (TBD), `primitives/formulas.md` (TBD),
`convoy.md`, `wisp.md`.

## Purpose

A molecule is the root bead of a formula-instantiated workflow DAG. It holds
the workflow's identity and type classification (swarm | patrol | work) and
is the anchor every child step points back to via `gc.root_bead_id`.
Molecules are created by `molecule.Instantiate` /
`molecule.Cook` in gascity (`internal/molecule/molecule.go`), which expands
a compiled `formula.Recipe` into a set of child beads with
`DepKind::Parent → molecule-id` and ordering deps between steps.

Gascity's creation sites:

- `internal/molecule/molecule.go::Instantiate` — the primary path. Forces
  root `Type = "molecule"` when `step.Metadata["gc.kind"] != "workflow"`
  (`molecule.go:413-418`).
- `internal/molecule/molecule.go::Attach` — grafts a sub-DAG onto an
  existing workflow bead; preserves the sub-DAG root's declared type
  (`molecule.go:274-279`, `PreserveRootType: true`).
- `internal/convergence/create.go` — creates a convergence-loop molecule.
- Any caller of `molecule.Cook` / `CookOn`.

Distinguishing feature vs adjacent:

- `epic` — hand-authored container. `molecule` — formula-derived container.
- `convoy` — merge-time grouping. `molecule` — workflow-time orchestration.
- `wisp` — ephemeral molecule instance. `molecule` — persistent.

## Typed field set

```rust
pub struct MoleculeFields {
    /// Workflow classification. Maps to Go's `MolType` field at
    /// `types.go:103`, stored in the Issue struct directly (not metadata).
    pub mol_type: Lww<MolType>,

    /// Formula source name (`recipe.Name`). Write-once at creation but
    /// wrapped in Lww for CRDT determinism; later writes should be
    /// rejected by apply-layer validation.
    pub mol_formula: Lww<Option<String>>,

    /// Idempotency key for crash-retry protection. Stamped at creation
    /// in gascity as `metadata["idempotency_key"]`
    /// (`molecule.go:424-429`) and `metadata["gc.idempotency_key"]`
    /// (`molecule.go:270-272`).
    pub mol_idempotency_key: Lww<Option<String>>,

    /// Root bead ID for the workflow this molecule anchors. For a
    /// standalone molecule, equals the molecule's own id. For a sub-DAG
    /// created via Attach, points at the parent workflow's root.
    /// Source: `metadata["gc.root_bead_id"]` (`molecule.go:226-229`).
    pub mol_root_bead_id: Lww<Option<BeadId>>,

    /// Optimistic-concurrency epoch for Attach races. Source:
    /// `metadata["gc.control_epoch"]` (`molecule.go:247-249, 291-295`).
    pub mol_control_epoch: Lww<u64>,

    /// Graph-apply flag: true when this root was created as a graph-first
    /// workflow head rather than a legacy molecule root. Source:
    /// `metadata["gc.kind"] == "workflow"` (`molecule.go:383, 414`).
    pub mol_graph_workflow: Lww<bool>,
}
```

| Field | Type | CRDT | Source (Go/gascity) | Default |
|---|---|---|---|---|
| `mol_type` | `MolType` | `Lww` | Go `Issue.MolType` at `types.go:103`. | `MolType::Work` |
| `mol_formula` | `Option<String>` | `Lww` (write-once semantically) | `recipe.Name`, stamped via `b.Ref = recipe.Name` at `molecule.go:417`. | `None` |
| `mol_idempotency_key` | `Option<String>` | `Lww` (write-once) | `metadata["idempotency_key"]`, `metadata["gc.idempotency_key"]`. | `None` |
| `mol_root_bead_id` | `Option<BeadId>` | `Lww` (write-once) | `metadata["gc.root_bead_id"]`. | `None` (implies root = self) |
| `mol_control_epoch` | `u64` | `Lww` (numeric, monotonic via apply-layer) | `metadata["gc.control_epoch"]`. | `0` |
| `mol_graph_workflow` | `bool` | `Lww` (write-once) | `metadata["gc.kind"] == "workflow"`. | `false` |

**CRDT merge for `mol_control_epoch`**: standard `Lww` by stamp is correct
for the write-once property (new processor claiming the attach bumps the
epoch; stale processors have older stamps). But the application intent is
monotonicity — apply-layer validation should reject a local `set` that
tries to decrease the epoch, since that signals a bug, not legitimate
concurrency.

## Enum variants

```rust
pub enum MolType {
    Swarm,   // Coordinated multi-worker work
    Patrol,  // Recurring operational work
    Work,    // Regular assigned work (default)
}
```

Provenance: Go `types.go:649-653`.
- `Swarm` → `"swarm"` (gascity dispatches multiple agents in parallel).
- `Patrol` → `"patrol"` (gascity scheduler re-fires periodically).
- `Work` → `"work"` (default, regular work assignment).

## Lifecycle + invariants

- Workflow transitions: `Open → InProgress → Closed`. A molecule closes when
  all its step children close, mediated by gascity's convergence/reconciler
  rather than a direct `bd close`.
- **Invariant**: a molecule's `mol_root_bead_id` either equals its own id
  (standalone) or points at a bead whose `mol_graph_workflow == true`
  (Attach sub-DAG).
- **Invariant**: `mol_formula` is immutable after creation; the apply layer
  should reject updates. This is an "information holds its shape" case.
- **Ready-exclusion**: YES. Excluded by Go's `readyExcludeTypes`
  (gascity `internal/beads/beads.go:87`). A molecule is not actionable work;
  its children are.
- **Container behavior**: YES via `DepKind::Parent`. Steps attach with
  `DepKind::Parent → molecule-id` (`molecule.go:432-438`).
- **`no-history` flag**: gascity sets `NoHistory=true` on certain patrol
  molecules so they are stored in wisps table but NOT GC-eligible
  (Go `types.go:80`). beads-rs's typed handling of this lives in
  `primitives/ephemeral-namespaces.md` (TBD) — the flag is a storage
  concern, not a molecule-type field.

## Dependencies

- As target:
  - `DepKind::Parent` from every step child.
  - `DepKind::Blocks` from an Attach bead: attach-bead blocks on sub-DAG
    root (`molecule.go:285-287`).
- As source:
  - Rarely. A molecule that has follow-up work attaches the follow-up as
    a child, not as an outbound dep.

## Wire shape

```json
{
  "id": "bd-mol-a1",
  "issue_type": "molecule",
  "title": "converge: port crdt",
  "description": "...",
  "status": "in_progress",
  "priority": 2,
  "workflow": { "state": "in_progress" },
  "claim": { "state": "unclaimed" },
  "mol_type": "work",
  "mol_formula": "convergence/port-crdt",
  "mol_idempotency_key": "conv-abc123",
  "mol_root_bead_id": "bd-mol-a1",
  "mol_control_epoch": 2,
  "mol_graph_workflow": true,
  "labels": [],
  "dependencies": [],
  "created": { ... },
  "updated_stamp": { ... },
  "content_hash": "..."
}
```

Type-specific: `mol_type`, `mol_formula`, `mol_idempotency_key`,
`mol_root_bead_id`, `mol_control_epoch`, `mol_graph_workflow`.

## Parity status

- **Rust source**: not implemented. `BeadType::Molecule` plus the fields
  above.
- **Gap vs Go bd**:
  - `mol_type` exists in Go (`types.go:103`) but as a single typed field
    alongside the Issue struct. beads-rs stores it in the per-type field
    group.
  - `mol_formula`, `mol_idempotency_key`, `mol_root_bead_id`,
    `mol_control_epoch`, `mol_graph_workflow` are stored in Go as
    metadata keys (`idempotency_key`, `gc.idempotency_key`,
    `gc.root_bead_id`, `gc.control_epoch`, `gc.kind`). beads-rs lifts
    them to typed fields.
- **Metadata remapping**: see `primitives/metadata-remapping.md` (TBD).
  The `gc.*` namespace is load-bearing for gascity; if beads-rs wants to
  round-trip Go-authored beads losslessly, these remappings need a
  registered table so `gc.idempotency_key` ↔ `mol_idempotency_key` is a
  single source of truth.
- **Migration**: existing molecule beads from Go with metadata keys need a
  one-shot hoist to typed fields. Implementation: read metadata at import
  time, populate typed fields, clear the metadata keys. Document in
  `bd migrate`.

## Revisit

- Does `mol_graph_workflow` deserve being a distinct enum variant of
  `MolType` (e.g. `MolType::GraphWorkflow`)? gascity's `gc.kind="workflow"`
  is orthogonal to `swarm|patrol|work` — a graph-first workflow can still
  be classified as work. So keep separate. Note here in case the dimensions
  actually collapse under scrutiny.
