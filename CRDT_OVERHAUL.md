## North Star

Implement the **locked decisions D1–D5** from `docs/CRDT_AUDIT.md` so that:

- **apply is total and deterministic** (no state-dependent interpretation),
- **merge is deterministic and aligns with apply**,
- labels + deps become **real CRDT sets** (OR-Set / ORSWOT),
- collision handling is **consistent and deterministic everywhere**,
- we **avoid durable-but-unapplied events** by eliminating avoidable apply-time errors,
- and we **delete code** (full-set LWW label/dep paths, dep LWW provenance, note-in-bead log, git remap collisions).

This plan is **self-contained, precise, and unambiguous** so implementation can proceed without further design debates.

---

## Global constraints (locked)

1) **No version bump.** We update v1 in-place because there is no installed base.
   - We will change v1 wire/schema layouts.
   - Old local stores (if any) are unsupported; wipe and re-init.
   - No `*_FORMAT_VERSION` or protocol bumps.

2) **D1–D5 are locked** and implemented in both core logic and wire formats.

3) **No full-set label/dep operations.** All label/dep changes are add/remove ops.

4) **Orphan labels/deps/notes are allowed.** Apply must not error if bead is missing.

Code refs (current):
- Store meta versions + version checks: `src/core/store_meta.rs:52-100`, `src/daemon/store/runtime.rs:91-118`
- Full-set labels in wire patch: `src/core/wire_bead.rs:427-473`
- Full-set label mutation paths: `src/daemon/mutation_engine.rs:981-1036`
- Apply currently errors on missing notes: `src/core/apply.rs:483-491`

---

## Summary of decisions baked into this plan

- **Dot allocation**: explicit dot on add ops (Dot A). Dot minted locally using a **persisted monotonic counter** per replica.
- **Orphans**: labels + deps + notes are stored even if bead missing.
- **Tie-breaks** (deterministic across apply + merge):
  - Note collision: `(note.at, note.author, sha256(note.content))`
  - Bead creation collision: `(created_stamp, content_hash)`
  - OR-Set dot collision: `(value lexicographic, sha256(dot||value))`
- **No full-set label/dep ops**: remove field + logic entirely.
- **Performance**: replace full-set LWW with delta ops; reduce copying; preserve dep indexes.

Code refs (current):
- Labels are LWW in BeadFields: `src/core/bead.rs:82-115`
- Dep edges are LWW with earliest provenance: `src/core/dep.rs:143-220`
- Note log embedded in Bead: `src/core/bead.rs:138-199`, `src/core/collections.rs:120-173`

---

## Repo anchors (what this plan maps to today)

The following are current code anchors that this plan modifies:

- **Apply logic**: `src/core/apply.rs` (`build_workflow`, `build_claim`, `apply_note_append`, dep apply)
- **Wire types**: `src/core/wire_bead.rs` (`WireBeadPatch`, `NotesPatch`, dep ops, TxnOpV1)
- **Notes**: `src/core/collections.rs` (NoteLog), `src/core/bead.rs` (notes in Bead)
- **Deps**: `src/core/dep.rs` (DepEdge, DepLife)
- **State**: `src/core/state.rs` (`CanonicalState::join`, tombstone resurrection rule)
- **Mutation engine**: `src/daemon/mutation_engine.rs` (labels, deps, claim/close patches, note id gen)
- **Event validation**: `src/core/event.rs` (limits validation)
- **Git checkpoint**: `src/git/wire.rs` (serialize state/deps/notes)
- **Queries / views**: `src/core/wire_bead.rs`, `src/api/issues.rs`, `src/daemon/query_model.rs` (updated_at)
- **Store meta**: `src/core/store_meta.rs`, `src/daemon/store/runtime.rs` (meta persistence)

---

## Core CRDT model changes

### 1) Labels and deps become OR-Set (ORSWOT style)

We use ORSWOT-style OR-Set:

**State:**
- `entries: Map<Value, Set<Dot>>` (active dots per value)
- `cc: Dvv` (causal context; dots known removed/observed)

**Operations:**
- **Add(value)**: insert a fresh dot into `entries[value]`.
- **Remove(value, ctx)**:
  - remove dots in `entries[value]` dominated by `ctx`
  - merge `ctx` into `cc`

**Merge:**
- `cc = join(cc_a, cc_b)`
- `entries[value] = union(entries_a[value], entries_b[value])`
- drop any dots dominated by merged `cc`
- drop empty entries

**Semantics:** observed-remove (add-wins) with deterministic convergence and bounded DVV size.

Code refs (current):
- Labels LWW today: `src/core/bead.rs:82-115`
- Deps as LWW edges today: `src/core/dep.rs:143-220`, `src/core/state.rs:98-105`

### 2) Dot + DVV definitions

**Dot** (explicit on add ops):

```
Dot {
  replica: ReplicaId,
  counter: u64,
}
```

**DVV**:

```
Dvv { max: BTreeMap<ReplicaId, u64> }
```

Dot `(r,c)` is dominated if `dvv.max[r] >= c`.

### 3) Dot allocation (explicit, persisted)

We do **not** derive dots from `origin_seq`. Dots are minted explicitly and persisted.

**Algorithm (per replica):**

1) Mutation planning reads `orset_counter` from store meta.
2) For each label/dep add op:
   - increment counter
   - set `dot.replica = local replica_id`
   - set `dot.counter = counter`
3) Persist the updated counter **before returning a planned delta**.

This ensures:
- dot uniqueness across restarts,
- no reliance on apply ordering,
- deterministic convergence.

**Persistence:**
- Add `orset_counter: u64` to `StoreMeta` (`src/core/store_meta.rs`).
- Update + write meta in `StoreRuntime` (`src/daemon/store/runtime.rs`).
- If WAL append fails after increment, the counter still advances (holes are OK; reuse is not).

Code refs (current):
- StoreMeta fields: `src/core/store_meta.rs:52-100`
- StoreMeta read/write and version checks: `src/daemon/store/runtime.rs:91-131`, `src/daemon/store/runtime.rs:474-480`

### 4) Orphan ops (labels, deps, notes)

Apply must be total:

- If label/dep/note arrives before bead exists, **store it anyway** keyed by bead_id.
- Queries only show labels/notes for beads that exist; orphans are hidden but preserved.
- Orphans become visible if the bead is created later.

Local mutation (CLI/API) can still enforce “bead must exist” for UX; remote apply must not reject.

Code refs (current):
- Note apply rejects missing bead: `src/core/apply.rs:483-491`
- Dep add rejects missing bead in mutation engine: `src/daemon/mutation_engine.rs:1141-1145`
- Label ops require live bead in mutation engine: `src/daemon/mutation_engine.rs:987-988`

---

## Deterministic collision handling (D4)

Collisions resolve deterministically in **both apply and merge**:

### A) Notes (same bead_id + note_id, different payload)

Winner rule:
1) higher `note.at` (WriteStamp)
2) higher `note.author` (ActorId lexicographic)
3) higher `sha256(note.content)`

### B) Bead creation collisions (same id)

Winner rule:
1) higher `created` stamp
2) if equal, higher `content_hash`

Loser lineage is suppressed with a lineage tombstone.

### C) OR-Set dot collisions (same dot, different value)

Winner rule:
1) lexicographically higher value
2) higher `sha256(dot || value)`

**Logging/metrics:** all collision resolutions should emit a metric/log event (not errors).

Code refs (current):
- Bead collision is an error today: `src/core/apply.rs:115-120`, `src/core/bead.rs:181-199`, `src/core/state.rs:426-434`
- Note collision is an error today: `src/core/apply.rs:504-511`
- NoteLog join keeps left on conflict: `src/core/collections.rs:152-160`

---

## CanonicalState model (new layout)

We decouple scalar bead fields from collections. Notes and labels are no longer embedded in `Bead`.

### CanonicalState (conceptual)

```
CanonicalState {
  beads: Map<BeadId, BeadEntry>,
  tombstones: TombstoneStore,
  collision_tombstones: TombstoneStore,
  labels: LabelStore,
  deps: DepStore,
  notes: NoteStore,
  dep_indexes: DepIndexes, // derived
}
```

### Bead (scalar LWW only)

```
Bead {
  core: BeadCore,
  fields: BeadFields, // scalar LWW only
}
```

### Stores

```
LabelStore: Map<BeadId, LabelState>
LabelState { set: OrSet<Label>, stamp: Stamp }

DepStore: OrSet<DepKey> + stamp

NoteStore: Map<BeadId, Map<NoteId, Note>>
```

### BeadView

Derived view with all data needed for API:

- `labels` (from LabelStore)
- `notes` (sorted by stamp + id)
- `updated_stamp`
- `content_hash`
- `WireBeadFull` serialization

**Updated stamp rule:**

```
updated_stamp = max(
  bead.fields stamps,
  label_store stamp for bead,
  note_store max stamp for bead
)
```

This preserves the existing tombstone resurrection rule in `CanonicalState::join`.

Code refs (current):
- CanonicalState shape and deps map: `src/core/state.rs:92-105`
- Bead embeds NoteLog today: `src/core/bead.rs:138-199`
- updated_stamp used for resurrection: `src/core/state.rs:381-466`

---

## Apply semantics (deterministic + total)

### D1 self-contained ops

- Close/Reopen must explicitly set or clear `closed_reason` and `closed_on_branch`.
- Claim/Extend/Unclaim must explicitly set or clear both assignee and expires.

Apply must **never** read existing state to interpret these ops.

### Notes

- Note append is total: store in NoteStore even if bead missing.
- Collision handled deterministically (no error).

### Labels/Deps

- Apply add/remove ops to OR-Set stores.
- Missing bead is not an error.
- Update store stamp only when OR-Set state changes.

### Collisions

- Bead creation collisions resolved deterministically, not errors.

**Goal:** apply should only fail on malformed encodings or invariant violations unrelated to ordering.

Code refs (current):
- State-dependent workflow/claim: `src/core/apply.rs:421-479`
- Missing-bead note error: `src/core/apply.rs:483-491`
- Dep apply LWW: `src/core/apply.rs:202-242`

---

## Wire and event model (v1 updated in place)

### Removed

- `WireDepV1`, `WireDepDeleteV1`
- `WireBeadPatch.labels`
- `NotesPatch` embedded in bead patch

### Added (v1)

```
WireDotV1 { replica_id: ReplicaId, counter: u64 }
WireDvvV1(Vec<(ReplicaId, u64)>) // sorted by replica

WireLabelAddV1 { bead_id, label, dot: WireDotV1 }
WireLabelRemoveV1 { bead_id, label, ctx: WireDvvV1 }

WireDepAddV1 { from, to, kind, dot: WireDotV1 }
WireDepRemoveV1 { from, to, kind, ctx: WireDvvV1 }
```

**TxnOpV1 variants become:**

```
BeadUpsert(Box<WireBeadPatch>)
BeadDelete(WireTombstoneV1)
LabelAdd(WireLabelAddV1)
LabelRemove(WireLabelRemoveV1)
DepAdd(WireDepAddV1)
DepRemove(WireDepRemoveV1)
NoteAppend(NoteAppendV1)
```

**TxnOpKey** must include dot for add ops to avoid dedup losing distinct dots:

```
LabelAdd { bead_id, label, dot }
DepAdd { from, to, kind, dot }
```

Remove keys can remain `{bead_id,label}` / `{from,to,kind}`.

### Semantic validation (EventBody)

Add validation in `src/core/event.rs`:

- reject workflow/claim patches that rely on Keep for touched fields
- reject full-set label/dep ops (removed)
- enforce limits (notes, labels) as today

Code refs (current):
- NotesPatch + WireBeadPatch.labels exist: `src/core/wire_bead.rs:92-473`
- TxnOpV1 dep ops: `src/core/wire_bead.rs:555-590`
- TxnOpKey shape: `src/core/wire_bead.rs:593-657`
- Event validation only checks sizes/limits: `src/core/event.rs:2000-2061`

---

## Mutation engine (writes CRDT-safe ops)

### D1 (workflow + claim)

- `plan_close`: always set `closed_reason` + `closed_on_branch` (Set or Clear)
- `plan_reopen`: set status Open + clear closure fields
- `plan_unclaim`: clear assignee + expires
- `plan_extend_claim`: explicitly set assignee + expires

### Labels

- `plan_add_labels`: emit `LabelAdd` per label with new dot
- `plan_remove_labels`: emit `LabelRemove` per label with DVV context
- **No patch.labels**

Label limits:
- Enforce at mutation time using `state.labels_for(id)` plus new labels.
- Do not enforce on apply (remote may exceed; still must converge).

### Deps

- `plan_add_dep`: emit `DepAdd` with new dot
- `plan_remove_dep`: emit `DepRemove` with context
- `plan_set_parent`: remove old parents (DepRemove + ctx) then add new parent

Cycle detection:
- Keep local cycle checks using `dep_indexes`.
- Remote apply does not reject cycles; it must remain total.

### Dot minting + DVV context

- Dot minted from `orset_counter` (persisted in StoreMeta).
- For remove, compute ctx from **currently observed dots** for that value:
  - labels: `LabelStore.by_bead[id].set.entries[label]`
  - deps: `DepStore.set.entries[dep_key]`
- DVV context is max counter per replica across those dots.

### Note id generation

- Replace `generate_unique_note_id(&bead.notes, ...)` with a lookup against NoteStore:
  - `state.note_id_exists(bead_id, note_id)`

Code refs (current):
- Full-set label patching: `src/daemon/mutation_engine.rs:981-1036`
- Close/reopen patches: `src/daemon/mutation_engine.rs:1038-1097`
- Dep add/remove + set_parent: `src/daemon/mutation_engine.rs:1128-1277`
- Claim/unclaim/extend_claim: `src/daemon/mutation_engine.rs:1312-1423`
- Note id generation uses NoteLog: `src/daemon/mutation_engine.rs:1286-1305`, `src/daemon/mutation_engine.rs:1992-2000`

---

## Git / checkpoint serialization

We must persist OR-Set metadata (dots + cc) and NoteStore globally.

### Recommended (minimal file kinds)

- `state.jsonl`: beads (scalar fields only)
- `tombstones.jsonl`
- `deps.jsonl`: OR-Set state
- `notes.jsonl`: notes keyed by bead_id
- labels embedded in `state.jsonl` as compact OR-Set blob per bead

**Concrete JSONL shapes (suggested):**

`state.jsonl` bead record:

```
{
  "id":"bd-123",
  "core":{...},
  "fields":{...},
  "labels":{
    "cc":[[replica_id,counter],...],
    "entries":[["label-a", [[replica,counter],...]], ...]
  }
}
```

`deps.jsonl` OR-Set:

```
{"type":"cc","cc":[[replica,counter],...]}
{"from":"bd-a","to":"bd-b","kind":"blocks","dots":[[replica,counter],...]}
```

`notes.jsonl` note record:

```
{"bead_id":"bd-123","note":{...}}
```

### Alternative (more modular)

- Add `labels.jsonl` shard (parallel to deps.jsonl)

We can take the minimal option now and split later if needed.

Code refs (current):
- State/deps/tombstone serialization: `src/git/wire.rs:156-236`

---

## Performance notes / easy wins

- **Eliminate full-set label cloning**: add/remove ops are O(delta), not O(n) sets.
- **Deps become lighter**: no LWW DepEdge provenance, no earliest-created tracking.
- **Updated stamp** can be computed with cached per-bead note/label stamps.
- **Apply errors reduced**: fewer durable-but-unapplied events.

Code refs (current):
- Full-set label clone paths: `src/daemon/mutation_engine.rs:981-1036`
- DepEdge provenance: `src/core/dep.rs:151-220`
- updated_stamp used for ordering and views: `src/core/wire_bead.rs:284-343`, `src/api/issues.rs:289-339`, `src/daemon/query_model.rs:327-377`, `src/daemon/query_executor.rs:686-693`

---

## File-by-file change list (concrete)

### Core

1) `src/core/orset.rs` (new)
   - `Dot`, `Dvv`, `OrSet<V>` (ORSWOT)
   - deterministic dot collision resolution hook

2) `src/core/store_meta.rs`
   - add `orset_counter: u64`

3) `src/core/state.rs`
   - add `LabelStore`, `DepStore`, `NoteStore`
   - remove `deps: BTreeMap<DepKey, DepEdge>`
   - add `bead_view()`, `content_hash_for()`, `updated_stamp_for()`
   - deterministic bead collision handling in `join()`

4) `src/core/bead.rs`
   - remove `notes` from Bead
   - remove labels from `BeadFields`
   - move content hash + updated stamp to `BeadView`

5) `src/core/dep.rs`
   - keep `DepKey`, `DepSpec`
   - delete `DepEdge` / `DepLife` and earliest provenance

6) `src/core/collections.rs`
   - remove `NoteLog` (or leave only Label utilities)

7) `src/core/apply.rs`
   - apply `LabelAdd/LabelRemove`, `DepAdd/DepRemove`
   - note apply total (no MissingBead)
   - deterministic collision handling for notes + bead creation

8) `src/core/wire_bead.rs`
   - new wire ops for labels/deps + WireDotV1/WireDvvV1
   - remove labels/notes from `WireBeadPatch`
   - update `TxnOpKey` (include dot for add ops)

9) `src/core/event.rs`
   - update encode/decode to new op variants
   - semantic validation (D1, no full-set labels/deps)

10) `src/core/composite.rs`
    - add `note_winner` helper

### Daemon

11) `src/daemon/store/runtime.rs`
    - persist `orset_counter` updates in StoreMeta

12) `src/daemon/mutation_engine.rs`
    - D1 patch changes (close/reopen/claim/extend/unclaim)
    - emit OR-Set ops for labels/deps
    - dot minting (StoreMeta `orset_counter`)
    - compute DVV for removes
    - update note id generation to use NoteStore

13) `src/daemon/query_model.rs`, `src/api/issues.rs`, `src/daemon/query_executor.rs`
    - use `BeadView` / `updated_stamp_for` instead of `bead.updated_stamp()`

14) `src/daemon/store/runtime.rs`
    - dirty shard tracking for labels/notes/deps

### Git

15) `src/git/wire.rs`
    - serialize/parse OR-Set state (deps + labels)
    - add `notes.jsonl` shard
    - adjust checksums accordingly

16) `src/git/sync.rs`
    - remove remap collision path; rely on deterministic `CanonicalState::join`

17) `src/git/collision.rs`
    - delete or reduce to no-op (remap no longer needed)

---

## Implementation order (safe sequencing)

1) Add `core/orset.rs`
2) Add LabelStore/DepStore/NoteStore + unit tests
3) Add BeadView and migrate `content_hash` + `updated_stamp`
4) Update wire ops + TxnOpKey + event encode/decode
5) Update apply.rs to new ops + collision handling
6) Update mutation engine to emit new ops
7) Update git/checkpoint serialization
8) Remove old code: DepEdge, NoteLog-in-Bead, full-set label ops, remap collision

---

## Explicit delete list (code we expect to remove)

- `WireBeadPatch.labels` (`src/core/wire_bead.rs:450-452`)
- `NotesPatch` on bead patch (`src/core/wire_bead.rs:92-173`, `src/core/wire_bead.rs:471-472`)
- `WireDepV1` / `WireDepDeleteV1` (`src/core/wire_bead.rs:527-547`)
- `DepEdge` + `DepLife` (`src/core/dep.rs:143-220`)
- `NoteLog` on Bead (`src/core/bead.rs:138-199`, `src/core/collections.rs:120-173`)
- git collision remap (`src/git/collision.rs`)
- full-set label planning in mutation engine (`src/daemon/mutation_engine.rs:981-1036`)

---

## Delete after (codepaths we can fully axe once OR-Set + NoteStore are live)

These are **hard deletions** (not just refactors). They exist only to support
the legacy LWW/full-set paths and should be removed after the new model lands.

### Types / fields / ops

- `NoteLog` type + helpers: `src/core/collections.rs:120-173`
- `Bead.notes` field and note accessors: `src/core/bead.rs:138-199`
- `BeadFields.labels` LWW field: `src/core/bead.rs:82-115`
- Dep LWW types (`DepLife`, `DepEdge`): `src/core/dep.rs:143-220`
- Wire dep ops (`WireDepV1`, `WireDepDeleteV1`): `src/core/wire_bead.rs:527-547`
- Full-set labels in bead patch (`WireBeadPatch.labels`): `src/core/wire_bead.rs:450-452`
- NotesPatch encoding on patches: `src/core/wire_bead.rs:92-173`, `src/core/wire_bead.rs:471-472`

### Apply / merge error paths

- LWW dep apply functions: `src/core/apply.rs:202-242`
- Note missing-bead error path: `src/core/apply.rs:483-491`
- Note collision error path: `src/core/apply.rs:504-511`
- Bead collision error path: `src/core/apply.rs:115-120`
- Bead::join collision error path: `src/core/bead.rs:181-199`
- CanonicalState join “keep a on collision”: `src/core/state.rs:426-434`

### Mutation engine legacy paths

- Full-set label patching (`plan_add_labels`, `plan_remove_labels`):
  `src/daemon/mutation_engine.rs:981-1036`
- Label canonicalization helpers (only used by full-set labels):
  `src/daemon/mutation_engine.rs:1627-1651`
- `normalize_patch` label handling: `src/daemon/mutation_engine.rs:1753-1778`
- Note ID generation against NoteLog: `src/daemon/mutation_engine.rs:1286-1305`,
  `src/daemon/mutation_engine.rs:1992-2000`

### Wire / event legacy branches

- TxnOpV1 DepUpsert/DepDelete variants and keys: `src/core/wire_bead.rs:555-657`
- Event validation branches for dep ops + NotesPatch on patch: `src/core/event.rs:2016-2041`

### Git collision remap

- Entire remap module: `src/git/collision.rs`
- Any call sites in `src/git/sync.rs`

### Tests / fixtures to delete or rewrite

- NotesPatch and dep op fixtures: `tests/integration/fixtures/event_body.rs`


---

## Final acceptance criteria (must hold)

- Apply and merge are deterministic and order-independent.
- Label/dep/note ops never fail due to missing bead.
- No full-set label or dep operations exist in wire or mutation path.
- Dot allocation is explicit and persistent; no implicit derivation.
- Bead creation collisions always converge deterministically and record lineage tombstones.
- Updated stamp includes label and note contributions (preserves resurrection rule).
- Code paths for label/dep LWW or dep provenance are removed.
