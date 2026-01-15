

# beads-rs Project Specification

## 1. Purpose

beads-rs is a **local-first, distributed work-item store** (“beads”) designed primarily for **automated agents** and orchestration tooling. It provides:

* A **machine-safe API** (CLI + local service) for creating and mutating beads.
* **Deterministic, convergent synchronization** across machines using Git repositories as the replication substrate.
* **Strong local concurrency guarantees** under high parallel client load.

The system prioritizes **correctness, determinism, and operability** over human UI ergonomics.

---

## 2. Definitions

* **Bead**: A work item (issue/task) with an ID and structured fields (title, status, etc.).
* **Replica**: One clone of a Git repository containing bead data and running (or capable of running) beads-rs.
* **Actor**: The identity responsible for a mutation (agent/user), represented as an `actor_id`.
* **Canonical store**: The authoritative bead state stored on a dedicated Git reference.
* **Local service**: The per-replica authority that serializes mutations and mediates sync. (Often a daemon, but this spec only cares about behavior.)
* **Mutation**: Any state change (create/update/close/delete/dependency changes).
* **Write stamp**: A totally ordered identifier associated with a mutation or field update, used to resolve concurrent writes deterministically.
* **Tombstone**: A record of deletion that prevents unintended resurrection for a bounded time window (TTL).

---

## 3. High-level requirements

### 3.1 Distributed behavior

1. The system **MUST** support multiple replicas on different machines synchronizing via a shared Git remote.
2. Replicas **MUST** converge to the same bead state after synchronization completes, assuming no further mutations and successful communication.

### 3.2 Local concurrency behavior

1. On a given replica, beads-rs **MUST** provide a single logical serialization point for mutations such that:

   * Concurrent clients cannot corrupt state.
   * Results are equivalent to some **single global order** of those operations (linearization on that replica).
2. The system **MUST** remain correct under at least **50 concurrent clients** issuing mutations and queries in parallel against the same replica.

### 3.3 No manual merge requirement

1. Synchronization **MUST NOT** require manual conflict resolution for valid states produced by beads-rs.
2. If synchronization cannot proceed (e.g., repository damage), beads-rs **MUST** fail with an explicit error and **MUST NOT** leave the canonical store in a partially written or invalid state.

---

## 4. Data model

### 4.1 Bead identity

1. Each bead **MUST** have a globally unique `id` within a repository namespace.
2. `id` **MUST** be stable for the bead’s lifetime and **MUST NOT** be reused after deletion.

#### 4.1.1 Concurrent ID collisions

If synchronization detects two distinct beads with the same `id` (same ID, different content hashes):

1. The system **MUST** resolve the collision deterministically without manual intervention.
2. One bead **MUST** retain the original ID (the "winner").
3. The other bead **MUST** be assigned a new, longer ID that does not collide (the "remapped" bead).
4. The winner **MUST** be determined by write stamp ordering on the bead's `created_at` field, with deterministic tie-break on `created_by` (lexicographic).
5. All dependency edges referencing the remapped bead's old ID **MUST** be rewritten to reference the new ID.
6. The remapped bead's old ID **MUST** be recorded as a tombstone to prevent resurrection of the old ID.

#### 4.1.2 Progressive ID length

The system **MAY** use short IDs that grow in length as the bead count increases.

1. ID generation **MUST** check for collision against current live beads before assignment.
2. If a collision is detected locally during generation, the system **MUST** retry with a different random value (not extend length for a single collision).
3. Length thresholds **MAY** be tuned by implementation.

### 4.2 Bead fields (public schema)

A bead **MUST** expose (at minimum) the following fields through the CLI JSON interface:

* `id: string`
* `title: string`
* `description: string`
* `status: "open" | "in_progress" | "closed"`
* `priority: integer` (0–4 inclusive)
* `type: "bug" | "feature" | "task" | "epic" | "chore"`
* `labels: string[]` (order-insensitive; duplicates are not meaningful)
* `assignee: string | null` (actor id or null)
* `created_at: timestamp`
* `created_by: actor_id`
* `updated_at: timestamp`
* `updated_by: actor_id`
* `closed_at: timestamp | null`
* `closed_by: actor_id | null`
* `closed_reason: string | null`
* `external_ref: string | null` (optional integration pointer)
* `source_repo: string | null` (optional multi-repo provenance)
* `content_hash: string` (hash of the bead’s public content; see §8.5)

Fields beyond this set **MAY** exist, but the above are **MUST** for v1 completeness.

#### 4.2.1 Extended contextual fields

In addition, a conforming implementation **MUST** expose:

* `design: string | null` (technical design notes)
* `acceptance_criteria: string | null` (completion criteria)
* `notes: Note[]` (append-only discussion log; see §4.2.2)
* `created_on_branch: string | null` (git branch name at creation time, informational)
* `closed_on_branch: string | null` (git branch name at close time, informational)

#### 4.2.2 Notes

Each `Note` **MUST** contain:

* `id: string` (unique within the bead)
* `content: string`
* `author: actor_id`
* `at: write_stamp`

Notes are **append-only**:

1. Notes **MUST NOT** be edited after creation.
2. Notes **MUST NOT** be deleted.
3. On merge, notes from all replicas **MUST** be unioned, deduplicated by `id`, and sorted by `at` ascending.

#### 4.2.3 Claim / assignee lease semantics

The following additional fields **MUST** be present:

* `assignee_at: write_stamp | null` (when current assignee claimed the bead)
* `assignee_expires: timestamp | null` (when claim lease expires; wall-clock time)

When an agent claims a bead:

1. `assignee` **MUST** be set to the claiming actor.
2. `assignee_at` **MUST** be set to the current write stamp.
3. `assignee_expires` **SHOULD** be set to current wall-clock time plus lease duration.
4. `status` **SHOULD** be set to `in_progress` if currently `open`.

Claim expiry behavior:

1. If `assignee` is non-null and `assignee_expires` is non-null and the current wall-clock time exceeds `assignee_expires`, the bead **SHOULD** be treated as **unclaimed** for `ready` queries.
2. The system **MAY** automatically clear expired claims during sync or periodic maintenance.
3. Default lease duration is implementation-defined but **SHOULD** be at least 1 hour.

Concurrent claims resolve via write stamp ordering on `assignee_at`. The claim with the higher write stamp wins.

### 4.3 Dependency model

The system **MUST** support directed dependencies between beads.

A dependency edge **MUST** contain:

* `from: bead_id`
* `to: bead_id`
* `kind: "blocks" | "parent" | "related" | "discovered_from"`
* `created_at: timestamp`
* `created_by: actor_id`

Dependency edges **MUST** be uniquely identified by the tuple `(from, to, kind)`.

A dependency edge **MAY** also contain:

* `deleted_at: write_stamp | null` (soft delete timestamp)
* `deleted_by: actor_id | null`

#### 4.3.1 Dependency lifecycle and merge

1. Dependency edges are **soft-deleted** by setting `deleted_at` and `deleted_by`.
2. Deleted edges **MUST** remain in `deps.jsonl` until garbage collection.
3. Queries for active dependencies **MUST** filter out edges where `deleted_at` is non-null.

Concurrent behavior:

1. Concurrent creation of the same edge `(from, to, kind)` is idempotent; the earlier `created_at` wins for provenance.
2. If one replica deletes an edge and another modifies/keeps it:
   * Compare `deleted_at` against the other replica's last observed write stamp for that edge.
   * If deletion is newer, edge is deleted.
   * If modification is newer, edge is restored (deletion is superseded).
3. Tie-break on `deleted_by` vs `created_by` lexicographically if write stamps are equal.

Deleted dependency edges **MAY** be garbage collected after the same TTL as tombstones (§8.4),
when tombstone GC is enabled.

### 4.4 Tombstones

Deletions **MUST** be represented explicitly as tombstones.

A tombstone **MUST** contain:

* `id: bead_id`
* `deleted_at: timestamp`
* `deleted_by: actor_id`
* `reason: string | null`

Tombstones **MUST** be queryable (at least for debugging/validation) and participate in sync semantics (§8.3).

### 4.5 Actor identity

1. Every mutation **MUST** be attributed to a non-empty `actor_id`.
2. `actor_id` **MUST** be stable for the duration of a process and **SHOULD** be stable across restarts on the same machine+agent identity.

---

## 5. Canonical storage in Git

### 5.1 Dedicated reference

1. The canonical store **MUST** live on a dedicated Git reference (the “sync ref”).
2. The sync ref **MUST** be isolated from ordinary code branches (i.e., it must not be `main`, feature branches, etc.).

### 5.2 Canonical tree contents

Each commit on the sync ref **MUST** contain, at minimum:

* `state.jsonl` (live beads)
* `tombstones.jsonl` (deletions; may be pruned if tombstone GC is enabled; see §8.4)
* `deps.jsonl` (dependency edges)
* `meta.json` (format metadata; includes at least a `format_version` integer)

If `deps.jsonl` is not present, dependency features are incomplete and the project is not “complete” under this specification.

#### 5.2.1 Write stamp storage in state.jsonl

Each bead object in `state.jsonl` **MUST** include sufficient write stamp information to perform per-field conflict resolution.

The canonical representation is:

1. Each bead object **MUST** include top-level `_at` and `_by` fields representing the bead-level write stamp (most recent mutation).
2. If any field's write stamp differs from the bead-level write stamp, the bead object **MUST** include a `_v` object mapping field names to their individual write stamps.
3. If all fields share the bead-level write stamp, `_v` **MAY** be omitted (sparse representation).

Example (sparse, common case):

```json
{"id":"bd-a1","title":"fix bug","status":"open","_at":[1702234567890,42],"_by":"claude@mac"}
```

Example (divergent field versions):

```json
{"id":"bd-a1","title":"fix bug","status":"closed","_at":[1702234567890,42],"_by":"claude@mac","_v":{"status":[[1702234599000,43],"alice@laptop"]}}
```

#### 5.2.2 Write stamp encoding

A write stamp **MUST** be encoded as a two-element array: `[physical_component, logical_component]`.

1. `physical_component`: integer, milliseconds since Unix epoch (or equivalent monotonic source).
2. `logical_component`: integer, monotonic counter for tie-breaking within same physical time.

Comparison: write stamps are compared lexicographically as `(physical, logical)` tuples. If equal, tie-break on `actor_id` lexicographically.

During merge:

1. For each field, determine the write stamp from `_v` if present, otherwise from bead-level `_at`/`_by`.
2. Compare write stamps from both replicas.
3. Select the value with the higher write stamp.
4. After merge, recompute `_v`: omit entries where field write stamp equals the new bead-level write stamp.

### 5.3 Deterministic encoding requirements

For all canonical JSON and JSONL files in the sync ref:

1. Encoding **MUST** be UTF-8.
2. JSON objects **MUST** be serialized deterministically (stable key ordering and stable formatting).
3. JSONL files **MUST** be deterministically ordered:

   * `state.jsonl` sorted by bead `id` ascending (bytewise).
   * `tombstones.jsonl` sorted by bead `id` ascending.
   * `deps.jsonl` sorted by `(from, to, kind)` ascending.
4. Every line in JSONL **MUST** end with a single `\n`.

These rules exist to ensure stable diffs and deterministic merge results.

### 5.4 Validity invariant

At all times, the sync ref **MUST** be readable as a self-contained snapshot:

* Parsing `state.jsonl`, `tombstones.jsonl`, `deps.jsonl`, and `meta.json` from any sync-ref commit **MUST** succeed (or the commit is invalid and non-conforming).

---

## 6. Core operations

### 6.1 Required operations (behavioral)

The system **MUST** support the following user-visible operations via CLI:

* **Initialization**: `init` creates necessary local state and establishes sync-ref tracking.
* **Create**: create a new bead with required fields.
* **Update**: update one or more fields of an existing bead.
* **Show**: fetch one bead by ID.
* **List/Search/Ready**:

  * list/filter beads by status, assignee, labels, etc.
  * `ready` returns beads that are eligible to work (see §6.3).
* **Close/Reopen**:

  * close sets status to `closed` and records `closed_at`, `closed_by`, and optional `closed_reason`.
  * reopen sets status back to `open` (or `in_progress` if specified).
* **Delete**:

  * delete removes the bead from `state.jsonl` and adds a tombstone record.
* **Dependency ops**:

  * add dependency edge
  * remove dependency edge
  * compute dependency tree
  * detect cycles (at least as a validation report)
* **Sync**:

  * explicit manual sync operation that brings the local replica into convergence with remote state.

### 6.2 Atomicity of operations

For any mutation operation that returns success:

1. The mutation **MUST** be locally durable (it must survive process crash/restart).
2. The canonical state used for subsequent queries on that replica **MUST** reflect the mutation.

### 6.3 “Ready” semantics

`ready` **MUST** return beads satisfying all of:

1. `status == "open"` (or, optionally, `in_progress` with an expired claim, if implementation chooses to treat expired claims as available).
2. The bead is not deleted (no live tombstone).
3. There exists **no** dependency edge of kind `blocks` such that the blocker bead is not `closed`.

---

## 7. Local query cache (SQLite or equivalent)

1. The system **MAY** maintain a local query index/cache for performance.
2. If present, the cache **MUST NOT** be a source of truth.
3. The cache **MUST** be rebuildable solely from the canonical snapshot stored on the sync ref.
4. If the cache is missing or corrupted, beads-rs **MUST** still be able to function by rebuilding it or operating without it.

---

## 8. Synchronization and merge semantics

### 8.1 Convergence requirement

Given replicas A and B that:

* start from a common valid state,
* apply any number of local mutations independently,
* and repeatedly synchronize with the same remote until no new changes exist,

then A and B **MUST** converge to an identical canonical snapshot (bitwise identical files under the deterministic encoding rules).

### 8.2 Deterministic conflict resolution requirement

When the same bead is concurrently updated on different replicas:

1. Concurrent updates to **different fields** **MUST** be preserved (no “unrelated overwrite”).
2. Concurrent updates to the **same field** **MUST** resolve deterministically using a total order (see §8.6).

This implies the system must track enough per-field (or per-mutation) provenance/version data to satisfy these properties.

#### 8.2.1 Set-valued fields (labels)

For fields with set semantics (e.g., `labels`):

1. The field **MUST** be treated as an atomic value for conflict resolution purposes.
2. Concurrent updates to the same set-valued field **MUST** resolve via write stamp ordering (last-writer-wins on the whole set).
3. Concurrent additions to different elements within the set are **NOT** automatically unioned; the entire set from the winning write stamp is used.

If a future version requires per-element merge semantics, the field **MUST** be migrated to a different structure (e.g., `labels_v2` with per-element write stamps).

### 8.3 Deletion vs modification (“resurrection”) semantics

Deletion is explicit via tombstones.

For a bead `X`:

1. If a tombstone for `X` exists (and has not been garbage-collected), `X` is considered deleted unless a post-delete update is proven newer per the conflict resolution order.
2. A modification **MAY** “resurrect” a bead only if it is ordered strictly after the delete in the merge order.
3. This rule **MUST** be deterministic and identical on all replicas.

### 8.4 Tombstone TTL and garbage collection

1. Tombstones **MUST** be retained by default (no GC) to prevent ID reuse/resurrection.
2. Garbage collection is optional and **MUST** be explicitly configured (e.g., `BD_TOMBSTONE_TTL_MS`). When enabled, TTL **MUST** be at least **7 days**.
3. Tombstones older than TTL **MAY** be garbage-collected.
4. After garbage collection, a replica that has been offline longer than TTL is **not guaranteed** to observe deletes that occurred during its absence (bounded anti-entropy). This is acceptable behavior for a conforming implementation.
5. If GC is enabled, manual reuse of deleted IDs **SHOULD** be avoided to prevent unintended resurrection.

### 8.5 `content_hash` requirement

1. Each bead **MUST** expose a `content_hash` computed over the bead’s **public schema fields** (excluding `content_hash` itself and any internal metadata).
2. For identical public content, `content_hash` **MUST** be identical across replicas.

#### 8.5.1 Content hash computation

`content_hash` **MUST** be computed as follows:

1. Serialize the bead's public schema fields (excluding `content_hash` itself and any internal metadata such as `_v`) as a JSON object.
2. The JSON object **MUST** use deterministic serialization: keys sorted lexicographically, no whitespace, UTF-8 encoding.
3. Compute SHA-256 over the resulting byte string.
4. Encode the hash as lowercase hexadecimal.

Fields included in hash computation (exhaustive):

* `id`, `title`, `description`, `status`, `priority`, `type`
* `labels` (sorted lexicographically before serialization)
* `assignee`, `assignee_expires`
* `design`, `acceptance_criteria`
* `notes` (sorted by `id` before serialization; each note includes `id`, `content`, `author`, `at`)
* `created_at`, `created_by`, `created_on_branch`
* `closed_at`, `closed_by`, `closed_reason`, `closed_on_branch`
* `external_ref`, `source_repo`

Fields **excluded** from hash computation:

* `content_hash`
* `updated_at`, `updated_by`
* `assignee_at`
* `_at`, `_by`, `_v` or any per-field version metadata

For identical public content, `content_hash` **MUST** be bitwise identical across all conforming implementations.

### 8.6 Total ordering (“write stamp”)

To support deterministic merges:

1. Every field update that participates in conflict resolution **MUST** have an associated **write stamp**.
2. Write stamps **MUST** form a **total order** across replicas (ties broken deterministically).
3. For any given actor, generated write stamps **MUST** be monotonic (never decreasing across that actor’s successive mutations).
4. The merge winner for a conflicting field update **MUST** be the one with the maximal write stamp.

(How the stamp is represented or generated is not specified, only these properties; the encoding in §5.2.2 is normative for on-disk representation.)

### 8.7 Dependency merge semantics

1. Concurrent addition of different dependency edges **MUST** result in the union of edges after convergence.
2. If a dependency edge is removed on one replica and added/kept on another concurrently, the winner **MUST** be determined by the same deterministic ordering principles as other conflicts (total order + deterministic tie-break as in §4.3.1).
3. Dependencies **MUST NOT** reference non-existent bead IDs in the converged live state; if they do, validation must report it (§10).

---

## 9. CLI interface requirements

### 9.1 Machine-readable output

1. The CLI **MUST** provide JSON output modes for:

   * show
   * list/search/ready
   * create/update/close/delete results
2. The JSON output schema for beads **MUST** match the public schema (§4.2).

### 9.2 Exit codes

1. Successful operations **MUST** exit with code `0`.
2. Expected user errors (unknown ID, validation failure, CAS failure) **MUST** return non-zero and an error object in JSON mode.

### 9.3 Optional optimistic concurrency check (CAS)

The CLI **SHOULD** support an optional “compare-and-set” parameter using `content_hash`:

* If supplied, the mutation **MUST** be rejected if the current bead’s `content_hash` does not match the provided one.

---

## 10. Validation and repair

### 10.1 Validate

The system **MUST** provide a validation operation that checks:

* Canonical files parse and satisfy deterministic ordering rules.
* No duplicate bead IDs in `state.jsonl`.
* No duplicate tombstone IDs in `tombstones.jsonl`.
* Dependencies reference valid bead IDs (or are explicitly reported as dangling).
* Field types/constraints are respected (status enum, priority range, etc.).

#### 10.1.1 Dangling and orphaned dependencies

A dependency edge is "dangling" if `from` or `to` references a bead ID that:

* Does not exist in `state.jsonl`, AND
* Does not exist in `tombstones.jsonl`.

Validation **MUST** report dangling dependencies as warnings, not errors.

A dependency edge is "orphaned" if `from` or `to` references a bead ID that exists only in `tombstones.jsonl` (target was deleted).

Orphaned dependencies **SHOULD** be reported by validation and **MAY** be automatically removed during garbage collection.

### 10.2 Repair safety

If the system provides an automatic repair mode:

* It **MUST NOT** silently discard user-authored bead content.
* Any destructive repair action **MUST** be explicit and auditable (e.g., a report or a backup commit).

---

## 11. Reliability requirements

1. The system **MUST** be crash-safe:

   * It must not produce partial/invalid canonical files even if interrupted.
2. Synchronization attempts that fail **MUST NOT** corrupt local durable state.
3. If remote push fails due to contention (non-fast-forward), the system **MUST** be capable of retrying to reach convergence without manual conflict resolution (deterministic merge).

---

## 12. Performance requirements

On a typical developer machine (modern laptop/desktop) and a repo with up to:

* 10,000 beads in history,
* ~1,000 live beads,
* ~10,000 dependency edges,

the following **SHOULD** hold:

1. `ready`/`show`/`list` queries complete in under 100ms locally (warm cache).
2. 50 concurrent local clients issuing small mutations do not corrupt state and complete with bounded latency (no unbounded lock contention).
3. Background synchronization batches mutations such that bursty activity does not cause one network push per mutation (i.e., batching behavior is present).

(Exact numbers can be tuned, but “fast enough for swarm orchestration” is required.)

---

## 13. Portability and environment constraints

1. beads-rs **MUST** run on Linux and macOS.
2. beads-rs **MUST** function in environments where invoking external system tools is unreliable (e.g., no assumption that `/usr/bin/git` exists).
3. All local IPC endpoints used for client-to-service communication **MUST** be restricted to the current user by default.

---

## 14. Non-goals (explicitly out of scope for “complete”)

Unless separately specified, beads-rs is **not required** to implement:

* Jira/GitHub/Linear two-way sync adapters
* Human-first TUI/GUI
* Global cross-repo federation (beyond `source_repo` metadata)
* Perfect offline delete propagation beyond tombstone TTL
* Arbitrary custom git merge drivers on user branches

These may be added later, but are not required for completion under this spec.

---

# Conformance checklist (what “done” means)

A beads-rs project is “complete” under this specification when:

* A new repo can be initialized; beads can be created/updated/closed/deleted; dependencies work; `ready` works.
* Canonical state is stored on a dedicated sync ref as deterministic JSONL snapshot files.
* Two machines can concurrently mutate and synchronize and will deterministically converge without manual merges.
* Local concurrency under heavy parallelism is safe (no corruption) and linearized.
* Validation can prove the canonical store is structurally sound.
* The cache is rebuildable and never required as truth.

---
