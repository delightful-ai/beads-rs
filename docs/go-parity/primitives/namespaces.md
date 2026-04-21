# Namespaces as containers for session, external-messaging, and wisp state

**Status:** research + design; no code changes.
**Audience:** implementers deciding whether to ship `sessions`, `external-messaging`, and `wisps` as first-class non-core namespaces.

This file answers three questions:

1. What can the existing beads-rs namespace feature actually do today (versus what it's specified to do on paper)?
2. What would each of the three proposed namespaces require from that feature?
3. Ship all three, stage them, or drop them in favor of typed variants plus a per-bead retention hint?

---

## 1. Current namespace feature in beads-rs

### 1.1 The claim vs. the code

The user's framing — "Rust has a namespace feature, and it was designed especially for this" — is **half true**. The spec (`REALTIME_SPEC_DRAFT.md:215-252`) and the types (`crates/beads-core/src/namespace.rs`) describe a rich per-namespace policy model with sync, retention, visibility, ready-eligibility, and GC authority dimensions. The types exist, they parse, they round-trip through config, and they survive an admin `reload_policies` call.

But most of the policy fields are **defined, parsed, and diffed — not enforced**. `persist_to_git`, `retention`, `visibility`, `ready_eligible`, `gc_authority`, and `ttl_basis` are each read in exactly two places today: the struct literal in `core_default()`/`sys_default()`/`wf_default()`/`tmp_default()` and the diff printer in `runtime/admin/policy_reload.rs`. Only `replicate_mode` is actually consumed by behavior (by the durability coordinator, `crates/beads-daemon-core/src/durability.rs:264-323`). Treat the rest as a scaffolded surface waiting for enforcement, not a finished feature.

### 1.2 `NamespaceId`: parse rules and default

Defined in `crates/beads-core/src/namespace.rs:10-75`.

- String wrapper, `#[serde(try_from = "String", into = "String")]`.
- Must be non-empty, `<= 32` bytes.
- First byte must be `[a-z]`; remaining bytes `[a-z0-9_]`.
- Default namespace string is `"core"` (`NamespaceId::CORE`, line 15).
- `NonCoreNamespaceId` wrapper (lines 77-88) exists precisely so call sites can prove at compile time they are not operating on `core`. Current uses of this wrapper are sparse; most code still passes `NamespaceId` and checks at runtime.

Validation: `NamespaceId::parse` (lines 18-54) and test in lines 329-352. Reserved names (`core`, `sys`, `wf`, `tmp`) are seeded by `beads-bootstrap::config::schema::default_namespace_policies()` (`crates/beads-bootstrap/src/config/schema.rs:567-583`) but the parser does not reserve them — any well-formed string is legal.

### 1.3 How state is partitioned

One `StoreState` per store, with a `BTreeMap<NamespaceId, CanonicalState>` inside it (`crates/beads-core/src/namespaced_state.rs:10-13`). `core` is always present (line 38). `CanonicalState` itself is namespace-ignorant: it holds beads, tombstones, deps, labels, and notes for exactly one namespace.

Key consequence: the `Bead` struct carries **no namespace field** (`crates/beads-core/src/bead.rs:52-57, 158-162`). `DepKey` carries no namespace either (`crates/beads-core/src/dep.rs:256-261`). The namespace is property of the *event* (`crates/beads-core/src/event.rs:150-161`, field `namespace: NamespaceId`) and of the *state partition* the event gets applied to.

CRDT merge is per-namespace: `StoreState::join` in `namespaced_state.rs:14-33` takes the union of namespace keys and calls `CanonicalState::join` pairwise within each namespace. Namespaces never merge across each other — a bead with the same `BeadId` in two different namespaces is two independent beads with independent lineages, independent tombstones, and independent lineage-scoped collision resolution.

### 1.4 Can a bead in `N1` reference a bead in `N2`?

**Not cleanly, and today's answer is "silently allowed but tooling-hostile."**

- `DepKey = (BeadId, BeadId, DepKind)` (`crates/beads-core/src/dep.rs:256-261`) has no namespace qualifier on either endpoint.
- `apply_dep_add` (`crates/beads-core/src/apply.rs:296-309`) operates on a single `CanonicalState`, so a dep edge added by an event in namespace `N1` lives entirely in `N1`'s dep store. The `to` endpoint is just a `BeadId` — the CRDT has no way to mark it as "points into `N2`."
- `BeadView` / `IssueSummary` / `DepEdge` (`crates/beads-api/src/issues.rs:209, 260`) render `namespace` off the *querying* namespace (the one the API caller is reading from), not off the ref target. The CLI uses this to prefix non-core refs (`crates/beads-cli/src/commands/show.rs:306-308, 600-605`).
- Result: a dep in namespace `sessions` pointing at `BeadId` `bd-foo` will render as if `bd-foo` also lives in `sessions`. If `bd-foo` actually lives in `core`, lookups from `sessions` will silently miss it and the edge is structurally an orphan — no validation error at create time, no render cue at read time.

**Verdict:** cross-namespace dep/parent references are a design gap. They are not prohibited; they are not modeled; they appear to work until you try to traverse. See §3 for the implications.

### 1.5 Current sync model

One git ref per **checkpoint group**, not per namespace:

- Legacy ref: `refs/heads/beads/store` (`crates/beads-git/src/sync.rs:60, 377, 443, …`). Today's default sync still uses this name.
- New scheme: `refs/beads/{store_id}/{group}` (`crates/beads-daemon/src/runtime/core/helpers.rs:488-499`).
- `CheckpointGroup` (`crates/beads-core/src/namespace.rs:312-323`) bundles `namespaces: Vec<NamespaceId>` with one `git_ref`, one `git_remote`, a writer set, and debounce tuning. The default config seeds exactly one group — `core` containing `[NamespaceId::core()]` (`crates/beads-bootstrap/src/config/schema.rs:585-591`).
- Checkpoint import/export is multi-namespace aware: `CheckpointMeta.namespaces: NamespaceSet` (`crates/beads-git/src/checkpoint/meta.rs:63, 82`) and the import path dispatches per-namespace state via `state_for_namespace` (`crates/beads-git/src/checkpoint/import.rs:168-172, 228-302`).

**So:** namespaces inside one group replicate together via a shared ref. Namespaces in different groups replicate via different refs. That's real multi-ref support, already implemented in the checkpoint format and the group scheduler. It is *not* exercised by default config — production runs have exactly one group with exactly one namespace.

The WAL is per-(namespace, origin, seq) (`docs/CRDT_AUDIT.md:106, 108`; `crates/beads-daemon-core/src/wal/index.rs` uses the `EventId(origin, namespace, seq)` tuple). Each namespace has its own WAL segments under `wal/{namespace}/`, listed by `list_namespaces` (`crates/beads-daemon-core/src/wal/replay.rs:909`). Replication frames carry namespace per event.

### 1.6 Current retention / GC model

There is exactly **one** GC path implemented today, and it is not policy-driven:

- Tombstone GC by TTL runs in `crates/beads-git/src/sync.rs:547-550`, controlled by a `tombstone_ttl_ms: Option<u64>` on `SyncConfig` (line 243), parsed from a single config entry (lines 257-276). That number is repo-global, not per-namespace.
- `RetentionPolicy::{Forever, Ttl{ttl_ms}, Size{max_bytes}}` (`crates/beads-core/src/namespace.rs:218-223`) is defined and parsed from TOML, but nothing reads the field outside `policy_reload.rs:103-110, 182-188` (diff printing) and the defaults constructors.
- No "GC eligible" concept exists at the bead level. No sweep task. No `gc_authority` wiring.

This is the largest gap between the spec and the code. The wisps story the user wants — GC-eligible, retention as a namespace property, not a per-bead flag — does not have an engine today.

### 1.7 IPC-level awareness

- `MutationMeta.namespace: Option<NamespaceId>` and `ReadConsistency.namespace: Option<NamespaceId>` (`crates/beads-surface/src/ipc/types.rs:22-25, 72-76`) flatten into every request.
- Unknown namespace rejects: `ReadScope::normalize_namespace` (`crates/beads-daemon/src/runtime/core/read.rs:23-33`) returns `OpError::NamespaceUnknown` (`crates/beads-daemon/src/runtime/ops.rs:163, 240, 290, 531-536`) when the policy map does not contain the requested namespace. The `NamespaceUnknown` error wire code exists in the replication path too (`crates/beads-daemon/src/runtime/core/replication.rs:331-335`).
- `AdminStatusOutput.namespaces: Vec<NamespaceId>` (`crates/beads-api/src/admin.rs:36`) lets a client enumerate namespaces the daemon knows about. There is no dedicated `list_namespaces` admin op; the status call carries it.
- Replication roster entries support `allowed_namespaces: Option<Vec<NamespaceId>>` (`crates/beads-core/src/replica_roster.rs:103, 125`; `crates/beads-bootstrap/src/config/schema.rs:509`) — peers can be scoped to subsets.
- `admin reload-policies` (`crates/beads-surface/src/ipc/types.rs:136-141`, `crates/beads-daemon/src/runtime/admin/policy_reload.rs`) re-parses `namespaces.toml` and diffs; retention / visibility / ready_eligible / replicate_mode are "safe" reloads; `persist_to_git` and `gc_authority` changes require restart.

The CLI exposes a global `--namespace` flag (`crates/beads-cli/src/cli.rs:50-52`) with config fallback via `DefaultsConfig.namespace` (`crates/beads-rs/src/cli/mod.rs:218-240`). No command today takes a namespace *argument*; all commands implicitly use the global flag.

### 1.8 Existing multi-namespace coverage

The unit layer has solid coverage: `namespace.rs` tests (lines 329-428), `namespace_policies.rs` tests (lines 49-157), `namespaced_state.rs` tests (lines 104-139), `canonical_cbor` tests over events carrying non-core namespaces.

End-to-end coverage is thin. Searches for `NamespaceId::parse("wf"` / `"sys"` / `"tmp"` turn up only unit tests and config defaults — no integration test that creates a bead in a non-core namespace via the CLI, replicates it, reads it back, and exercises retention. The daemon reload test uses `core_policy.ready_eligible = false` (`crates/beads-daemon/src/runtime/core/mod.rs:2120`, `crates/beads-rs/tests/integration/daemon/admin.rs:319-344`) but mutates only the core namespace's policy.

### 1.9 Ground truth summary

| Dimension | State |
| --- | --- |
| `NamespaceId` parse + validation | Solid. Tests cover happy and sad paths. |
| Per-namespace `CanonicalState` partitioning | Solid. `StoreState::join` merges per-namespace. |
| `Bead` / `DepKey` carry namespace | **No.** Namespace lives on the *event* and the *state partition*. |
| Cross-namespace dep/parent refs | Not modeled. Silently allowed, silently orphaned on traversal. |
| Git sync per-namespace | Via checkpoint groups. Multi-group is implemented; default config uses a single group with just `core`. |
| WAL per-namespace | Yes; per-(namespace, origin, seq). |
| Replication: per-peer namespace ACL | `allowed_namespaces` on roster entries. |
| `RetentionPolicy` enforcement | **No.** Field exists, no sweeper reads it. |
| `persist_to_git` enforcement | **No.** Field exists, no git worker reads it. |
| `ready_eligible` enforcement | **No.** Field exists, no query path consults it. |
| `visibility` enforcement | **No.** Field exists, no CLI/render path consults it. |
| `gc_authority`, `ttl_basis` enforcement | **No.** Fields exist, no GC path consults them. |
| IPC: `namespace` on read + mutate | Yes. Unknown-namespace error wired end-to-end. |
| IPC: list/enumerate | Only as a field inside `AdminStatus`. |
| CLI: first-class per-command namespace UX | `--namespace` global flag only. No `bd ns ls`, no "create in ns X" command. |

Honest characterization: **the namespace skeleton is well-designed and well-partitioned, and the scaffolding for richer policy exists, but most policy dimensions are inert today. Anything beyond "partition the state graph" needs new enforcement code.**

---

## 2. What each proposed namespace needs

Membership and Group beads — per the user's framing — stay in `core` because they are durable coordination state. The three new namespaces host the cross-cutting state that would otherwise clutter `core`.

### 2a. `sessions` namespace

**Contents:** Session, WorkerProfile, Wait, NudgeQueue, TranscriptEntry beads (see `docs/go-parity/primitives/metadata-remapping.md:374-459` for the typed shapes).

**Policy dimensions required:**

- `persist_to_git = false` (transcripts are per-machine runtime-adjacent state; they should not follow the code repo around). The *spec* allows this; the *implementation* doesn't filter checkpoint export by `persist_to_git` today. **Gap.**
- `replicate_mode = None` or `Anchors` — depends on whether session state is local-to-replica or shared across the replica set. Default should be `None` for this namespace: a human operator working on machine A should not see their transcripts replicated to machine B's daemon. The *spec* supports this; replication enforcement via `allowed_namespaces` already exists (`crates/beads-core/src/replica_roster.rs:103`).
- `retention = Ttl { ttl_ms: 7 days }` for TranscriptEntry; Sessions themselves might be `Forever` until archived. This is heterogeneous *within* one namespace, which is not a dimension the current policy type supports — `RetentionPolicy` is per-namespace, not per-BeadType. **Gap.**
- `ready_eligible = false` — Session/Wait/Nudge beads should not appear in `bd ready`. No enforcement today. **Gap.**
- `visibility = Normal` or `Pinned` — session beads probably want `Normal` (they're discoverable) but should not leak into a generic `bd list`. **Gap.**

**Cross-namespace reference model:**

- Sessions need to point at mainline workflow beads: "this session is working on `bd-1234` in `core`." The canonical place for this in gascity is a dep edge. Today's model would require extending `DepKey` (or adding a wrapping edge table) to carry `(from_namespace, to_namespace)`. Without that, the edge exists in `sessions`'s dep store but the `to` target resolves ambiguously. **Hard dependency on cross-namespace refs.** See §3.
- Wait beads reference Nudge beads — same namespace, no concern.
- WorkerProfile sits on Session — same namespace, no concern.

**Open question flagged by user, resolved here:** TranscriptEntry almost certainly wants looser sync than core. The spec's `tmp_default()` (`replicate_mode = None`, `persist_to_git = false`, `retention = ttl:1d`) is close to right for transcripts; Sessions themselves want something richer (7d+ retention, anchors replication so a restart on the same box can recover). This *does* argue for splitting sessions further into `sessions` (long-lived spec/lifecycle) and `sessions_transcript` (ephemeral log). The current `RetentionPolicy` is per-namespace; two namespaces is how you model two retention classes.

### 2b. `extmsg` namespace

**Contents:** Binding, Delivery records (see `docs/go-parity/primitives/metadata-remapping.md:501-528`).

**Policy dimensions required:**

- `persist_to_git = true` — bindings must survive crashes and clone. Currently unenforced but the default is already to persist everything, so this is a no-op until enforcement lands.
- `replicate_mode = Peers` — extmsg state is shared across the replica set.
- `retention = Forever` for bindings; `Ttl { ttl_ms: 30d }` for deliveries. Same heterogeneity problem as §2a — delivery records are short-lived and bindings are not. Again, two-namespace split (`extmsg` vs `extmsg_delivery`) may be the right shape.
- `ready_eligible = false` — not workflow beads.
- `visibility = Normal`.

**Cross-namespace reference model:**

- Binding beads point at the mainline bead they bind work to: `(extmsg:binding-foo) -> core:bd-1234`. Same hard dependency on cross-namespace refs.
- Delivery records point at Binding records: same namespace, no concern.

**Open question flagged by user:** "Do Delivery records need to sync globally, or per-recipient-machine?" Gascity's `DeliveryContextRecord` (`docs/go-parity/primitives/metadata-remapping.md:514`) is per-delivery-attempt context — a replica that has never been the recipient has no reason to store it. **Default `replicate_mode = Anchors`** (only replicas with an anchor role store it) would match gascity's intent better than blanket `Peers`. This *is* supported by the current type system.

### 2c. `wisps` namespace

**Contents:** any bead created explicitly as ephemeral/GC-eligible — heartbeat pings, patrol cycle reports, GC reports, recovery action records, error reports, human escalations (per `docs/go-parity/types/wisp.md:31-34, 69-78`).

**Design constraint from the user:** retention is a *namespace property*, not a per-bead flag. Wisps should be GC-*eligible*, not GC-*default* — they survive until the sweeper fires.

**Policy dimensions required:**

- `retention = Ttl { ttl_ms: ??? }` — but real wisp usage (per `docs/go-parity/types/wisp.md:69-77`) has seven distinct TTL classes (Heartbeat 6h, Ping 6h, Patrol 24h, GcReport 24h, Recovery 7d, Error 7d, Escalation 7d). A single `ttl_ms` on the namespace cannot express that. Options:
  - **Multiple wisp namespaces** — `wisps_6h`, `wisps_24h`, `wisps_7d`. Honest, namespace-as-retention-class. Three namespaces just for wisps.
  - **Extend `RetentionPolicy`** — add variants like `TtlClasses { classes: BTreeMap<WispTtlClass, u64> }` plus a per-bead field saying which class applies. That's per-bead retention hint again, which the user said they want to avoid. **This contradicts the stated design goal.**
  - **Move TTL class to BeadType variant data** — each wisp bead-type carries its class; namespace policy says "wisps" and "respect the per-type class." Still per-bead TTL, just hidden in the type. Same contradiction.
  - **Honest answer:** if retention really is a namespace property, accept three wisp namespaces (one per retention class). The spec supports arbitrary namespace count (§1.2). The main downside is list-every-namespace operational friction, which is a tooling concern, not a correctness one.
- `persist_to_git` — user is unsure. Wisps as "local-to-replica, lost on crash" matches `tmp_default()` (`crates/beads-core/src/namespace.rs:297-309`, `persist_to_git = false, replicate_mode = None`). That *is* the gascity pattern for truly ephemeral state. For a wisp that records an error report meant for a human reviewer, though, you want `persist_to_git = true` and `replicate_mode = Anchors`. Again, different wisp categories want different treatment — which argues for named-by-retention-class namespaces rather than one `wisps` namespace.
- `ready_eligible = false`, `visibility = Normal`.
- `gc_authority = CheckpointWriter` or `ExplicitReplica` — the sweeper must run exactly once per tick per namespace. Not enforced today; would need a new scheduler.

**Cross-namespace reference model:**

- Wisps often point at the workflow bead they're about: `(wisps:error-foo) -> core:bd-1234`. Cross-namespace ref.
- Or at a session: `(wisps:heartbeat-foo) -> sessions:session-foo`. Two-hop cross-namespace.

**Key infrastructure gaps (all wisp namespaces share these):**

- **No GC sweeper exists.** `RetentionPolicy::Ttl` is defined but not enforced anywhere. Must be built.
- **No per-namespace `persist_to_git` filtering in the checkpoint path.** The exporter exports the whole `StoreState`; selective inclusion is not implemented.
- **No per-bead "last touched" write-stamp read by a GC sweeper.** `TtlBasis::LastMutationStamp` is the default and the data is available (`Bead::fields.all_stamps()`), so this one is cheap — but it's not wired.

---

## 3. Architectural implications

### 3.1 Complexity cost, concretely

Three namespaces beyond `core` means:

- **Zero new IPC ops.** Existing `namespace` flattening on every request covers reads and mutations. A future `bd ns ls` command is a nice-to-have but not required for function.
- **Zero new WAL structure.** WAL is already per-(namespace, origin, seq) and `list_namespaces` already enumerates.
- **Zero or one new git refs.** The decision is whether to put `sessions`, `extmsg`, `wisps` in the existing `core` checkpoint group (one ref) or split into separate groups (more refs). Default is probably one group per retention/replication class: `{core, extmsg}` together; `{sessions, wisps}` each on their own ref, because neither should follow the code repo's `main` branch lifecycle.
- **One new enforcement path: retention GC.** This is the load-bearing gap. Without it, `wisps` is ambitious marketing. Estimate: medium-sized feature. Needs a daemon-side sweeper that walks `StoreState` per namespace, reads `RetentionPolicy`, applies `TtlBasis` against `CanonicalState` per-bead stamps, emits tombstone events. Not complicated; not implemented.
- **One new enforcement path: cross-namespace refs.** Either (a) extend `DepKey` to carry both namespaces and migrate existing WAL/checkpoint/state; or (b) add a new `CrossNamespaceRef` edge table stored in whichever namespace owns the edge; or (c) decide cross-namespace refs are always by **opaque BeadId only** and accept that traversal from `sessions` to `core` requires the caller to state the target namespace explicitly.
  - Option (c) is the cheapest and the one most consistent with the current code. It's also the most hostile to users who expect `bd show sessions/session-foo` to follow the edge to `core/bd-1234` without hand-holding. Recommend option (c) for the first cut, with CLI affordances that prompt for the target namespace, and revisit later if traversal pressure grows.
- **One new UX path: namespace-qualified bead refs.** The CLI already renders non-core namespaces with a `ns/` prefix in `fmt_show_issue_ref` (`crates/beads-cli/src/commands/show.rs:600-605`). What it does not do is *accept* a `ns/id` form on the input side — all ID lookup today assumes the namespace from `--namespace` or config default. Minor UX work.

### 3.2 Per-namespace CRDT state tree

Already yes: `StoreState { by_namespace: BTreeMap<NamespaceId, CanonicalState> }` (`crates/beads-core/src/namespaced_state.rs:10-13`). The merge is strictly pairwise within each namespace key; adding namespaces does not grow `CanonicalState::join` complexity. What prevents a query in `N1` from seeing `N2`'s beads: `Daemon::namespace_state` (`crates/beads-daemon/src/runtime/core/repo_access.rs:44-54`) picks one `CanonicalState` by namespace; the query executor never reads others.

The one place this leaks is the `IssueSummary`/`Issue` shape — `namespace` is stamped by the *reading* namespace, not by where the bead actually lives. For cross-namespace refs (§3.1 last bullet) this will matter; for standalone namespace queries it does not.

### 3.3 Does `beads-git` already support multi-namespace?

**Yes — the sync format, not the defaults.**

- Checkpoint meta (`crates/beads-git/src/checkpoint/meta.rs:63, 82, 155`) carries `NamespaceSet`. Default construction seeds `vec![NamespaceId::core()]`.
- Checkpoint import (`crates/beads-git/src/checkpoint/import.rs:201-315`) iterates the manifest's `namespaces` and dispatches per-namespace via `state_for_namespace`. Rejects manifest paths whose namespace isn't in the allowed set. This path is covered by `commutativity` tests (per `docs/CRDT_AUDIT.md:124-125`).
- Checkpoint export (`crates/beads-git/src/checkpoint/export.rs`) iterates namespaces of the state being exported.
- The wire format does not filter by `persist_to_git`. If a namespace is in the group, it's in the ref. Adding `persist_to_git` filtering needs a branch point in either the checkpoint scheduler (exclude the namespace from the group) or the exporter (include the namespace in the group but elide its content).

Cleanest approach: `persist_to_git = false` means "do not put this namespace in any checkpoint group whose `durable_copy_via_git = true`." Enforce at group assembly time, not at export time.

### 3.4 "Just add another namespace for X"

Real risk. The spec already lists four reserved names (`core`, `sys`, `wf`, `tmp`), only one of which is used by default. Adding `sessions`, `extmsg`, `wisps` (or worse, `wisps_6h`, `wisps_24h`, `wisps_7d`) brings us to 7-9. Discipline needed.

**Proposed decision rule for a new namespace:**

A new namespace is warranted iff at least **two** of the following are true *and* at least one is a policy-dimension difference, not just a taxonomy preference:

1. The contents need a different `replicate_mode` than `core`.
2. The contents need a different `retention` than `core`.
3. The contents need a different `persist_to_git` than `core`.
4. The contents should be excluded from `ready_eligible` by default.
5. The contents form a coherent subsystem with its own lifecycle (session lifecycle, extmsg lifecycle, wisp lifecycle) that a human operator should be able to purge, migrate, or disable wholesale.

If only taxonomy (5) holds — "these beads feel different, so they should have their own namespace" — refuse. Use `BeadType` variants instead. Types tell the truth; a new namespace needs its policy profile to differ from its neighbors.

By this rule:

- `sessions` passes: different `persist_to_git` (false), different `replicate_mode` (None/Anchors), coherent subsystem.
- `extmsg` passes: different `retention` for deliveries, coherent subsystem.
- `wisps` passes: different `retention`, different `persist_to_git` for ephemeral classes.
- Membership, Group, Convoy, Gate, etc. — stay in `core`. They have the same policy profile as `core` beads; they're just different types.

---

## 4. Decision matrix + recommendation

### 4.1 Matrix

| Namespace | Required policy features | Required new enforcement | Cost | Hard deps on missing features | Risk if wrong |
| --- | --- | --- | --- | --- | --- |
| `sessions` | `persist_to_git=false`, `replicate_mode=None`, retention split for transcripts, `ready_eligible=false` | Checkpoint-group filter by `persist_to_git`; per-namespace exclusion from `ready` query; cross-namespace refs to `core` | **Medium** | `persist_to_git` enforcement in checkpoint group assembly; cross-namespace ref model (can defer via opaque-BeadId approach) | Low-medium. Getting replication mode wrong leaks transcripts to other machines. Getting retention wrong grows unbounded. Recoverable via cleanup. |
| `extmsg` | `replicate_mode=Anchors` for deliveries, `retention=Forever` for bindings, `ready_eligible=false` | Per-namespace `ready_eligible` filter; replication anchor gating (already partially in `durability.rs`) | **Small** | Retention split for delivery vs binding — solvable by splitting into two sibling namespaces, or accepting binding+delivery share one retention class until it hurts | Medium. Bindings are durable coordination state; if they aren't replicated correctly, two replicas can double-assign work. But this failure mode already exists in gascity and is well-understood. |
| `wisps` | Retention TTL enforcement per class (6h/24h/7d); `persist_to_git=false` for most classes; `replicate_mode=None` or `Anchors` depending on class | **GC sweeper.** Per-class namespace enforcement. `persist_to_git=false` checkpoint filter. | **Large** | GC sweeper (not implemented). Per-class retention model (doesn't fit in one namespace; use three wisp namespaces). Cross-namespace refs back to `core`/`sessions` (defer). | Medium-high if the sweeper is wrong. An over-eager sweep deletes live data; an under-eager sweep fills disk. The spec's `TtlBasis` + `GcAuthority` fields anticipate this; they're unimplemented. |

### 4.2 Recommendation

**Ship sessions-namespace + extmsg-namespace first; defer wisps until the GC sweeper exists.**

Reasoning:

1. The first two mostly exercise machinery that *is* implemented — per-namespace CanonicalState partitioning, WAL per-namespace, checkpoint group multi-namespace, replication `allowed_namespaces`. They require small-to-medium enforcement additions (`persist_to_git` filter at checkpoint-group assembly; `ready_eligible` filter at ready-query time). Neither needs a new GC engine.
2. Wisps exist *because* of GC. Without a sweeper, wisps are a namespace that slowly fills disk — strictly worse than today's "no wisps exist and nothing fills." The correct ordering is: build the GC sweeper, wire `RetentionPolicy` enforcement, then introduce `wisps_*` namespaces.
3. Dropping namespaces entirely in favor of typed variants plus per-bead retention is what the user explicitly wants to avoid (§2c design constraint, and per `docs/go-parity/types/wisp.md:40-55`). Typed-variant-only would re-smear the retention decision across every bead, which is the Go pattern beads-rs is trying to leave behind.

**Alternative considered and rejected: ship all three at once.** Pros: one migration, one mental model, matches the user's stated target. Cons: requires the GC sweeper *now*, and without it `wisps` is a footgun. If GC sweeper lands quickly (~1-2 weeks of work, plus tests), the alternative is fine.

**Alternative considered and rejected: drop namespaces for these, use typed variants plus per-bead `Ephemerality` field.** Pros: zero new infra. Cons: contradicts scatter philosophy (retention as per-bead flag = smearing); contradicts the wisp doc's own conclusion; loses the per-namespace replication scoping which is the cleanest place to express "transcripts stay on this machine."

### 4.3 Concrete next steps

1. **File a bead: enforce `persist_to_git`.** Checkpoint-group assembly filters out namespaces whose policy has `persist_to_git = false` from any group whose `durable_copy_via_git = true`. Add an integration test that creates a bead in `tmp`, verifies it is absent from `refs/beads/{store_id}/core`.
2. **File a bead: enforce `ready_eligible` in ready queries.** `compute_ready` reads the active namespace's policy and short-circuits if `ready_eligible = false`. Add a test that creates a Session-like bead in a `ready_eligible=false` namespace, verifies `bd ready` excludes it.
3. **File a bead: decide cross-namespace ref model.** Option (c) — opaque `BeadId`-only refs with explicit target-namespace resolution in the CLI — is the low-cost path. Write the ADR; update `DepKey` docstring to reflect that `to: BeadId` is always a same-namespace target; add a `bd show ns/id` form that qualifies the target explicitly for readers.
4. **File a bead: add `sessions` namespace as the first real non-core namespace.** Seed it in `default_namespace_policies()`; add smoke tests for create-in-sessions, list-in-sessions, replicate-in-sessions-none.
5. **File a bead: extmsg namespace.** Decide 1-namespace vs 2-namespace (`extmsg` + `extmsg_delivery`). Prefer 1 unless delivery retention pressure is observed.
6. **File an epic: GC sweeper + wisps.** Design the sweeper. Decide one-namespace-per-retention-class vs one-wisps-namespace-with-richer-policy. Implement `RetentionPolicy::Ttl` enforcement, `TtlBasis::LastMutationStamp` evaluation, `GcAuthority::CheckpointWriter` single-writer guard, tombstone emission. Then introduce wisp namespaces.

Until step 6 lands, `wisps` should not exist as a reserved name.

---

## Source index (claims → files)

- `NamespaceId` parse + validation: `crates/beads-core/src/namespace.rs:10-75`, `:329-352`
- Non-core wrapper: `crates/beads-core/src/namespace.rs:77-95`, `:120-125` (in tests)
- `NamespacePolicy` fields: `crates/beads-core/src/namespace.rs:245-310`
- Policy defaults (`core`, `sys`, `wf`, `tmp`): `crates/beads-core/src/namespace.rs:259-310`; `crates/beads-bootstrap/src/config/schema.rs:567-583`
- `NamespacePolicies` TOML parse: `crates/beads-core/src/namespace_policies.rs:1-47`
- `StoreState` per-namespace map: `crates/beads-core/src/namespaced_state.rs:10-13`
- `StoreState::join`: `crates/beads-core/src/namespaced_state.rs:14-33`
- `CanonicalState` has no namespace: `crates/beads-core/src/state/canonical.rs` (no namespace references)
- `Bead` has no namespace field: `crates/beads-core/src/bead.rs:52-57, 158-162`
- `DepKey` has no namespace: `crates/beads-core/src/dep.rs:252-261`
- `apply_event` per-namespace: `crates/beads-core/src/apply.rs:60-103`; callers at `crates/beads-daemon/src/runtime/core/helpers.rs:324, 357`, `crates/beads-daemon/src/runtime/executor.rs:245, 442`, `crates/beads-daemon/src/runtime/core/repl_ingest.rs:195, 202`
- Event carries namespace: `crates/beads-core/src/event.rs:149-161, 840-920`
- `IssueSummary.namespace` from read context: `crates/beads-api/src/issues.rs:260, 290-340`
- IPC `MutationMeta.namespace` / `ReadConsistency.namespace`: `crates/beads-surface/src/ipc/types.rs:22-37, 72-80`
- Unknown-namespace error: `crates/beads-daemon/src/runtime/core/read.rs:23-33`; `crates/beads-daemon/src/runtime/ops.rs:163, 240, 531-536`
- CLI `--namespace` global flag: `crates/beads-cli/src/cli.rs:50-52`
- CLI non-core render prefix: `crates/beads-cli/src/commands/show.rs:306-308, 600-605`
- Legacy single ref: `crates/beads-git/src/sync.rs:60, 377, 443, 646, 748, 1143, 1390`
- New per-group ref scheme: `crates/beads-daemon/src/runtime/core/helpers.rs:488-499`
- `CheckpointGroup`: `crates/beads-core/src/namespace.rs:312-323`
- `CheckpointMeta.namespaces`: `crates/beads-git/src/checkpoint/meta.rs:63, 82, 155`
- Checkpoint import multi-namespace: `crates/beads-git/src/checkpoint/import.rs:168-315`
- Default checkpoint group: `crates/beads-bootstrap/src/config/schema.rs:585-591`
- Tombstone GC by TTL (repo-global): `crates/beads-git/src/sync.rs:243, 257-276, 547-550`
- `RetentionPolicy` unused outside defaults+diff: `crates/beads-core/src/namespace.rs:218-223`; `crates/beads-daemon/src/runtime/admin/policy_reload.rs:103-110, 182-188`
- `persist_to_git` unused outside defaults+diff: `crates/beads-daemon/src/runtime/admin/policy_reload.rs:121-127`
- `ready_eligible` unused outside defaults+diff+tests: `crates/beads-daemon/src/runtime/admin/policy_reload.rs:94-101`; `crates/beads-daemon/src/runtime/core/mod.rs:2120`
- `ReplicateMode` consumed by durability: `crates/beads-daemon-core/src/durability.rs:264-323`
- Replica roster `allowed_namespaces`: `crates/beads-core/src/replica_roster.rs:103, 125`; `crates/beads-bootstrap/src/config/schema.rs:509`
- `AdminStatusOutput.namespaces`: `crates/beads-api/src/admin.rs:36`
- `reload_policies` admin op: `crates/beads-surface/src/ipc/types.rs:136-141`; `crates/beads-daemon/src/runtime/admin/policy_reload.rs`
- Spec for namespace policies (drafted but uncoded): `REALTIME_SPEC_DRAFT.md:215-252`
- Wisp design decision + doc: `docs/go-parity/types/wisp.md`
- Session/wait/transcript/binding/delivery typed mapping: `docs/go-parity/primitives/metadata-remapping.md:367-530`
- CRDT audit (namespace-relevant lines): `docs/CRDT_AUDIT.md:106-125`
