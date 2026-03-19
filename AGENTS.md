## beads-rs

Distributed work-item database for agent swarms, using git as the sync layer. Rust rewrite of the original Go beads.

Core idea: beads are a CRDT. Most fields are last-writer-wins via `Lww<T>` (timestamp + actor); merges are deterministic. State syncs through `refs/heads/beads/store`, separate from code branches.

A local daemon holds canonical state and schedules git sync (~500ms). CLI talks to it over Unix socket, auto-starts on first use. One daemon serves many clones by keying state on normalized remote URL.

If this is your first time touching beads this session, run `bd prime`. It is the fastest way to reload the mental model and command language.

## Worldview

- **Kill scatter**: One source of truth per concept. If data lives in two places, merge or derive.
- **Types tell the truth**: Typed digests, enums not strings, validated IDs. Wrong states should be unrepresentable.
- **Proof beats vibes**: deterministic merges, explicit invariants, and cheap verification are part of the design, not cleanup.

## Architecture

The important ownership split is:

```text
crates/beads-core/         Canonical domain model, CRDT semantics, validated types, apply/merge rules
crates/beads-api/          Output/result schemas for CLI/daemon JSON surfaces
crates/beads-bootstrap/    Repo discovery, path derivation, config schema + precedence
crates/beads-surface/      IPC request/response contract, client helpers, shared patch/query types
crates/beads-cli/          Command language, clap parsing, request construction, human/JSON rendering
crates/beads-git/          Git-backed sync, wire format, checkpoint import/export/publication
crates/beads-daemon-core/  Shared REPL/WAL/durability contracts
crates/beads-daemon/       Runtime process, store/session lifecycle, mutation/query orchestration
crates/beads-rs/           Assembly crate/package entrypoint, host seams, binary wiring, upgrade/migration glue
crates/beads-rs/tests/     Assembly/product integration, package behavior, public-boundary enforcement
scripts/                   Side-effectful tooling, profiling helpers, release/install automation
docs/                      Source-of-truth specs, design notes, and repo policy documents
lints/                     Custom Dylint rules and vendored lint authoring utilities
```

`beads-rs` is the tiny assembly crate/package entrypoint. It is not the old compatibility umbrella. Do not route new internal dependencies through it just because it is convenient.

## Cross-Cutting Invariants

- The CLI tree must not import daemon modules directly. `beads-cli` owns command language and rendering; `beads-rs` CLI code is limited to host orchestration hooks.
- Non-daemon crates must not import `beads_daemon::runtime::*`, and no crate may import `beads_daemon::git::*`. Those are private organization shims, not public APIs.
- `crates/beads-rs/tests` is for assembly/product seams only. Pure core, git, daemon, or protocol coverage should live with the owning crate unless there is a genuine cross-crate reason not to.

## Design References

- `docs/CRATE_DAG.md` — canonical crate ownership + forbidden dependency edges
- `docs/AGENTSMD_INFO.md` — local spec for what `AGENTS.md` files should contain and how they should stack
- `docs/philosophy/type_design.md` — read before introducing new state types, validated wrappers, enums, or other domain-shaping types
- `docs/philosophy/trait_design.md` — read before adding or reshaping capability traits
- `docs/philosophy/error_design.md` — read before adding new error surfaces
- `docs/philosophy/test_design.md` — read before adding new test families, harnesses, or proof loops
- `docs/philosophy/scatter.md` — read when code placement, ownership, or source-of-truth questions feel diffuse

## Build & Verify

```bash
cargo check                      # typecheck (run compulsively)
cargo fmt --all                  # format
just dylint                      # required boundary gate (includes crate DAG policy test)
cargo clippy --all-features -- -D warnings  # lint (CI uses --all-features)
cargo xtest                      # canonical fast tier (nextest fast profile + beads-rs/e2e-tests)
cargo nextest run --profile slow --workspace --all-features --features slow-tests
```

Logging: `LOG=debug` or `LOG=beads_rs=trace`, or `-v/-vv` on `bd`.

The default workspace verification does **not** automatically cover every specialty surface.

- If you touch `crates/beads_stateright_models`, run `cargo check -p beads_stateright_models` and at least one relevant model/example check.
- If you touch `lints/`, run `cargo test -p beads_lints --manifest-path lints/Cargo.toml` in addition to `just dylint`.
- If you touch release/install scripts or CI workflows, sanity-check the referenced cargo commands and artifact flow locally instead of assuming root verification covers them.

## Test Infrastructure

Root keeps only the cross-subtree assembly-test rules. The child files under `crates/beads-rs/tests/**` own the local harness tactics and helper-level lore.

- Default to `cargo xtest` for the warm fast tier. It intentionally runs the workspace fast profile plus `beads-rs/e2e-tests`, not `--all-features`, so anything behind `slow-tests` still needs the slow tier.
- Use `cargo nextest run --profile slow --workspace --all-features --features slow-tests` when you touch time-based daemon/load/tailnet paths. `cargo xtest` will not prove those.
- The shared nextest runner is intentionally capped in `.config/nextest.toml` (`test-threads = 4`). Treat changes there as performance/flake work, not housekeeping.
- Tailnet fault-injection `daemon::repl_e2e` tests run in the `tailnet-fault-injection` nextest group (`max-threads = 1`). If you add another tailnet/proxy stress test, fence it into the same group instead of letting it silently contend with the rest of the suite.
- Profile the suite with `./scripts/profile-tests.sh`. It records per-test nextest timing plus env-gated fixture timing under `tmp/perf/tests-*`; override its default thread count only when you are deliberately measuring a different concurrency level.
- Reuse shared helpers under `crates/beads-rs/tests/integration/fixtures/`. If a test needs new repo/runtime/daemon setup behavior, add or extend a shared fixture instead of creating another one-off local harness.
- Reuse `fixtures::temp` for integration temp roots. Keep Unix socket paths short and deterministic instead of deriving them from `current_dir()/tmp`.
- Avoid fixed sleeps in tests. Prefer shared condition-based wait helpers and explicit readiness barriers.
- If a fixture intends to own a daemon process, spawn `bd daemon run` explicitly and drive bootstrap/status through `IpcClient::for_runtime_dir(...).with_autostart(false)` so helper CLI autostart paths do not steal lifecycle ownership.
- For namespace convergence polling, prefer `AdminFingerprint` over full `AdminStatus` unless the assertion actually needs the heavier payload.
- For crash/restart replication tests, snapshot readiness first and then require fresh post-restart handshakes with `replication_ready_snapshot()` and `assert_replication_ready_since(...)`.
- Do not call `reload_replication()` after ordinary replicated writes when the roster/config has not changed; wait on the existing readiness/convergence helpers instead.
- `crates/beads-rs/tests/public_boundary.rs` is the assembly-boundary guardrail. When you re-home tests or move package-owned seams, update its assertions and the nearby ownership markers in the same change.
- `crates/beads-rs/tests/e2e.rs` now consumes `beads_daemon::testkit::e2e`; do not grow a second in-memory replication harness under `crates/beads-rs/tests`.
- Tests should usually live with the owning crate. `crates/beads-rs/tests` is reserved for assembly/product integration, package behavior, public-boundary enforcement, and cross-crate seams that do not belong cleanly to a single owner crate.

## Code Style

- Types should tell the truth, especially uncomfortable truths. Prefer validated/newtyped/enumerated states over strings, booleans, or parallel flags when the distinction changes behavior.
- Preserve information until a deliberate boundary. Do not collapse rich domain state or error context into lossy helper shapes just to satisfy one caller.
- New traits should represent real capability seams, not bags of methods. If context is unavoidable, make it explicit in the type or method contract instead of smuggling it through lore.
- New error types should be structured caller decision surfaces. Use crate-local `thiserror` enums, preserve evidence until a real boundary, and reserve panics for bugs.
- New tests must kill a family of wrong implementations. Assert at the lowest clean layer first; use assembly/e2e tests only for shipped seams and cross-crate behavior that cannot be proven lower.
- Parse and validate at the boundary, then pass typed values inward. Do not re-parse, re-normalize, or re-derive canonical semantics in higher layers.
- Prefer one canonical helper stack per seam. If multiple call sites or tests want the same setup, promote a shared helper in the owning subtree instead of cloning one-off flows.
- Follow `rustfmt` and keep `clippy` clean, but treat formatting/lints as hygiene, not the design bar.
- Naming still matters: `snake_case.rs` modules, `CamelCase` types, and `kebab-case` CLI flags.

## AGENTS.md Policy

Root `AGENTS.md` stays rich on purpose. It is the only context guaranteed to be present in every stack, so it keeps the beads mental model, repo worldview, crate ownership map, default verification tiers, and `bd` + `jj` workflow.

The hierarchy rule is highest stable truth, not highest possible truth. Put guidance at the highest layer where it is still true for the whole subtree, changes decisions there, and is unlikely to churn with local implementation details.

Agents should read `AGENTS.md` files from root down to the working directory. Higher levels set defaults; lower levels add local delta and explicit exceptions.

Add a child `AGENTS.md` when a subtree has its own ownership boundary, verification delta, proof model, side effects, or nearby legacy bait that a smart stranger would otherwise copy.

Prefer child files at:

- first-class crate roots
- test roots with their own proof model
- tooling/script roots with side effects
- docs roots where source-of-truth vs historical context needs to be explicit

Child files should mostly contain **delta**, not duplication.

- **On-touch rule**: if you change a directory's boundary, canonical pattern, verification command, or known hazard, update the nearest relevant `AGENTS.md` in the same change.
- **No-void rule**: do not demote guidance out of a parent unless the receiving child `AGENTS.md` already exists or is being created in the same change.
- **Missing-child rule**: if a subtree clearly needs local guidance and has no child `AGENTS.md`, either add it immediately or keep the rule at the parent until that child exists.
- **Promotion rule**: if the same rule becomes true for two or more siblings, move it up one level.
- **Demotion rule**: if a root rule names a helper, fixture, or command that only matters in one subtree, move it down.
- **Stale-child rule**: if the nearest child `AGENTS.md` is stale, fix it before relying on it. Do not preserve known-bad local guidance just because a parent file is more accurate.
- **Ownership rule**: every child `AGENTS.md` should have an obvious review affinity through the owning crate, test root, or tooling area. If no one can say who should keep a file true, the file is probably at the wrong layer.
- **Churn rule**: high-churn or high-incident directories should get periodic `AGENTS.md` review when touched, not just opportunistic edits.
- **Validation rule**: when you touch an `AGENTS.md`, sanity-check that referenced paths still exist and that referenced commands still parse or obviously match the current workflow.
- **Incident rule**: if an `AGENTS.md` lied badly enough to waste time or cause a bug, fix it immediately or file a `P0` bead immediately, and complete that bead before declaring the current chunk done.

## Version Control

We use **jj** (jujutsu), not git directly. Use the `jj` skill for VCS operations.
- Resolving conflicts? Open `reference/conflicts.md` from the `jj` skill.
- Making PRs? Open `github.md` from the `jj` skill first.
- Messing with the DAG? Open `surgery.md` from the `jj` skill first.

### Why jj

- No staging area. Working copy IS the commit.
- `jj describe` is retroactive labeling, not “committing”.
- Rewrites are trivial (`jj squash`, `jj rebase`, etc.), so capture progress now and reorganize later.

### JJ Rhythm

**Commits are checkpoints, not milestones.**

```bash
jj new                              # start fresh change
# edit: one logical thing
jj describe "<bead-id>: what you did"  # label it
# repeat
```

This loop runs 3-20 times per bead. A bead spanning 1 commit means you batched too much.

- ~50 lines without `jj describe`? Too much. Describe and `jj new`.
- Touched 2+ unrelated things? Should've been 2 commits.
- About to context-switch? Describe first.
- **Every commit message includes the bead ID.**

Fixing mistakes: `jj split` (batched too much), `jj describe` (wrong message), `jj squash`/`jj rebase -r` (reorganize).

## Issue Tracking

We use **bd**. If this is your first time interacting with beads this session, run `bd prime` for a quick, dense tutorial.

### The Core Workflow

```bash
bd ready                    # see what's next
bd show <bead-id>           # understand it
bd claim <bead-id>          # you own it now
jj new                      # start first change

# --- this loop runs MANY times per bead ---
# edit: one coherent thing (add fn, fix bug, write test)
jj describe "<bead-id>: added validation for Foo"
jj new
# edit: next coherent thing
jj describe "<bead-id>: tests for Foo validation"
jj new
# ---

# required verification gate before close
cargo fmt --all
just dylint                       # required boundary gate (crate layering)
cargo clippy --all-features -- -D warnings
cargo xtest

# when the change touches slow or specialty surfaces, run their extra gates too

bd close <bead-id>         # bead done, all acceptance criteria met
bd ready                    # next bead
```

The jj loop is INSIDE the bead loop. Many commits per bead is correct.

### Follow-up Beads

When you notice out-of-scope work while implementing something, **file a bead immediately**—don't just mention it in commit messages.

```bash
bd create "Hardcoded 30s timeout in sync.rs:234" --type=bug --priority=2
bd create "executor.rs:145 check-then-unwrap should use require_live" --type=chore --priority=3
```

### Writing Good Beads

Each bead should be **one self-contained, independently doable thing**. If you're writing a bead that says "and also..." — stop and make two beads.

**Structure for non-trivial beads:**

```text
**Problem**
What's wrong or missing. Be specific — file paths, error messages, code snippets.

**Design**
How to fix it. Include implementation approach and code examples.

**Acceptance**
- [ ] Concrete, verifiable checklist items
- [ ] Tests pass
- [ ] Specific behavior works

**Files:** list of affected files
```

**Priority:** P0 (critical) → P1 (high) → P2 (medium) → P3 (low) → P4 (backlog)

**Epics & deps:**
```bash
bd create "Auth overhaul" --type=epic
bd create "Add OAuth support" --parent=<epic-id>
bd dep add A B              # A depends on B
```

## Philosophy

This codebase will outlive you. Every shortcut becomes someone else's burden. Every hack compounds.

Fight entropy. Leave the codebase better than you found it.
