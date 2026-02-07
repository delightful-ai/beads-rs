## beads-rs

Distributed work-item database for agent swarms, using git as the sync layer. Rust rewrite of the original Go beads.

Core idea: beads are a CRDT. Most fields are last-writer-wins via `Lww<T>` (timestamp + actor); merges are deterministic. State syncs through `refs/heads/beads/store`, separate from code branches.

A local daemon holds canonical state and schedules git sync (~500ms). CLI talks to it over Unix socket, auto-starts on first use. One daemon serves many clones by keying state on normalized remote URL.

## Worldview

- **Kill scatter**: One source of truth per concept. If data lives in two places, merge or derive.
- **Types tell the truth**: Typed digests, enums not strings, validated IDs. Wrong states should be unrepresentable.

## Architecture

```
crates/beads-core/     CRDT/domain model, Lww<T>, CanonicalState, validated types
crates/beads-api/      IPC + --json schemas
crates/beads-rs/src/
  ├── git/             sync protocol, wire format, checkpoint
  ├── daemon/          ops + queries, Unix socket IPC, sync scheduling
  ├── cli/             clap parsing, command handlers, rendering
  ├── migrate/         import utilities (beads-go JSONL)
  └── bin/             bd entrypoint
```

## Key Files

- `SPEC.md` — data model + invariants
- `CLI_SPEC.md` — CLI surface
- `WORKFLOW.md` — jj + bd workflow details

## Build & Verify

```bash
cargo check                      # typecheck (run compulsively)
cargo fmt --all                  # format
just dylint                      # required boundary gate (CLI/daemon layering)
cargo clippy --all-features -- -D warnings  # lint (CI uses --all-features)
cargo test                       # unit tests
cargo test --features slow-tests # slow tests (CI runs these separately)
```

Logging: `LOG=debug` or `LOG=beads_rs=trace`, or `-v/-vv` on `bd`.

## Coding Style

- Follow `rustfmt` + keep `clippy` clean.
- Explicit error types (`thiserror`), `Result<T, beads_rs::Error>` in library APIs.
- Naming: `snake_case.rs` modules, `CamelCase` types, `kebab-case` CLI flags.
- Regression test for bug fixes; keep tests deterministic and OS-independent.

## Version Control

We use **jj** (jujutsu), not git directly. Use the `/using-jj` skill for VCS operations.
- Resolving conflicts? See the conflicts section in the jj skill.
- Making PRs? Read the github file first.
- Messing with the DAG? Read the surgery file.

### JJ Rhythm

**Commits are checkpoints, not milestones.**

```
jj new                              # start fresh change
# edit: one logical thing
jj describe "bd-xyz: what you did"  # label it
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
bd show bd-xyz              # understand it
bd claim bd-xyz             # you own it now
jj new                      # start first change

# --- this loop runs MANY times per bead ---
# edit: one coherent thing (add fn, fix bug, write test)
jj describe "bd-xyz: added validation for Foo"
jj new
# edit: next coherent thing
jj describe "bd-xyz: tests for Foo validation"
jj new
# ---

# required verification gate before close
cargo fmt --all
just dylint                       # required boundary gate (crate layering)
cargo clippy --all-features -- -D warnings
cargo test

bd close bd-xyz            # bead done, all acceptance criteria met
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

```
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
bd create "Add OAuth support" --parent=bd-xxx
bd dep add A B              # A depends on B
```

## Philosophy

This codebase will outlive you. Every shortcut becomes someone else's burden. Every hack compounds.

Fight entropy. Leave the codebase better than you found it.
