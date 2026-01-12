## beads-rs

`beads-rs` is a distributed work-item database for agent swarms, using git as the sync layer. It’s a Rust rewrite of the original Go beads.

Core idea: beads are a CRDT. Most fields are last-writer-wins via `Lww<T>` (timestamp + actor); merges are deterministic and require no manual conflict resolution. State syncs through a dedicated git ref (`refs/heads/beads/store`), kept separate from normal code branches.

A local daemon holds canonical state in memory and schedules git sync after mutations (~500ms). The CLI talks to it over a Unix socket and auto-starts it on first use (`bd daemon run`). One daemon can serve many local clones by keying state on the normalized remote URL; `Workflow` and `CanonicalState` make invalid state unrepresentable.

## Architecture Overview (Directory Map)

- `src/core/`: CRDT/domain model (“Layers 0–9”), including `Lww<T>`, `Workflow`, `CanonicalState` invariants
- `src/git/`: sync protocol over `git2` (typestate machine, wire format, collision resolution) targeting `refs/heads/beads/store`
- `src/daemon/`: serialization point (ops + queries), Unix-socket IPC, sync scheduling, per-remote state sharing
- `src/api/`: canonical IPC + `--json` schemas (avoid lossy “view” structs)
- `src/cli/`: `clap` parsing, per-command handlers, and human/JSON rendering; should stay thin and delegate to daemon
- `src/migrate/`: import utilities (currently beads-go JSONL export)
- `src/bin/`: `bd` entrypoint and tracing setup
- `.github/`: CI workflows and issue templates
- `.cargo/config.toml`: local `cargo` aliases
- `flake.nix` / `shell.nix`: optional Nix dev shells

## Key Files

- `src/bin/main.rs`: `bd` entrypoint + tracing init
- `src/lib.rs`: library exports
- `SPEC.md`: data model + invariants
- `CLI_SPEC.md`: CLI surface and compatibility notes

## Build, Test, and Development Commands

- `cargo check`: typecheck quickly. Run this compulsively. 
- `cargo build`: build debug binaries/library
- `cargo build --release`: optimized build
- `cargo run --bin bd -- <args>`: run the CLI locally
- `cargo test`: run unit tests (tests are co-located under `#[cfg(test)]`)
- `cargo fmt --all`: format code
- `cargo clippy -- -D warnings`: lint (CI treats warnings as errors). Run proactively and often.

## Coding Style & Naming Conventions

- Follow `rustfmt` (`rustfmt.toml`) and keep `clippy` clean.
- Prefer explicit error types (`thiserror`) and `Result<T, beads_rs::Error>` in library APIs.
- Naming: modules/files in `snake_case.rs`, types in `CamelCase`, CLI flags in `kebab-case`.

## Testing Guidelines

- Add a regression test for bug fixes; keep tests deterministic and OS-independent.
- Prefer temp directories/fixtures over touching real repositories or user state.

## Commit & Pull Request Guidelines

- Use Conventional Commits (seen in history): `feat(scope): ...`, `fix: ...`, `test(scope): ...`, `chore: ...`.
- Include the tracker reference when available, e.g. `(bd-abc123)` or `(#123)`.
- PRs should explain “why” and “what”, link relevant issues/spec changes, and pass: `cargo fmt`, `cargo clippy`, `cargo test`.

## Debugging & Configuration Tips

- Logging uses `tracing`: set `LOG=debug` (or a module filter like `LOG=beads_rs=trace`) and/or pass `-v/-vv` to `bd`.

## CI Note

This repo does not currently run a coverage job in CI.


## Version Control

We use `jj` (jujutsu), not git directly. jj has different mental model—internalize it.

### Why jj

- No staging area. Working copy IS the commit.
- `jj describe` is retroactive labeling, not "committing"
- Rewrites are trivial (`jj squash`, `jj rebase`, etc.)
- You can reorganize history later, so capture progress NOW

### JJ Rhythm

**Commits are checkpoints, not milestones.**

The loop:
```
jj new                              # start fresh change
# edit: one logical thing
jj describe "bd-xyz: what you did"  # label it
# repeat
```

That's it. This loop runs 3-20 times per bead. A bead spanning 1 commit means you batched too much.

**Heuristics:**
- ~50 lines edited without `jj describe`? Too much. Describe and `jj new`.
- Touched 2+ unrelated things? Should've been 2 commits.
- About to context-switch (run tests, check something, take a break)? Describe first.

**Every commit message includes the bead ID** you're working on. Not just the "last" one—there is no last one until the bead is Finished.

Think: ctrl+s for semantic progress. You save files constantly; same energy for commits.

### Fixing mistakes

Batched too much? `jj split` to break it apart.
Wrong message? `jj describe` again (overwrites).
Need to reorganize? `jj squash`, `jj rebase -r`, etc.

jj makes history malleable. Capture first, organize later.

## Issue Tracking

**bd** is infrastructure for you, the agent. It's your external memory.

A bead is a **promise**: you WILL get to this, just not now. [... keep existing content ...]

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
# edit: ... 
# ---

bd finish bd-xyz            # bead done, all acceptance criteria met
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
This is the "what would I tell another engineer" section.

**Design Notes** (optional)
Tradeoffs, alternatives considered, dependencies on other work, open questions.

**Acceptance**
- [ ] Concrete, verifiable checklist items
- [ ] Tests pass
- [ ] Specific behavior works

**Files:** list of affected files (helps with scoping)
```

**Quick beads are fine too.** A one-liner like `bd create "Timeout hardcoded in auth.rs:45" --type=bug` is perfectly valid when the fix is obvious.

**Priority guide:**
- P0 (critical): Blocking all work, data loss, security issue
- P1 (high): Blocking important work, significant bug
- P2 (medium): Should do soon, meaningful improvement
- P3 (low): Nice to have, cleanup
- P4 (backlog): Someday/maybe

**Epics** group related work. Create subtasks with `--parent`:
```bash
bd create "Auth overhaul" --type=epic
bd create "Add OAuth support" --parent=bd-xxx
bd create "Add session management" --parent=bd-xxx
```

**Dependencies** express "A can't start until B is done":
```bash
bd dep add A B              # A depends on B (A waits for B)
bd dep tree bd-xxx          # Visualize what blocks what
bd blocked                  # See what's stuck
```

Be proactive about dependencies. When creating related beads, think: "Can these run in parallel, or does one need the other's output?" Add deps immediately — don't leave implicit ordering in your head.


