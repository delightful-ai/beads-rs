# beads-rs Workflow

Version control with jj, issue tracking with bd.

## Crate DAG Policy

Canonical target crate DAG and forbidden dependency edges live in:

- `docs/CRATE_DAG.md`

## Version Control (jj)

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

## Issue Tracking (bd)

**bd** is infrastructure for you, the agent. It's your external memory.

A bead is a **promise**: you WILL get to this, just not now.

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

## Commit Guidelines

- Use Conventional Commits: `feat(scope): ...`, `fix: ...`, `test(scope): ...`, `chore: ...`.
- Include the tracker reference: `bd-abc123` or `(#123)`.
- PRs should explain "why" and "what", link relevant issues/spec changes.
- Before merge: `cargo fmt --all`, `just dylint` (required boundary gate), `cargo clippy --all-features -- -D warnings`, `cargo test`.
