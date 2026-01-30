---
name: beads-jj-workflow
description: Use before starting any code work in beads-rs. Establishes jj commit rhythm and bd issue tracking workflow.
---

# beads-rs Development Workflow

jj for version control, bd for issue tracking. Use together.

## First Time This Session?

Run `bd prime` for a quick, dense tutorial on how beads works and how we use it.

## VCS Operations

Use the `/using-jj` skill for all version control.
- Resolving conflicts? See the **conflicts** section.
- Making PRs? Read **github.md** first.
- Messing with the DAG? Read **surgery.md**.

## The Loop

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

bd close bd-xyz             # bead done, all acceptance met
```

## JJ Rhythm

**Commits are checkpoints, not milestones.** The loop runs 3-20 times per bead.

- ~50 lines without `jj describe`? Too much. Describe and `jj new`.
- Touched 2+ unrelated things? Should've been 2 commits.
- About to context-switch? Describe first.
- **Every commit message includes the bead ID.**

Fixing mistakes:
- Batched too much? `jj split`
- Wrong message? `jj describe` again
- Reorganize? `jj squash`, `jj rebase -r`

## Follow-up Beads

Notice out-of-scope work? File a bead immediately:

```bash
bd create "Hardcoded 30s timeout in sync.rs:234" --type=bug --priority=2
```

Don't just mention it in commits—make it trackable.

## Verification Before Close

Before `bd close`:
```bash
cargo fmt --all
cargo clippy -- -D warnings
cargo test
```

All must pass. Then close.
