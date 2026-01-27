---
name: execute-epic
description: Use when working on beads from a specific epic, or when asked to "do a bead from <epic-id>". Claims the next available bead from the epic, works it with jj rhythm, verifies, and closes it.
---

# Execute Epic Bead

Claim and complete ONE bead from a specified epic, then stop.

## Arguments

`/execute-epic <epic-id>` - The epic to pull the next bead from (e.g., `bd-qz97`)

## Phase 1: Orient

Before claiming, understand the landscape:

```bash
bd prime                    # full workflow state
bd ready                    # what's available
jj log --limit 20           # recent work
```

Filter to the epic:
```bash
bd show <epic-id>           # understand the epic
bd list --parent=<epic-id>  # beads in this epic
```

Before writing new types, traits, errors, or tests, read the relevant doc in `docs/philosophy/`:
- `type_design.md` - typestate, parse-don't-validate
- `error_design.md` - error context, propagation
- `trait_design.md` - trait boundaries, coherence
- `test_design.md` - test structure, property-based testing

Before writing code, read the essay in `docs/philosophy/scatter.md` . 

## Phase 2: Decide

Pick the next bead from the epic. Justify:
- Why this one matters now
- What it unblocks
- Why higher priority than alternatives

Then proceed immediately to execution.

## Phase 3: Execute

```bash
bd show <bead-id>           # understand it fully
bd claim <bead-id>          # own it
```

### JJ Rhythm

jj is not git. No staging area - working copy IS the commit.

The loop (runs MANY times per bead):
```
jj new
# edit ONE thing (add a fn, write a test, fix a bug)
jj describe "<bead-id>: what you did"
# repeat
```

**Heuristics:**
- ~50 lines changed without describe? Too much.
- About to run cargo check/test? Describe first.
- Did two unrelated things? Should've been two commits. Use `jj split`.

Every commit message includes the bead ID. 3-15 commits per bead is normal. 1 commit = batched too much.

### Handling Uncommitted Changes

If there are uncommitted changes when you start:
- They're probably from your previous work in this session
- `jj describe` them with what they actually are
- Or if genuinely unclear, ask the user what they are
- Do NOT create fake "wip: pre-existing" commits

### Cargo Discipline

Run proactively and often - not just at verification:
```bash
cargo check
cargo clippy -- -D warnings
cargo test
```

Run tests after writing them. Don't wait until the end.

### Before Closing

Verify:

1. **Acceptance criteria met** - re-read the bead, check each item

2. **Philosophy compliance:**
   - Audit for type_design, error_design, trait_design, test_design
   - Fix violations or document exceptions

3. **Tests pass:**
   - cargo check, cargo clippy -D warnings, cargo test
   - Tests cover error paths, not just happy path
   - **Every bug fix MUST have a regression test**

If verification uncovers issues, use the same jj rhythm. `jj new` -> fix -> `jj describe` -> repeat.

Then close:
```bash
bd close <bead-id>
```

## File Beads for Tech Debt

When you encounter ANY of these, file a P2 bead immediately:
- Code violating type_design.md principles
- Maintainability issues (unclear ownership, missing abstractions)
- Missing or weak tests
- Hardcoded values that should be configurable
- Error handling that swallows context

```bash
bd create "Brief description" --priority=2 --type=task
```

Don't fix it now unless blocking. File it, keep going.

## Output

After the bead is closed:
- The bead you chose and why
- What you completed (commits, tests added)
- Any beads you created for follow-on work
- What's next per `bd ready`
