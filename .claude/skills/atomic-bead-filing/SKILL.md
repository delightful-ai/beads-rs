---
name: atomic-bead-filing
description: Use when noticing something off (code smell, design smell, workaround) while working and want to capture it without losing flow - grounds observation in codebase and files a proper bead that future agents can execute
---

# Atomic Bead Filing

## Overview

File beads that future agents can execute with zero shared context. You receive an observation, ground it in the codebase, and create a bead that stands alone.

## When to Use

- Main agent noticed something off while working
- Need to capture it without derailing current task
- Want a proper bead, not a vague note

## Core Principle

A bead is good if a stranger-agent can execute it knowing ONLY:
1. The bead's description
2. The codebase (via search/read)

They should NOT need: conversation history, the observer's context, or clarification.

## Workflow

1. **Receive observation** from main agent (you have conversation context)
2. **Explore codebase** to ground it - find specific files, line numbers, patterns
3. **Ask questions** via AskUserQuestion if genuinely unclear
4. **Determine actionability**:
   - Clear path → actionable bead
   - Multiple valid approaches → add `human-needed` label, describe options
5. **File bead** with `bd create`
6. **Sync** with `bd sync` (just git commit isn't enough - beads live on beads-sync branch)
7. **Return bead ID + full bead details** so main agent sees what was filed

## Bead Structure

**Title**: Concise, grounded summary (not vague)
- Bad: "Fix type issues"
- Good: "RuleViolation missing priority field - Rust hardcodes HIGH"

**Description**: The concrete issue
```markdown
## What's Wrong
[Specific problem - what IS off, grounded in files]

## Where
[Exact file paths, line numbers]

## Why It Matters
[What could go wrong, why this is tech debt]

## Files to Study
- path/to/file.rs (the problematic code)
- path/to/related.py (the other side of the mismatch)
```

**Fields to set** (run `bd create --help` to see all available):

| Flag | When to use |
|------|-------------|
| `--type task` | Default. Use `bug` if actively broken, `feature` for new capability |
| `--priority` | P0 critical (security/data loss), P1 high (bugs/blockers), P2 medium, P3 low (polish), P4 backlog - default to your judgment based on impact |
| `--labels human-needed` | Multiple valid approaches exist (see below) |
| `--deps discovered-from:<id>` | Main agent was working on a specific bead |
| `--acceptance` | **Fill if obvious** - what would "done" look like? |
| `--design` | **Fill if obvious** - brief notes on approach/constraints |

**Fold context into description footer**:
```markdown
---
*Discovered while: [what triggered this observation]*
```

## human-needed Label

Use when there are forks requiring human decision:
- Multiple valid architectural approaches
- Trade-offs that depend on priorities you don't know
- Breaking changes vs. compatibility concerns

When using human-needed, add to description:
```markdown
## Decision Needed
[Specific question]

## Options
1. **Option A**: [approach] - [pros/cons]
2. **Option B**: [approach] - [pros/cons]
```

## What NOT to Do

- Don't file vague beads ("look into this")
- Don't propose fixes unless obvious (focus on WHAT is off)
- Don't skip grounding (always find specific files)
- Don't ask unnecessary questions (use conversation context + codebase)

## Example

**Input from main agent**:
"The severity is hardcoded in transform.rs. Noticed while adding analyzer endpoint."

**Your process**:
1. Search for severity in transform.rs
2. Find line 250: `parse_severity("HIGH")`
3. Trace back - where should severity come from?
4. Find RuleViolation struct doesn't have severity field
5. Find Python RuleViolationDto DOES have priority field
6. Ground the full issue

**Output bead**:
```bash
bd create "RuleViolation drops priority - Rust hardcodes HIGH severity" \
  --type task \
  --priority 1 \
  --acceptance "Rust RuleViolation receives and uses Python's computed priority" \
  --description "## What's Wrong
Rust transform hardcodes severity to HIGH, ignoring Python's computed priority.

Line 250 in transform.rs:
\`\`\`rust
let priority = parse_severity(\"HIGH\").unwrap_or(RuleSeverity::Medium);
\`\`\`

## Where
- oxide/server/src/eval/transform.rs:250 (hardcoded value)
- oxide/server/src/eval_client.rs:165-173 (RuleViolation missing priority field)
- src/app/features/evaluation/contracts/rules_contracts.py:62-75 (RuleViolationDto HAS priority)

## Why It Matters
Python evaluator computes actual priority per-violation, but Rust discards it. All violations appear HIGH priority regardless of actual severity. Silent data loss.

## Files to Study
- oxide/server/src/eval/transform.rs (the transform)
- oxide/server/src/eval_client.rs (HTTP receive types)
- src/app/features/evaluation/contracts/rules_contracts.py (Python source types)

---
*Discovered while: adding analyzer endpoint*"
```
