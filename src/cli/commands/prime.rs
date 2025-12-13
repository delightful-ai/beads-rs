//! `bd prime` - Output AI-optimized workflow context.
//!
//! Designed for Claude Code hooks (SessionStart, PreCompact) to prevent
//! agents from forgetting bd workflow after context compaction.

use std::io::Write;

use crate::Result;
use crate::repo;

/// Output workflow context. Returns Ok(()) even if not in a beads repo
/// (silent success for cross-platform hook compatibility).
pub(crate) fn handle() -> Result<()> {
    // Check if we're in a git repo with beads initialized
    let in_beads_repo = match repo::discover() {
        Ok((repo, _)) => repo.refname_to_id("refs/heads/beads/store").is_ok(),
        Err(_) => false,
    };

    if !in_beads_repo {
        // Not in a beads project - silent exit with success
        // CRITICAL: No stderr output, exit cleanly
        // This enables cross-platform hook integration
        return Ok(());
    }

    // Output workflow context
    let context = get_context();

    let mut stdout = std::io::stdout().lock();
    if let Err(e) = write!(stdout, "{}", context) {
        if e.kind() != std::io::ErrorKind::BrokenPipe {
            return Err(crate::daemon::IpcError::from(e).into());
        }
    }

    Ok(())
}

fn get_context() -> &'static str {
    r#"# Beads Workflow

Track ALL work in beads. No TodoWrite, no markdown TODOs.
Sync is automatic (~500ms). Run `bd prime` after context compaction.

## Finding Work

```bash
bd ready                          # Unblocked, unclaimed — start here
bd list --status=in_progress      # Your active work
bd blocked                        # What's stuck and why
bd stale                          # Forgotten (30+ days untouched)
```

Search and filter:
```bash
bd search auth                    # Text search in title/description
bd list --type=bug --priority=0   # Critical bugs
bd list --status=open -l security # Open issues labeled security
```

Combine filters: `bd list --status=open --type=feature --assignee=me`

## Understanding Structure

```bash
bd show <id>                      # Full details, what blocks it, what it blocks
bd dep tree <id>                  # Visualize dependency graph
bd status                         # Project overview
bd epic status                    # Epic completion percentages
```

## Working on Issues

```bash
bd claim <id>                     # Claim it (I'm working on this)
# ... do the work ...
bd close <id>                     # Done (or: --reason="Won't fix: out of scope")
```

Found something while working? Capture it and keep going:
```bash
bd create "Timeout hardcoded in auth.rs:45" --type=bug --deps discovered_from:<current-id>
```

The `discovered_from` link preserves where you found it without blocking your current work.

## Creating Issues

A bead should have enough context to pick up cold — what, where, why. Can be one sentence if that's enough.

```bash
bd create "Timeout hardcoded in auth.rs:45" --type=bug --priority=1 \
  --desc="30s timeout causes 504s on slow connections. Make configurable."
```

**Types:** task, bug, feature, epic, chore
**Priority:** 0=critical, 1=high, 2=medium, 3=low, 4=backlog

Epics and subtasks:
```bash
bd create "Auth overhaul" --type=epic
bd create "Add OAuth" --parent=<epic-id>   # Creates bd-xxx.1
```

For complex work: `--design` for approach, `--acceptance` for done criteria.

## Dependencies

`bd dep add A B` — A depends on B (A waits for B to complete).

"Phase 2 depends on Phase 1" → `bd dep add phase2 phase1`

Verify with `bd blocked` — tasks should be blocked by their prerequisites.

## Labels

Type + priority + status covers most organization. Labels are for cross-cutting concerns:
```bash
bd label add <id> tech-debt
```
"#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_is_not_empty() {
        let ctx = get_context();
        assert!(!ctx.is_empty());
        assert!(ctx.contains("Beads Workflow"));
        assert!(ctx.contains("bd ready"));
        assert!(ctx.contains("bd create"));
    }
}
