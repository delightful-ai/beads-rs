//! `bd setup` - Setup integration with AI editors.
//!
//! Subcommands:
//! - `bd setup claude` - Claude Code hooks
//! - `bd setup cursor` - Cursor IDE rules
//! - `bd setup aider` - Aider configuration

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value, json};

use crate::daemon::OpError;
use crate::{Error, Result};

// =============================================================================
// Claude Code integration
// =============================================================================

pub(crate) fn handle_claude(project: bool, check: bool, remove: bool) -> Result<()> {
    if check {
        return check_claude();
    }
    if remove {
        return remove_claude(project);
    }
    install_claude(project)
}

fn install_claude(project: bool) -> Result<()> {
    let settings_path = if project {
        println!("Installing Claude hooks for this project...");
        PathBuf::from(".claude/settings.local.json")
    } else {
        println!("Installing Claude hooks globally...");
        let home = home_dir()?;
        home.join(".claude/settings.json")
    };

    // Ensure parent directory exists
    if let Some(parent) = settings_path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            Error::Op(OpError::ValidationFailed {
                field: "setup".into(),
                reason: format!("failed to create directory: {e}"),
            })
        })?;
    }

    // Load or create settings
    let mut settings: Map<String, Value> = if settings_path.exists() {
        let data = fs::read_to_string(&settings_path).map_err(|e| {
            Error::Op(OpError::ValidationFailed {
                field: "setup".into(),
                reason: format!("failed to read settings: {e}"),
            })
        })?;
        serde_json::from_str(&data).unwrap_or_default()
    } else {
        Map::new()
    };

    // Get or create hooks section
    let hooks = settings
        .entry("hooks".to_string())
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| {
            Error::Op(OpError::ValidationFailed {
                field: "hooks".into(),
                reason: "hooks is not an object".into(),
            })
        })?;

    let command = "bd prime";

    // Add SessionStart hook
    if add_hook_command(hooks, "SessionStart", command) {
        println!("✓ Registered SessionStart hook");
    }

    // Add PreCompact hook
    if add_hook_command(hooks, "PreCompact", command) {
        println!("✓ Registered PreCompact hook");
    }

    // Write back
    let data = serde_json::to_string_pretty(&settings).map_err(|e| {
        Error::Op(OpError::ValidationFailed {
            field: "setup".into(),
            reason: format!("failed to serialize settings: {e}"),
        })
    })?;
    atomic_write(&settings_path, data.as_bytes())?;

    println!();
    println!("✓ Claude Code integration installed");
    println!("  Settings: {}", settings_path.display());
    println!();
    println!("Restart Claude Code for changes to take effect.");

    Ok(())
}

fn check_claude() -> Result<()> {
    let home = home_dir()?;
    let global_settings = home.join(".claude/settings.json");
    let project_settings = PathBuf::from(".claude/settings.local.json");

    let global_hooks = has_beads_hooks(&global_settings);
    let project_hooks = has_beads_hooks(&project_settings);

    if global_hooks {
        println!("✓ Global hooks installed: {}", global_settings.display());
        Ok(())
    } else if project_hooks {
        println!("✓ Project hooks installed: {}", project_settings.display());
        Ok(())
    } else {
        println!("✗ No hooks installed");
        println!("  Run: bd setup claude");
        std::process::exit(1);
    }
}

fn remove_claude(project: bool) -> Result<()> {
    let settings_path = if project {
        println!("Removing Claude hooks from project...");
        PathBuf::from(".claude/settings.local.json")
    } else {
        println!("Removing Claude hooks globally...");
        let home = home_dir()?;
        home.join(".claude/settings.json")
    };

    let data = match fs::read_to_string(&settings_path) {
        Ok(d) => d,
        Err(_) => {
            println!("No settings file found");
            return Ok(());
        }
    };

    let mut settings: Map<String, Value> = serde_json::from_str(&data).unwrap_or_default();

    let Some(hooks) = settings.get_mut("hooks").and_then(|h| h.as_object_mut()) else {
        println!("No hooks found");
        return Ok(());
    };

    // Remove bd prime hooks
    remove_hook_command(hooks, "SessionStart", "bd prime");
    remove_hook_command(hooks, "PreCompact", "bd prime");

    // Write back
    let data = serde_json::to_string_pretty(&settings).map_err(|e| {
        Error::Op(OpError::ValidationFailed {
            field: "setup".into(),
            reason: format!("failed to serialize settings: {e}"),
        })
    })?;
    atomic_write(&settings_path, data.as_bytes())?;

    println!("✓ Claude hooks removed");
    Ok(())
}

fn add_hook_command(hooks: &mut Map<String, Value>, event: &str, command: &str) -> bool {
    let event_hooks = hooks
        .entry(event.to_string())
        .or_insert_with(|| json!([]))
        .as_array_mut();

    let Some(event_hooks) = event_hooks else {
        return false;
    };

    // Check if bd hook already registered
    for hook in event_hooks.iter() {
        if let Some(hook_map) = hook.as_object() {
            if let Some(commands) = hook_map.get("hooks").and_then(|c| c.as_array()) {
                for cmd in commands {
                    if let Some(cmd_map) = cmd.as_object() {
                        if cmd_map.get("command").and_then(|c| c.as_str()) == Some(command) {
                            println!("✓ Hook already registered: {event}");
                            return false;
                        }
                    }
                }
            }
        }
    }

    // Add bd hook
    let new_hook = json!({
        "matcher": "",
        "hooks": [{
            "type": "command",
            "command": command
        }]
    });

    event_hooks.push(new_hook);
    true
}

fn remove_hook_command(hooks: &mut Map<String, Value>, event: &str, command: &str) {
    let Some(event_hooks) = hooks.get_mut(event).and_then(|h| h.as_array_mut()) else {
        return;
    };

    let original_len = event_hooks.len();

    event_hooks.retain(|hook| {
        let Some(hook_map) = hook.as_object() else {
            return true;
        };
        let Some(commands) = hook_map.get("hooks").and_then(|c| c.as_array()) else {
            return true;
        };

        for cmd in commands {
            if let Some(cmd_map) = cmd.as_object() {
                if cmd_map.get("command").and_then(|c| c.as_str()) == Some(command) {
                    return false;
                }
            }
        }
        true
    });

    if event_hooks.len() < original_len {
        println!("✓ Removed {event} hook");
    }
}

fn has_beads_hooks(settings_path: &Path) -> bool {
    let Ok(data) = fs::read_to_string(settings_path) else {
        return false;
    };

    let Ok(settings) = serde_json::from_str::<Map<String, Value>>(&data) else {
        return false;
    };

    let Some(hooks) = settings.get("hooks").and_then(|h| h.as_object()) else {
        return false;
    };

    for event in ["SessionStart", "PreCompact"] {
        let Some(event_hooks) = hooks.get(event).and_then(|h| h.as_array()) else {
            continue;
        };

        for hook in event_hooks {
            let Some(hook_map) = hook.as_object() else {
                continue;
            };
            let Some(commands) = hook_map.get("hooks").and_then(|c| c.as_array()) else {
                continue;
            };
            for cmd in commands {
                if let Some(cmd_map) = cmd.as_object() {
                    if cmd_map.get("command").and_then(|c| c.as_str()) == Some("bd prime") {
                        return true;
                    }
                }
            }
        }
    }

    false
}

// =============================================================================
// Cursor IDE integration
// =============================================================================

const CURSOR_RULES: &str = r#"# Beads Issue Tracking
# Auto-generated by 'bd setup cursor' - do not remove these markers
# BEGIN BEADS INTEGRATION

This project uses [Beads (bd)](https://github.com/steveyegge/beads) for issue tracking.

## Core Rules
- Track ALL work in bd (never use markdown TODOs or comment-based task lists)
- Use `bd ready` to find available work
- Use `bd create` to track new issues/tasks/bugs
- Sync is automatic (~500ms) - no manual sync needed

## Quick Reference
```bash
bd prime                              # Load complete workflow context
bd ready                              # Show issues ready to work (no blockers)
bd list --status=open                 # List all open issues
bd create --title="..." --type=task  # Create new issue
bd update <id> --status=in_progress  # Claim work
bd close <id>                         # Mark complete
bd dep add <issue> <depends-on>       # Add dependency
```

## Workflow
1. Check for ready work: `bd ready`
2. Claim an issue: `bd update <id> --status=in_progress`
3. Do the work
4. Mark complete: `bd close <id>`

## Context Loading
Run `bd prime` to get complete workflow documentation in AI-optimized format.

# END BEADS INTEGRATION
"#;

pub(crate) fn handle_cursor(check: bool, remove: bool) -> Result<()> {
    if check {
        return check_cursor();
    }
    if remove {
        return remove_cursor();
    }
    install_cursor()
}

fn install_cursor() -> Result<()> {
    let rules_path = PathBuf::from(".cursor/rules/beads.mdc");

    println!("Installing Cursor integration...");

    // Ensure parent directory exists
    if let Some(parent) = rules_path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            Error::Op(OpError::ValidationFailed {
                field: "setup".into(),
                reason: format!("failed to create directory: {e}"),
            })
        })?;
    }

    atomic_write(&rules_path, CURSOR_RULES.as_bytes())?;

    println!();
    println!("✓ Cursor integration installed");
    println!("  Rules: {}", rules_path.display());
    println!();
    println!("Restart Cursor for changes to take effect.");

    Ok(())
}

fn check_cursor() -> Result<()> {
    let rules_path = PathBuf::from(".cursor/rules/beads.mdc");

    if rules_path.exists() {
        println!("✓ Cursor integration installed: {}", rules_path.display());
        Ok(())
    } else {
        println!("✗ Cursor integration not installed");
        println!("  Run: bd setup cursor");
        std::process::exit(1);
    }
}

fn remove_cursor() -> Result<()> {
    let rules_path = PathBuf::from(".cursor/rules/beads.mdc");

    println!("Removing Cursor integration...");

    match fs::remove_file(&rules_path) {
        Ok(_) => {
            println!("✓ Removed Cursor integration");
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("No rules file found");
        }
        Err(e) => {
            return Err(Error::Op(OpError::ValidationFailed {
                field: "setup".into(),
                reason: format!("failed to remove file: {e}"),
            }));
        }
    }

    Ok(())
}

// =============================================================================
// Aider integration
// =============================================================================

const AIDER_CONFIG: &str = r#"# Beads Issue Tracking Integration for Aider
# Auto-generated by 'bd setup aider'

# Load Beads workflow instructions for the AI
# This file is marked read-only and cached for efficiency
read:
  - .aider/BEADS.md
"#;

const AIDER_INSTRUCTIONS: &str = r#"# Beads Issue Tracking Instructions for AI

This project uses **Beads (bd)** for issue tracking. Aider requires explicit command execution - suggest commands to the user.

## Core Workflow Rules

1. **Track ALL work in bd** (never use markdown TODOs or comment-based task lists)
2. **Suggest 'bd ready'** to find available work
3. **Suggest 'bd create'** for new issues/tasks/bugs
4. **Sync is automatic** - no manual sync needed
5. **ALWAYS suggest commands** - user will run them via /run

## Quick Command Reference (suggest these to user)

- `bd ready` - Show unblocked issues
- `bd list --status=open` - List all open issues
- `bd create --title="..." --type=task` - Create new issue
- `bd update <id> --status=in_progress` - Claim work
- `bd close <id>` - Mark complete
- `bd dep add <issue> <depends-on>` - Add dependency

## Workflow Pattern to Suggest

1. **Check ready work**: "Let's run `/run bd ready` to see what's available"
2. **Claim task**: "Run `/run bd update <id> --status=in_progress` to claim it"
3. **Do the work**
4. **Complete**: "Run `/run bd close <id>` when done"

## Context Loading

Suggest `/run bd prime` for complete workflow documentation.

## Issue Types

- `bug` - Something broken that needs fixing
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature composed of multiple issues
- `chore` - Maintenance work (dependencies, tooling)

## Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (nice-to-have features, minor bugs)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

## Important Notes

- **Always use /run prefix** - Aider requires explicit command execution
- **Link discovered work** - Use `--deps discovered_from:<parent-id>` when creating issues found during work
- **Include descriptions** - Always provide meaningful context when creating issues
"#;

const AIDER_README: &str = r#"# Aider + Beads Integration

This project uses [Beads (bd)](https://github.com/steveyegge/beads) for issue tracking.

## How This Works with Aider

**Important**: Aider requires you to explicitly run commands using the `/run` command.
The AI will **suggest** bd commands, but you must confirm them.

## Quick Start

1. Check for available work:
   ```bash
   /run bd ready
   ```

2. Create new issues:
   ```bash
   /run bd create "Issue title" --description="Details" -t bug|feature|task -p 1
   ```

3. Claim work:
   ```bash
   /run bd update bd-42 --status in_progress
   ```

4. Complete work:
   ```bash
   /run bd close bd-42 --reason "Done"
   ```

## Configuration

The `.aider.conf.yml` file contains instructions for the AI about bd workflow.
The AI will read these instructions and suggest appropriate bd commands.

## Workflow

Ask the AI questions like:
- "What issues are ready to work on?"
- "Create an issue for this bug I found"
- "Show me the details of bd-42"
- "Mark bd-42 as complete"

The AI will suggest the appropriate `bd` command, which you run via `/run`.

## Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance work

## Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (default, nice-to-have)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

## More Information

- Run `bd --help` for full command reference
- Run `bd prime` for AI-optimized workflow context
"#;

pub(crate) fn handle_aider(check: bool, remove: bool) -> Result<()> {
    if check {
        return check_aider();
    }
    if remove {
        return remove_aider();
    }
    install_aider()
}

fn install_aider() -> Result<()> {
    let config_path = PathBuf::from(".aider.conf.yml");
    let instructions_path = PathBuf::from(".aider/BEADS.md");
    let readme_path = PathBuf::from(".aider/README.md");

    println!("Installing Aider integration...");

    // Ensure .aider directory exists
    fs::create_dir_all(".aider").map_err(|e| {
        Error::Op(OpError::ValidationFailed {
            field: "setup".into(),
            reason: format!("failed to create directory: {e}"),
        })
    })?;

    // Write config file
    atomic_write(&config_path, AIDER_CONFIG.as_bytes())?;

    // Write instructions file (loaded by AI)
    atomic_write(&instructions_path, AIDER_INSTRUCTIONS.as_bytes())?;

    // Write README (for humans)
    atomic_write(&readme_path, AIDER_README.as_bytes())?;

    println!();
    println!("✓ Aider integration installed");
    println!("  Config: {}", config_path.display());
    println!(
        "  Instructions: {} (loaded by AI)",
        instructions_path.display()
    );
    println!("  README: {} (for humans)", readme_path.display());
    println!();
    println!("Usage:");
    println!("  1. Start aider in this directory");
    println!("  2. Ask AI for available work (it will suggest: /run bd ready)");
    println!("  3. Run suggested commands using /run");
    println!();
    println!("Note: Aider requires you to explicitly run commands via /run");

    Ok(())
}

fn check_aider() -> Result<()> {
    let config_path = PathBuf::from(".aider.conf.yml");

    if config_path.exists() {
        println!("✓ Aider integration installed: {}", config_path.display());
        Ok(())
    } else {
        println!("✗ Aider integration not installed");
        println!("  Run: bd setup aider");
        std::process::exit(1);
    }
}

fn remove_aider() -> Result<()> {
    let config_path = PathBuf::from(".aider.conf.yml");
    let instructions_path = PathBuf::from(".aider/BEADS.md");
    let readme_path = PathBuf::from(".aider/README.md");
    let aider_dir = PathBuf::from(".aider");

    println!("Removing Aider integration...");

    let mut removed = false;

    for path in [&config_path, &instructions_path, &readme_path] {
        match fs::remove_file(path) {
            Ok(_) => removed = true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(Error::Op(OpError::ValidationFailed {
                    field: "setup".into(),
                    reason: format!("failed to remove file: {e}"),
                }));
            }
        }
    }

    // Try to remove .aider directory if empty
    let _ = fs::remove_dir(&aider_dir);

    if removed {
        println!("✓ Removed Aider integration");
    } else {
        println!("No Aider integration files found");
    }

    Ok(())
}

// =============================================================================
// Helpers
// =============================================================================

fn home_dir() -> Result<PathBuf> {
    dirs::home_dir().ok_or_else(|| {
        Error::Op(OpError::ValidationFailed {
            field: "setup".into(),
            reason: "failed to get home directory".into(),
        })
    })
}

fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    // Write to temp file then rename for atomicity
    let temp_path = path.with_extension("tmp");

    let mut file = fs::File::create(&temp_path).map_err(|e| {
        Error::Op(OpError::ValidationFailed {
            field: "setup".into(),
            reason: format!("failed to create temp file: {e}"),
        })
    })?;

    file.write_all(data).map_err(|e| {
        Error::Op(OpError::ValidationFailed {
            field: "setup".into(),
            reason: format!("failed to write file: {e}"),
        })
    })?;

    file.sync_all().map_err(|e| {
        Error::Op(OpError::ValidationFailed {
            field: "setup".into(),
            reason: format!("failed to sync file: {e}"),
        })
    })?;

    fs::rename(&temp_path, path).map_err(|e| {
        Error::Op(OpError::ValidationFailed {
            field: "setup".into(),
            reason: format!("failed to rename file: {e}"),
        })
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_rules_contains_beads() {
        assert!(CURSOR_RULES.contains("Beads"));
        assert!(CURSOR_RULES.contains("bd ready"));
    }

    #[test]
    fn aider_instructions_contains_beads() {
        assert!(AIDER_INSTRUCTIONS.contains("Beads"));
        assert!(AIDER_INSTRUCTIONS.contains("bd ready"));
    }
}
