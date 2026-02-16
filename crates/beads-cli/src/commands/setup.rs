//! `bd setup` - Setup integration with AI editors.
//!
//! Subcommands:
//! - `bd setup claude` - Claude Code hooks
//! - `bd setup cursor` - Cursor IDE rules
//! - `bd setup aider` - Aider configuration

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use clap::{Args, Subcommand};
use serde_json::{Map, Value, json};

pub type Result<T> = std::result::Result<T, SetupError>;

#[derive(Debug, thiserror::Error)]
pub enum SetupError {
    #[error("{field}: {reason}")]
    Validation { field: String, reason: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Subcommand, Debug)]
pub enum SetupCmd {
    /// Setup Claude Code integration (hooks for SessionStart/PreCompact).
    Claude(SetupClaudeArgs),
    /// Setup Cursor IDE integration (rules file).
    Cursor(SetupCursorArgs),
    /// Setup Aider integration (config + instructions).
    Aider(SetupAiderArgs),
}

#[derive(Args, Debug)]
pub struct SetupClaudeArgs {
    /// Install for this project only (not globally).
    #[arg(long)]
    pub project: bool,

    /// Check if Claude integration is installed.
    #[arg(long)]
    pub check: bool,

    /// Remove bd hooks from Claude settings.
    #[arg(long)]
    pub remove: bool,
}

#[derive(Args, Debug)]
pub struct SetupCursorArgs {
    /// Check if Cursor integration is installed.
    #[arg(long)]
    pub check: bool,

    /// Remove bd rules from Cursor.
    #[arg(long)]
    pub remove: bool,
}

#[derive(Args, Debug)]
pub struct SetupAiderArgs {
    /// Check if Aider integration is installed.
    #[arg(long)]
    pub check: bool,

    /// Remove bd config from Aider.
    #[arg(long)]
    pub remove: bool,
}

// =============================================================================
// Claude Code integration
// =============================================================================

pub fn handle_claude(project: bool, check: bool, remove: bool) -> Result<()> {
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
        print_line("Installing Claude hooks for this project...")?;
        PathBuf::from(".claude/settings.local.json")
    } else {
        print_line("Installing Claude hooks globally...")?;
        let home = home_dir()?;
        home.join(".claude/settings.json")
    };

    // Ensure parent directory exists
    if let Some(parent) = settings_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| validation_error("setup", format!("failed to create directory: {e}")))?;
    }

    // Load or create settings
    let mut settings: Map<String, Value> = if settings_path.exists() {
        let data = fs::read_to_string(&settings_path)
            .map_err(|e| validation_error("setup", format!("failed to read settings: {e}")))?;
        serde_json::from_str(&data).unwrap_or_default()
    } else {
        Map::new()
    };

    // Get or create hooks section
    let hooks = settings
        .entry("hooks".to_string())
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| validation_error("hooks", "hooks is not an object"))?;

    let command = "bd prime";

    // Add SessionStart hook
    match add_hook_command(hooks, "SessionStart", command) {
        HookAddOutcome::Added => print_line("✓ Registered SessionStart hook")?,
        HookAddOutcome::AlreadyPresent => {
            print_line("✓ Hook already registered: SessionStart")?;
        }
        HookAddOutcome::Skipped => {}
    }

    // Add PreCompact hook
    match add_hook_command(hooks, "PreCompact", command) {
        HookAddOutcome::Added => print_line("✓ Registered PreCompact hook")?,
        HookAddOutcome::AlreadyPresent => {
            print_line("✓ Hook already registered: PreCompact")?;
        }
        HookAddOutcome::Skipped => {}
    }

    // Write back
    let data = serde_json::to_string_pretty(&settings)
        .map_err(|e| validation_error("setup", format!("failed to serialize settings: {e}")))?;
    atomic_write(&settings_path, data.as_bytes())?;

    print_line("")?;
    print_line("✓ Claude Code integration installed")?;
    print_line(&format!("  Settings: {}", settings_path.display()))?;
    print_line("")?;
    print_line("Restart Claude Code for changes to take effect.")?;

    Ok(())
}

fn check_claude() -> Result<()> {
    let home = home_dir()?;
    let global_settings = home.join(".claude/settings.json");
    let project_settings = PathBuf::from(".claude/settings.local.json");
    check_claude_at(&global_settings, &project_settings)
}

fn check_claude_at(global_settings: &Path, project_settings: &Path) -> Result<()> {
    let global_hooks = has_beads_hooks(global_settings);
    let project_hooks = has_beads_hooks(project_settings);

    if global_hooks {
        print_line(&format!(
            "✓ Global hooks installed: {}",
            global_settings.display()
        ))?;
        Ok(())
    } else if project_hooks {
        print_line(&format!(
            "✓ Project hooks installed: {}",
            project_settings.display()
        ))?;
        Ok(())
    } else {
        print_line("✗ No hooks installed")?;
        print_line("  Run: bd setup claude")?;
        Err(validation_error(
            "setup",
            "claude integration not installed",
        ))
    }
}

fn remove_claude(project: bool) -> Result<()> {
    let settings_path = if project {
        print_line("Removing Claude hooks from project...")?;
        PathBuf::from(".claude/settings.local.json")
    } else {
        print_line("Removing Claude hooks globally...")?;
        let home = home_dir()?;
        home.join(".claude/settings.json")
    };

    let data = match fs::read_to_string(&settings_path) {
        Ok(d) => d,
        Err(_) => {
            print_line("No settings file found")?;
            return Ok(());
        }
    };

    let mut settings: Map<String, Value> = serde_json::from_str(&data).unwrap_or_default();

    let Some(hooks) = settings.get_mut("hooks").and_then(|h| h.as_object_mut()) else {
        print_line("No hooks found")?;
        return Ok(());
    };

    // Remove bd prime hooks
    let removed_session = remove_hook_command(hooks, "SessionStart", "bd prime");
    let removed_precompact = remove_hook_command(hooks, "PreCompact", "bd prime");
    if removed_session {
        print_line("✓ Removed SessionStart hook")?;
    }
    if removed_precompact {
        print_line("✓ Removed PreCompact hook")?;
    }

    // Write back
    let data = serde_json::to_string_pretty(&settings)
        .map_err(|e| validation_error("setup", format!("failed to serialize settings: {e}")))?;
    atomic_write(&settings_path, data.as_bytes())?;

    print_line("✓ Claude hooks removed")?;
    Ok(())
}

enum HookAddOutcome {
    Added,
    AlreadyPresent,
    Skipped,
}

fn hook_contains_command(hook: &Value, command: &str) -> bool {
    let Some(hook_map) = hook.as_object() else {
        return false;
    };
    let Some(commands) = hook_map.get("hooks").and_then(|c| c.as_array()) else {
        return false;
    };

    for cmd in commands {
        if let Some(cmd_map) = cmd.as_object()
            && cmd_map.get("command").and_then(|c| c.as_str()) == Some(command)
        {
            return true;
        }
    }
    false
}

fn add_hook_command(hooks: &mut Map<String, Value>, event: &str, command: &str) -> HookAddOutcome {
    let event_hooks = hooks
        .entry(event.to_string())
        .or_insert_with(|| json!([]))
        .as_array_mut();

    let Some(event_hooks) = event_hooks else {
        return HookAddOutcome::Skipped;
    };

    // Check if bd hook already registered
    for hook in event_hooks.iter() {
        if hook_contains_command(hook, command) {
            return HookAddOutcome::AlreadyPresent;
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
    HookAddOutcome::Added
}

fn remove_hook_command(hooks: &mut Map<String, Value>, event: &str, command: &str) -> bool {
    let Some(event_hooks) = hooks.get_mut(event).and_then(|h| h.as_array_mut()) else {
        return false;
    };

    let original_len = event_hooks.len();

    event_hooks.retain(|hook| !hook_contains_command(hook, command));

    event_hooks.len() < original_len
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
            if hook_contains_command(hook, "bd prime") {
                return true;
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

pub fn handle_cursor(check: bool, remove: bool) -> Result<()> {
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

    print_line("Installing Cursor integration...")?;

    // Ensure parent directory exists
    if let Some(parent) = rules_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| validation_error("setup", format!("failed to create directory: {e}")))?;
    }

    atomic_write(&rules_path, CURSOR_RULES.as_bytes())?;

    print_line("")?;
    print_line("✓ Cursor integration installed")?;
    print_line(&format!("  Rules: {}", rules_path.display()))?;
    print_line("")?;
    print_line("Restart Cursor for changes to take effect.")?;

    Ok(())
}

fn check_cursor() -> Result<()> {
    let rules_path = PathBuf::from(".cursor/rules/beads.mdc");
    check_cursor_at(&rules_path)
}

fn check_cursor_at(rules_path: &Path) -> Result<()> {
    if rules_path.exists() {
        print_line(&format!(
            "✓ Cursor integration installed: {}",
            rules_path.display()
        ))?;
        Ok(())
    } else {
        print_line("✗ Cursor integration not installed")?;
        print_line("  Run: bd setup cursor")?;
        Err(validation_error(
            "setup",
            "cursor integration not installed",
        ))
    }
}

fn remove_cursor() -> Result<()> {
    let rules_path = PathBuf::from(".cursor/rules/beads.mdc");

    print_line("Removing Cursor integration...")?;

    match fs::remove_file(&rules_path) {
        Ok(_) => {
            print_line("✓ Removed Cursor integration")?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            print_line("No rules file found")?;
        }
        Err(e) => {
            return Err(validation_error(
                "setup",
                format!("failed to remove file: {e}"),
            ));
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

pub fn handle_aider(check: bool, remove: bool) -> Result<()> {
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

    print_line("Installing Aider integration...")?;

    // Ensure .aider directory exists
    fs::create_dir_all(".aider")
        .map_err(|e| validation_error("setup", format!("failed to create directory: {e}")))?;

    // Write config file
    atomic_write(&config_path, AIDER_CONFIG.as_bytes())?;

    // Write instructions file (loaded by AI)
    atomic_write(&instructions_path, AIDER_INSTRUCTIONS.as_bytes())?;

    // Write README (for humans)
    atomic_write(&readme_path, AIDER_README.as_bytes())?;

    print_line("")?;
    print_line("✓ Aider integration installed")?;
    print_line(&format!("  Config: {}", config_path.display()))?;
    print_line(&format!(
        "  Instructions: {} (loaded by AI)",
        instructions_path.display()
    ))?;
    print_line(&format!("  README: {} (for humans)", readme_path.display()))?;
    print_line("")?;
    print_line("Usage:")?;
    print_line("  1. Start aider in this directory")?;
    print_line("  2. Ask AI for available work (it will suggest: /run bd ready)")?;
    print_line("  3. Run suggested commands using /run")?;
    print_line("")?;
    print_line("Note: Aider requires you to explicitly run commands via /run")?;

    Ok(())
}

fn check_aider() -> Result<()> {
    let config_path = PathBuf::from(".aider.conf.yml");
    check_aider_at(&config_path)
}

fn check_aider_at(config_path: &Path) -> Result<()> {
    if config_path.exists() {
        print_line(&format!(
            "✓ Aider integration installed: {}",
            config_path.display()
        ))?;
        Ok(())
    } else {
        print_line("✗ Aider integration not installed")?;
        print_line("  Run: bd setup aider")?;
        Err(validation_error("setup", "aider integration not installed"))
    }
}

fn remove_aider() -> Result<()> {
    let config_path = PathBuf::from(".aider.conf.yml");
    let instructions_path = PathBuf::from(".aider/BEADS.md");
    let readme_path = PathBuf::from(".aider/README.md");
    let aider_dir = PathBuf::from(".aider");

    print_line("Removing Aider integration...")?;

    let mut removed = false;

    for path in [&config_path, &instructions_path, &readme_path] {
        match fs::remove_file(path) {
            Ok(_) => removed = true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(validation_error(
                    "setup",
                    format!("failed to remove file: {e}"),
                ));
            }
        }
    }

    // Try to remove .aider directory if empty
    let _ = fs::remove_dir(&aider_dir);

    if removed {
        print_line("✓ Removed Aider integration")?;
    } else {
        print_line("No Aider integration files found")?;
    }

    Ok(())
}

// =============================================================================
// Helpers
// =============================================================================

fn validation_error(field: impl Into<String>, reason: impl Into<String>) -> SetupError {
    SetupError::Validation {
        field: field.into(),
        reason: reason.into(),
    }
}

fn print_line(line: &str) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    if let Err(e) = writeln!(stdout, "{line}")
        && e.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(SetupError::Io(e));
    }
    Ok(())
}

fn home_dir() -> Result<PathBuf> {
    dirs::home_dir().ok_or_else(|| validation_error("setup", "failed to get home directory"))
}

fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    // Write to temp file then rename for atomicity
    let temp_path = path.with_extension("tmp");

    let mut file = fs::File::create(&temp_path)
        .map_err(|e| validation_error("setup", format!("failed to create temp file: {e}")))?;

    file.write_all(data)
        .map_err(|e| validation_error("setup", format!("failed to write file: {e}")))?;

    file.sync_all()
        .map_err(|e| validation_error("setup", format!("failed to sync file: {e}")))?;

    fs::rename(&temp_path, path)
        .map_err(|e| validation_error("setup", format!("failed to rename file: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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

    #[test]
    fn check_claude_returns_error_when_not_installed() {
        let dir = tempdir().expect("temp dir");
        let global_path = dir.path().join("global.json");
        let project_path = dir.path().join("project.json");
        let err = check_claude_at(&global_path, &project_path).expect_err("expected missing hooks");
        assert!(matches!(err, SetupError::Validation { .. }));
    }

    #[test]
    fn check_claude_detects_hooks() {
        let dir = tempdir().expect("temp dir");
        let settings_path = dir.path().join("settings.json");

        let settings = json!({
            "hooks": {
                "SessionStart": [
                    {
                        "hooks": [
                            {
                                "command": "bd prime"
                            }
                        ]
                    }
                ]
            }
        });

        fs::write(&settings_path, serde_json::to_string(&settings).unwrap()).unwrap();

        let other_path = dir.path().join("other.json");
        let result = check_claude_at(&settings_path, &other_path);
        assert!(result.is_ok());
    }

    #[test]
    fn check_cursor_returns_error_when_not_installed() {
        let dir = tempdir().expect("temp dir");
        let rules_path = dir.path().join("beads.mdc");
        let err = check_cursor_at(&rules_path).expect_err("expected missing rules");
        assert!(matches!(err, SetupError::Validation { .. }));
    }

    #[test]
    fn check_aider_returns_error_when_not_installed() {
        let dir = tempdir().expect("temp dir");
        let config_path = dir.path().join(".aider.conf.yml");
        let err = check_aider_at(&config_path).expect_err("expected missing config");
        assert!(matches!(err, SetupError::Validation { .. }));
    }

    #[test]
    fn test_hook_add_remove_logic() {
        let mut hooks = Map::new();
        let event = "SessionStart";
        let command = "bd prime";

        // Initial add
        let outcome = add_hook_command(&mut hooks, event, command);
        assert!(matches!(outcome, HookAddOutcome::Added));

        let event_hooks = hooks.get(event).unwrap().as_array().unwrap();
        assert_eq!(event_hooks.len(), 1);

        // Verify structure roughly
        let hook = &event_hooks[0];
        let cmds = hook.get("hooks").unwrap().as_array().unwrap();
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].get("command").unwrap().as_str().unwrap(), command);

        // Duplicate add
        let outcome = add_hook_command(&mut hooks, event, command);
        assert!(matches!(outcome, HookAddOutcome::AlreadyPresent));

        let event_hooks = hooks.get(event).unwrap().as_array().unwrap();
        assert_eq!(event_hooks.len(), 1);

        // Remove
        let removed = remove_hook_command(&mut hooks, event, command);
        assert!(removed);

        let event_hooks = hooks.get(event).unwrap().as_array().unwrap();
        assert_eq!(event_hooks.len(), 0);

        // Remove again
        let removed = remove_hook_command(&mut hooks, event, command);
        assert!(!removed);
    }
}
