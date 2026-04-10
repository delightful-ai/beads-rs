use clap::Args;
use std::io::Write;
use std::path::{Path, PathBuf};

const LOCAL_AGENT_MARKERS: &[&str] = &[
    ".aider.conf.yml",
    ".cursor/rules/beads.mdc",
    ".codex/AGENTS.md",
    ".gemini/GEMINI.md",
    ".opencode/AGENTS.md",
    ".windsurf/rules/beads.md",
];

#[derive(Args, Debug, Clone, Default)]
pub struct PrimeArgs {
    /// Force full CLI context output.
    #[arg(long, conflicts_with = "mcp")]
    pub full: bool,

    /// Force compact MCP/editor-hook output.
    #[arg(long, conflicts_with = "full")]
    pub mcp: bool,

    /// Prefer a no-git close protocol for agent sessions.
    #[arg(long)]
    pub stealth: bool,

    /// Output the built-in default content and ignore PRIME.md overrides.
    #[arg(long)]
    pub export: bool,
}

/// Output workflow context when the caller is in a beads repository.
///
/// Callers are responsible for determining repo status and handling
/// broken-pipe behavior appropriate for their process boundary.
pub fn write_context_if<W: Write>(
    out: &mut W,
    in_beads_repo: bool,
    args: &PrimeArgs,
) -> std::io::Result<()> {
    if !in_beads_repo {
        return Ok(());
    }

    if !args.export
        && let Some(override_path) = resolve_override_path(Path::new(".beads/PRIME.md"))
        && let Ok(content) = std::fs::read_to_string(override_path)
    {
        return write!(out, "{content}");
    }

    write!(out, "{}", render_context(args))
}

pub fn render_context(args: &PrimeArgs) -> String {
    let mode = if args.mcp {
        PrimeMode::Mcp
    } else if args.full {
        PrimeMode::Full
    } else if is_mcp_active() {
        PrimeMode::Mcp
    } else {
        PrimeMode::Full
    };

    match mode {
        PrimeMode::Full => full_context(args.stealth),
        PrimeMode::Mcp => mcp_context(args.stealth),
    }
}

fn full_context(stealth: bool) -> String {
    let close_protocol = if stealth {
        "Before saying done: summarize what changed, run the required verification, and leave git push/manual publish decisions to the user."
    } else {
        "Before saying done: summarize what changed, run the required verification, and handle the normal local jj/git workflow for this repo."
    };

    format!(
        r#"# Beads Workflow

Track ALL work in beads. No TodoWrite, no markdown TODOs.
Sync is automatic (~500ms). Run `bd prime` after context compaction.

## Session Close

{}

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
"#,
        close_protocol
    )
}

fn mcp_context(stealth: bool) -> String {
    let close_protocol = if stealth {
        "Close protocol: verify and summarize only; do not assume git operations."
    } else {
        "Close protocol: verify, summarize, and follow the repo's normal jj/git rhythm."
    };

    format!(
        r#"# Beads Workflow

- Track all work in `bd`, not markdown TODOs.
- Start with `bd ready`, inspect with `bd show <id>`, and create follow-up work with `bd create`.
- Sync is automatic; rerun `bd prime` after context compaction.
- Use `bd help` for grouped command discovery and `bd help --advanced <cmd>` for transport flags.
- {}
"#,
        close_protocol
    )
}

fn resolve_override_path(local_override: &Path) -> Option<PathBuf> {
    if local_override.exists() {
        return Some(local_override.to_path_buf());
    }

    let global_override = dirs::config_dir()?.join("beads/PRIME.md");
    if global_override.exists() {
        return Some(global_override);
    }
    None
}

fn is_mcp_active() -> bool {
    let cwd = std::env::current_dir().ok();
    let home = dirs::home_dir().or_else(|| std::env::var_os("HOME").map(PathBuf::from));
    is_mcp_active_in(cwd.as_deref(), home.as_deref())
}

fn is_mcp_active_in(cwd: Option<&Path>, home: Option<&Path>) -> bool {
    if let Some(cwd) = cwd {
        if LOCAL_AGENT_MARKERS
            .iter()
            .any(|marker| cwd.join(marker).exists())
        {
            return true;
        }
        if claude_settings_has_beads_server(cwd.join(".claude/settings.local.json")) {
            return true;
        }
    }

    let Some(home) = home else {
        return false;
    };
    claude_settings_has_beads_server(home.join(".claude/settings.json"))
}

fn claude_settings_has_beads_server(path: PathBuf) -> bool {
    let Ok(data) = std::fs::read_to_string(path) else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&data) else {
        return false;
    };
    let Some(servers) = value
        .get("mcpServers")
        .and_then(|servers| servers.as_object())
    else {
        return false;
    };
    servers
        .keys()
        .any(|key| key.to_ascii_lowercase().contains("beads"))
}

enum PrimeMode {
    Full,
    Mcp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_context_is_not_empty() {
        let ctx = render_context(&PrimeArgs {
            full: true,
            ..PrimeArgs::default()
        });
        assert!(!ctx.is_empty());
        assert!(ctx.contains("Beads Workflow"));
        assert!(ctx.contains("bd ready"));
        assert!(ctx.contains("bd create"));
    }

    #[test]
    fn write_context_if_is_silent_outside_beads_repo() {
        let mut out = Vec::new();
        write_context_if(&mut out, false, &PrimeArgs::default()).expect("write");
        assert!(out.is_empty());
    }

    #[test]
    fn write_context_if_outputs_context_in_beads_repo() {
        let mut out = Vec::new();
        let args = PrimeArgs {
            full: true,
            ..PrimeArgs::default()
        };
        write_context_if(&mut out, true, &args).expect("write");
        let rendered = String::from_utf8(out).expect("utf8");
        assert_eq!(rendered, render_context(&args));
    }

    #[test]
    fn mcp_context_is_compact() {
        let rendered = render_context(&PrimeArgs {
            mcp: true,
            ..PrimeArgs::default()
        });
        assert!(rendered.contains("Close protocol:"));
        assert!(rendered.contains("bd ready"));
    }

    #[test]
    fn resolve_override_path_prefers_local_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let local = dir.path().join("PRIME.md");
        std::fs::write(&local, "local").expect("write local");
        let resolved = resolve_override_path(&local).expect("override");
        assert_eq!(resolved, local);
    }

    #[test]
    fn mcp_detection_recognizes_local_editor_markers() {
        let dir = tempfile::tempdir().expect("temp dir");
        let marker = dir.path().join(".codex/AGENTS.md");
        std::fs::create_dir_all(marker.parent().expect("parent")).expect("mkdirs");
        std::fs::write(&marker, "beads").expect("write marker");
        assert!(is_mcp_active_in(Some(dir.path()), None));
    }

    #[test]
    fn mcp_detection_recognizes_project_claude_settings() {
        let dir = tempfile::tempdir().expect("temp dir");
        let settings = dir.path().join(".claude/settings.local.json");
        std::fs::create_dir_all(settings.parent().expect("parent")).expect("mkdirs");
        std::fs::write(
            &settings,
            r#"{"mcpServers":{"beads":{"command":"bd","args":["mcp","serve"]}}}"#,
        )
        .expect("write settings");
        assert!(is_mcp_active_in(Some(dir.path()), None));
    }
}
