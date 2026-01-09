use std::io::{BufRead, Write};

use super::super::render;
use super::super::{
    CreateArgs, Ctx, fetch_issue, normalize_bead_id_for, normalize_dep_specs, print_ok,
    resolve_description, send,
};
use crate::core::BeadType;
use crate::daemon::ipc::{Request, Response, ResponsePayload, send_request};
use crate::daemon::ops::OpResult;
use crate::daemon::query::QueryResult;
use crate::{Error, Result};

pub(crate) fn handle(ctx: &Ctx, mut args: CreateArgs) -> Result<()> {
    if let Some(path) = args.file.take() {
        if args.title.is_some() || args.title_flag.is_some() {
            return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
                field: "create".into(),
                reason: "cannot specify both title and --file flag".into(),
            }));
        }
        return handle_from_markdown_file(ctx, &path);
    }

    let title = resolve_title(args.title.take(), args.title_flag.take())?;
    let description = resolve_description(args.description.take(), args.body.take())?;

    let bead_type = args.bead_type.unwrap_or(BeadType::Task);
    let priority = args.priority.unwrap_or_default();

    let mut labels = args.labels;
    labels.extend(args.label);

    if args.force && !ctx.json {
        tracing::warn!("note: --force is ignored in beads-rs (not required)");
    }

    // Warn if creating without description (unless title contains "test")
    let warn_no_desc = description.is_none() && !title.to_lowercase().contains("test");

    let id = args
        .id
        .take()
        .map(|raw| normalize_bead_id_for("id", &raw))
        .transpose()?;
    let parent = args
        .parent
        .take()
        .map(|raw| normalize_bead_id_for("parent", &raw))
        .transpose()?;
    let dependencies = normalize_dep_specs(args.deps)?;

    let req = Request::Create {
        repo: ctx.repo.clone(),
        id,
        parent,
        title,
        bead_type,
        priority,
        description,
        design: args.design,
        acceptance_criteria: args.acceptance,
        assignee: args.assignee,
        external_ref: args.external_ref,
        estimated_minutes: args.estimate,
        labels,
        dependencies,
    };

    let created_payload = send(&req)?;
    let created_id = match created_payload {
        ResponsePayload::Op(OpResult::Created { ref id }) => id.as_str().to_string(),
        _ => {
            print_ok(&created_payload, ctx.json)?;
            return Ok(());
        }
    };

    let issue = fetch_issue(ctx, &created_id)?;

    // Print warning to stderr (visible even in --json mode)
    if warn_no_desc {
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(
            stderr,
            "⚠ Creating issue '{}' without description. Issues without descriptions lack context for future work.",
            issue.title
        );
    }
    if ctx.json {
        print_ok(&ResponsePayload::Query(QueryResult::Issue(issue)), true)?;
    } else {
        println!("{}", render::render_create(&issue));
    }
    Ok(())
}

fn resolve_title(positional: Option<String>, flag: Option<String>) -> Result<String> {
    match (positional, flag) {
        (Some(p), Some(f)) => {
            if p != f {
                return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
                    field: "title".into(),
                    reason: format!(
                        "cannot specify different titles as both positional argument and --title flag (positional={p:?}, --title={f:?})"
                    ),
                }));
            }
            Ok(p)
        }
        (Some(p), None) => Ok(p),
        (None, Some(f)) => Ok(f),
        (None, None) => Err(Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "title".into(),
            reason: "title required (or use --file to create from markdown)".into(),
        })),
    }
}

fn handle_from_markdown_file(ctx: &Ctx, path: &std::path::Path) -> Result<()> {
    let templates = parse_markdown_file(path)?;
    if templates.is_empty() {
        return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "file".into(),
            reason: "no issues found in markdown file".into(),
        }));
    }

    let mut created = Vec::new();
    let mut failed = Vec::new();

    for t in templates {
        let dependencies = match normalize_dep_specs(t.dependencies.clone()) {
            Ok(v) => v,
            Err(e) => {
                failed.push(t.title.clone());
                tracing::error!("error creating issue {:?}: {e}", t.title);
                continue;
            }
        };

        let req = Request::Create {
            repo: ctx.repo.clone(),
            id: None,
            parent: None,
            title: t.title.clone(),
            bead_type: t.bead_type,
            priority: t.priority,
            description: Some(t.description.clone()),
            design: t.design.clone(),
            acceptance_criteria: t.acceptance_criteria.clone(),
            assignee: t.assignee.clone(),
            external_ref: None,
            estimated_minutes: None,
            labels: t.labels.clone(),
            dependencies,
        };

        // Best-effort: keep going on per-issue failures.
        let resp = send_request(&req)?;
        let created_id = match resp {
            Response::Ok {
                ok: ResponsePayload::Op(OpResult::Created { id }),
            } => id.as_str().to_string(),
            Response::Ok { ok } => {
                // Unexpected ok payload; treat as failure for bulk mode.
                failed.push(t.title.clone());
                tracing::warn!(
                    "warning: unexpected response creating {:?}: {ok:?}",
                    t.title
                );
                continue;
            }
            Response::Err { err } => {
                failed.push(t.title.clone());
                tracing::error!(
                    "error creating issue {:?}: {} - {}",
                    t.title,
                    err.code,
                    err.message
                );
                continue;
            }
        };

        let issue = fetch_issue(ctx, &created_id)?;
        created.push(issue);
    }

    if !failed.is_empty() {
        tracing::error!("✗ Failed to create {} issue(s):", failed.len());
        for title in &failed {
            tracing::error!("  - {title}");
        }
    }

    if ctx.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&created).map_err(crate::daemon::IpcError::from)?
        );
        return Ok(());
    }

    let file_label = path.display();
    println!("✓ Created {} issues from {}:", created.len(), file_label);
    for issue in &created {
        println!(
            "  {}: {} [P{}, {}]",
            issue.id, issue.title, issue.priority, issue.issue_type
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct MarkdownIssue {
    title: String,
    description: String,
    design: Option<String>,
    acceptance_criteria: Option<String>,
    bead_type: BeadType,
    priority: crate::core::Priority,
    assignee: Option<String>,
    labels: Vec<String>,
    dependencies: Vec<String>,
}

fn parse_markdown_file(path: &std::path::Path) -> Result<Vec<MarkdownIssue>> {
    validate_markdown_path(path)?;

    let file = std::fs::File::open(path).map_err(crate::daemon::IpcError::from)?;
    let reader = std::io::BufReader::new(file);

    let mut issues: Vec<MarkdownIssue> = Vec::new();
    let mut current: Option<MarkdownIssue> = None;
    let mut current_section: Option<String> = None;
    let mut section_buf: Vec<String> = Vec::new();

    for line in reader.lines() {
        let line = line.map_err(crate::daemon::IpcError::from)?;
        let trimmed = line.trim_end();

        if let Some(rest) = trimmed.strip_prefix("### ") {
            if let Some(ref mut issue) = current {
                flush_md_section(issue, &mut current_section, &mut section_buf)?;
                current_section = Some(rest.trim().to_string());
            }
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix("## ") {
            if let Some(ref mut issue) = current {
                flush_md_section(issue, &mut current_section, &mut section_buf)?;
            }
            if let Some(issue) = current.take() {
                issues.push(issue);
            }

            current = Some(MarkdownIssue {
                title: rest.trim().to_string(),
                description: String::new(),
                design: None,
                acceptance_criteria: None,
                bead_type: BeadType::Task,
                priority: crate::core::Priority::default(),
                assignee: None,
                labels: Vec::new(),
                dependencies: Vec::new(),
            });
            current_section = None;
            section_buf.clear();
            continue;
        }

        let Some(issue) = current.as_mut() else {
            continue;
        };
        if current_section.is_some() {
            section_buf.push(line);
            continue;
        }

        // Before any ### sections: treat as (multi-line) description.
        if !trimmed.trim().is_empty() {
            if !issue.description.is_empty() {
                issue.description.push('\n');
            }
            issue.description.push_str(trimmed);
        }
    }

    if let Some(ref mut issue) = current {
        flush_md_section(issue, &mut current_section, &mut section_buf)?;
    }
    if let Some(issue) = current {
        issues.push(issue);
    }

    Ok(issues)
}

fn flush_md_section(
    issue: &mut MarkdownIssue,
    current_section: &mut Option<String>,
    section_buf: &mut Vec<String>,
) -> Result<()> {
    let Some(section) = current_section.take() else {
        return Ok(());
    };
    let content = section_buf.join("\n").trim().to_string();
    section_buf.clear();
    if content.is_empty() {
        return Ok(());
    }

    match section.to_lowercase().as_str() {
        "priority" => {
            issue.priority = parse_md_priority(&content);
        }
        "type" => {
            issue.bead_type = parse_md_type(&content);
        }
        "description" => {
            issue.description = content;
        }
        "design" => {
            issue.design = Some(content);
        }
        "acceptance criteria" | "acceptance" => {
            issue.acceptance_criteria = Some(content);
        }
        "assignee" => {
            let v = content.trim();
            issue.assignee = if v.is_empty() {
                None
            } else {
                Some(v.to_string())
            };
        }
        "labels" => {
            issue.labels = parse_md_list(&content);
        }
        "dependencies" | "deps" => {
            issue.dependencies = parse_md_list(&content);
        }
        _ => {}
    }

    Ok(())
}

fn validate_markdown_path(path: &std::path::Path) -> Result<()> {
    let clean = std::path::PathBuf::from(path);
    if clean
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "file".into(),
            reason: "invalid file path: directory traversal not allowed".into(),
        }));
    }

    let ext = clean
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();
    if ext != "md" && ext != "markdown" {
        return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "file".into(),
            reason: "invalid file type: only .md and .markdown are supported".into(),
        }));
    }

    let meta = std::fs::metadata(&clean).map_err(crate::daemon::IpcError::from)?;
    if meta.is_dir() {
        return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "file".into(),
            reason: "path is a directory, not a file".into(),
        }));
    }

    Ok(())
}

fn parse_md_priority(s: &str) -> crate::core::Priority {
    let t = s.trim().trim_start_matches('p').trim_start_matches('P');
    if let Ok(n) = t.parse::<u8>() {
        return crate::core::Priority::new(n).unwrap_or_default();
    }
    crate::core::Priority::default()
}

fn parse_md_type(s: &str) -> BeadType {
    match s.trim().to_lowercase().as_str() {
        "bug" | "bugs" => BeadType::Bug,
        "feature" | "features" | "feat" => BeadType::Feature,
        "epic" | "epics" => BeadType::Epic,
        "chore" | "chores" | "maintenance" => BeadType::Chore,
        _ => BeadType::Task,
    }
}

fn parse_md_list(content: &str) -> Vec<String> {
    content
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter_map(|s| {
            let t = s.trim();
            if t.is_empty() {
                None
            } else {
                Some(t.to_string())
            }
        })
        .collect()
}
