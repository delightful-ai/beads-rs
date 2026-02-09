use std::io::{BufRead, Write};

use crate::parsers::{parse_bead_type, parse_priority};
use crate::validation::{normalize_bead_id_for, normalize_dep_specs, validation_error};
use clap::Args;

use super::{CommandResult, print_ok};
use crate::render::{print_json, print_line};
use crate::runtime::{CliRuntimeCtx, resolve_description, send, send_raw};
use beads_api::{Issue, QueryResult};
use beads_core::{BeadId, BeadType, Priority};
use beads_surface::OpResult;
use beads_surface::ipc::{CreatePayload, IdPayload, Request, Response, ResponsePayload};

#[derive(Args, Debug)]
pub struct CreateArgs {
    /// Title (positional).
    #[arg(value_name = "TITLE", required = false)]
    pub title: Option<String>,

    /// Title (compat: `--title`).
    #[arg(long = "title", alias = "title-flag", value_name = "TITLE")]
    pub title_flag: Option<String>,

    /// Create multiple issues from a markdown file.
    #[arg(short = 'f', long = "file", value_name = "PATH")]
    pub file: Option<std::path::PathBuf>,

    /// Issue type.
    #[arg(short = 't', long = "type", alias = "issue-type", value_parser = parse_bead_type)]
    pub bead_type: Option<BeadType>,

    /// Priority 0-4 or words like "high".
    #[arg(short = 'p', long, value_parser = parse_priority)]
    pub priority: Option<Priority>,

    /// Description.
    #[arg(short = 'd', long, allow_hyphen_values = true)]
    pub description: Option<String>,

    /// Alias for --description (GitHub CLI convention).
    #[arg(long = "body", hide = true, allow_hyphen_values = true)]
    pub body: Option<String>,

    /// Assignee (compat; only supports current actor).
    #[arg(short = 'a', long)]
    pub assignee: Option<String>,

    /// Labels (comma-separated or repeat).
    #[arg(short = 'l', long = "labels", value_delimiter = ',', num_args = 0..)]
    pub labels: Vec<String>,

    /// Alias for --labels.
    #[arg(long = "label", hide = true, value_delimiter = ',', num_args = 0..)]
    pub label: Vec<String>,

    /// Design text.
    #[arg(long, allow_hyphen_values = true)]
    pub design: Option<String>,

    /// Acceptance criteria text.
    #[arg(
        long = "acceptance",
        alias = "acceptance-criteria",
        allow_hyphen_values = true
    )]
    pub acceptance: Option<String>,

    /// External reference (e.g., "gh-9", "jira-ABC").
    #[arg(long = "external-ref")]
    pub external_ref: Option<String>,

    /// Explicit issue ID (partitioning).
    #[arg(long)]
    pub id: Option<String>,

    /// Parent bead id (adds `parent` dep).
    #[arg(long)]
    pub parent: Option<String>,

    /// Dependencies (repeat or comma-separated): "type:id" or "id" (defaults to blocks).
    #[arg(long = "deps", value_delimiter = ',', num_args = 0..)]
    pub deps: Vec<String>,

    /// Time estimate in minutes.
    #[arg(short = 'e', long)]
    pub estimate: Option<u32>,

    /// No-op (compat). `bd` doesn't require forcing.
    #[arg(long)]
    pub force: bool,
}

pub fn handle(ctx: &CliRuntimeCtx, mut args: CreateArgs) -> CommandResult<()> {
    if let Some(path) = args.file.take() {
        if args.title.is_some() || args.title_flag.is_some() {
            return Err(
                validation_error("create", "cannot specify both title and --file flag").into(),
            );
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
        tracing::warn!("note: --force is ignored in bd (not required)");
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
        ctx: ctx.mutation_ctx(),
        payload: CreatePayload {
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
        },
    };

    let created_payload = send(&req)?;
    let issue = match &created_payload {
        ResponsePayload::Op(op) => match &op.result {
            OpResult::Created { id } => match &op.issue {
                Some(issue) => issue.clone(),
                None => fetch_issue(ctx, id)?,
            },
            _ => {
                print_ok(&created_payload, ctx.json)?;
                return Ok(());
            }
        },
        ResponsePayload::Query(QueryResult::Issue(attached)) => attached.clone(),
        _ => {
            print_ok(&created_payload, ctx.json)?;
            return Ok(());
        }
    };

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
        print_line(&render_create(&issue))?;
    }
    Ok(())
}

pub fn render_created(id: &str) -> String {
    format!("✓ Created issue: {id}")
}

fn render_create(issue: &Issue) -> String {
    let mut out = String::new();
    out.push_str(&format!("✓ Created issue: {}\n", issue.id));
    out.push_str(&format!("  Title: {}\n", issue.title));
    out.push_str(&format!("  Priority: P{}\n", issue.priority));
    out.push_str(&format!("  Status: {}", issue.status.as_str()));
    out
}

fn render_created_from_markdown(created_summaries: &[CreatedSummary], file_label: &str) -> String {
    let mut out = format!(
        "✓ Created {} issues from {}:\n",
        created_summaries.len(),
        file_label
    );
    for issue in created_summaries {
        out.push_str(&format!(
            "  {}: {} [P{}, {}]\n",
            issue.id,
            issue.title,
            issue.priority.value(),
            issue.bead_type.as_str()
        ));
    }
    out.trim_end().into()
}

fn resolve_title(positional: Option<String>, flag: Option<String>) -> CommandResult<String> {
    match (positional, flag) {
        (Some(p), Some(f)) => {
            if p != f {
                return Err(validation_error(
                    "title",
                    format!(
                        "cannot specify different titles as both positional argument and --title flag (positional={p:?}, --title={f:?})"
                    ),
                )
                .into());
            }
            Ok(p)
        }
        (Some(p), None) => Ok(p),
        (None, Some(f)) => Ok(f),
        (None, None) => Err(validation_error(
            "title",
            "title required (or use --file to create from markdown)",
        )
        .into()),
    }
}

fn handle_from_markdown_file(ctx: &CliRuntimeCtx, path: &std::path::Path) -> CommandResult<()> {
    let templates = parse_markdown_file(path)?;
    if templates.is_empty() {
        return Err(validation_error("file", "no issues found in markdown file").into());
    }

    let mut created = Vec::new();
    let mut created_summaries = Vec::new();
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
            ctx: ctx.mutation_ctx(),
            payload: CreatePayload {
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
            },
        };

        // Best-effort: keep going on per-issue failures.
        let resp = send_raw(&req)?;
        let created_id = match resp {
            Response::Ok {
                ok: ResponsePayload::Op(op),
            } => match op.result {
                OpResult::Created { id } => id,
                _ => {
                    failed.push(t.title.clone());
                    tracing::warn!(
                        "warning: unexpected response creating {:?}: {:?}",
                        t.title,
                        op.result
                    );
                    continue;
                }
            },
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

        if ctx.json {
            let issue = fetch_issue(ctx, &created_id)?;
            created.push(issue);
        } else {
            created_summaries.push(CreatedSummary {
                id: created_id.as_str().to_string(),
                title: t.title.clone(),
                priority: t.priority,
                bead_type: t.bead_type,
            });
        }
    }

    if !failed.is_empty() {
        tracing::error!("✗ Failed to create {} issue(s):", failed.len());
        for title in &failed {
            tracing::error!("  - {title}");
        }
    }

    if ctx.json {
        print_json(&created)?;
        return Ok(());
    }

    let file_label = path.display();
    print_line(&render_created_from_markdown(
        &created_summaries,
        &file_label.to_string(),
    ))?;
    Ok(())
}

#[derive(Debug, Clone)]
struct MarkdownIssue {
    title: String,
    description: String,
    design: Option<String>,
    acceptance_criteria: Option<String>,
    bead_type: BeadType,
    priority: Priority,
    assignee: Option<String>,
    labels: Vec<String>,
    dependencies: Vec<String>,
}

#[derive(Debug, Clone)]
struct CreatedSummary {
    id: String,
    title: String,
    priority: Priority,
    bead_type: BeadType,
}

fn parse_markdown_file(path: &std::path::Path) -> CommandResult<Vec<MarkdownIssue>> {
    validate_markdown_path(path)?;

    let file = std::fs::File::open(path).map_err(beads_surface::IpcError::from)?;
    let reader = std::io::BufReader::new(file);

    let mut issues: Vec<MarkdownIssue> = Vec::new();
    let mut current: Option<MarkdownIssue> = None;
    let mut current_section: Option<String> = None;
    let mut section_buf: Vec<String> = Vec::new();

    for line in reader.lines() {
        let line = line.map_err(beads_surface::IpcError::from)?;
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
                priority: Priority::default(),
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
) -> CommandResult<()> {
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

fn validate_markdown_path(path: &std::path::Path) -> CommandResult<()> {
    let clean = std::path::PathBuf::from(path);
    if clean
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(
            validation_error("file", "invalid file path: directory traversal not allowed").into(),
        );
    }

    let ext = clean
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();
    if ext != "md" && ext != "markdown" {
        return Err(validation_error(
            "file",
            "invalid file type: only .md and .markdown are supported",
        )
        .into());
    }

    let meta = std::fs::metadata(&clean).map_err(beads_surface::IpcError::from)?;
    if meta.is_dir() {
        return Err(validation_error("file", "path is a directory, not a file").into());
    }

    Ok(())
}

fn parse_md_priority(s: &str) -> Priority {
    let t = s.trim().trim_start_matches('p').trim_start_matches('P');
    if let Ok(n) = t.parse::<u8>() {
        return Priority::new(n).unwrap_or_default();
    }
    Priority::default()
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

fn fetch_issue(ctx: &CliRuntimeCtx, id: &BeadId) -> CommandResult<Issue> {
    let req = Request::Show {
        ctx: ctx.read_ctx(),
        payload: IdPayload { id: id.clone() },
    };
    match send(&req)? {
        ResponsePayload::Query(QueryResult::Issue(issue)) => Ok(issue),
        other => Err(beads_surface::ipc::IpcError::DaemonUnavailable(format!(
            "unexpected response for show: {other:?}"
        ))
        .into()),
    }
}
