use clap::Args;

use super::super::render;
use super::super::{
    Ctx, fetch_issue, normalize_bead_id, normalize_bead_id_for, normalize_dep_specs, print_ok,
    resolve_description, send,
};
use crate::api::QueryResult;
use crate::cli::parse::{parse_bead_type, parse_priority, parse_status as parse_status_arg};
use crate::core::{BeadType, Priority};
use crate::core::{DepKind, WorkflowStatus};
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::ops::{BeadPatch, BeadPatchExt, Patch};
use crate::{Error, Result};

#[derive(Args, Debug)]
pub struct UpdateArgs {
    pub id: String,

    /// Reparent the bead (adds/removes `parent` dependency).
    #[arg(long)]
    pub parent: Option<String>,

    /// Remove any existing parent relationship.
    #[arg(long = "no-parent", conflicts_with = "parent")]
    pub no_parent: bool,

    #[arg(long)]
    pub title: Option<String>,

    #[arg(short = 'd', long, allow_hyphen_values = true)]
    pub description: Option<String>,

    /// Alias for --description (GitHub CLI convention).
    #[arg(long = "body", hide = true, allow_hyphen_values = true)]
    pub body: Option<String>,

    #[arg(long, allow_hyphen_values = true)]
    pub design: Option<String>,

    #[arg(
        long = "acceptance",
        alias = "acceptance-criteria",
        allow_hyphen_values = true
    )]
    pub acceptance: Option<String>,

    /// External reference (e.g., "gh-9", "jira-ABC").
    #[arg(long = "external-ref")]
    pub external_ref: Option<String>,

    /// Time estimate in minutes.
    #[arg(short = 'e', long)]
    pub estimate: Option<u32>,

    #[arg(short = 's', long, value_parser = parse_status_arg)]
    pub status: Option<String>,

    /// Close reason (only valid with --status=closed).
    #[arg(long, allow_hyphen_values = true)]
    pub reason: Option<String>,

    #[arg(short = 'p', long, value_parser = parse_priority)]
    pub priority: Option<Priority>,

    /// Change the issue type (bug, feature, task, epic, chore).
    #[arg(short = 't', long = "type", alias = "issue-type", value_parser = parse_bead_type)]
    pub bead_type: Option<BeadType>,

    /// Compat: assignee/claim.
    #[arg(short = 'a', long)]
    pub assignee: Option<String>,

    #[arg(long = "add-label", alias = "add_label", value_delimiter = ',', num_args = 0..)]
    pub add_label: Vec<String>,

    #[arg(long = "remove-label", alias = "remove_label", value_delimiter = ',', num_args = 0..)]
    pub remove_label: Vec<String>,

    /// Add a note.
    #[arg(long = "notes", alias = "note", allow_hyphen_values = true)]
    pub notes: Option<String>,

    /// Dependencies to add (repeat or comma-separated): "type:id" or "id" (defaults to blocks).
    #[arg(long = "deps", value_delimiter = ',', num_args = 0..)]
    pub deps: Vec<String>,
}

pub(crate) fn handle(ctx: &Ctx, mut args: UpdateArgs) -> Result<()> {
    let id = normalize_bead_id(&args.id)?;
    let id_str = id.as_str().to_string();
    let mut patch = BeadPatch::default();
    let close_reason = normalize_reason(args.reason);
    let status = match args.status.as_deref() {
        Some(raw) => Some(parse_status(raw)?),
        None => None,
    };
    let status_closed = matches!(status, Some(WorkflowStatus::Closed));
    let add_labels = std::mem::take(&mut args.add_label);
    let remove_labels = std::mem::take(&mut args.remove_label);

    if close_reason.is_some() && !status_closed {
        return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "reason".into(),
            reason: "--reason requires --status=closed".into(),
        }));
    }

    let parent_action = if args.no_parent {
        Some(None)
    } else if let Some(p) = &args.parent {
        let v = p.trim();
        if v.is_empty()
            || v == "-"
            || v.eq_ignore_ascii_case("none")
            || v.eq_ignore_ascii_case("null")
        {
            Some(None)
        } else {
            Some(Some(
                normalize_bead_id_for("parent", v)?.as_str().to_string(),
            ))
        }
    } else {
        None
    };

    if let Some(title) = args.title {
        patch.title = Patch::Set(title);
    }
    if let Some(desc) = resolve_description(args.description, args.body)? {
        patch.description = Patch::Set(desc);
    }
    if let Some(design) = args.design {
        patch.design = Patch::Set(design);
    }
    if let Some(acc) = args.acceptance {
        patch.acceptance_criteria = Patch::Set(acc);
    }
    if let Some(external_ref) = args.external_ref {
        let v = external_ref.trim();
        if v.is_empty() || v == "-" || v.eq_ignore_ascii_case("none") {
            patch.external_ref = Patch::Clear;
        } else {
            patch.external_ref = Patch::Set(v.to_string());
        }
    }
    if let Some(m) = args.estimate {
        patch.estimated_minutes = Patch::Set(m);
    }
    if let Some(priority) = args.priority {
        patch.priority = Patch::Set(priority);
    }
    if let Some(bead_type) = args.bead_type {
        patch.bead_type = Patch::Set(bead_type);
    }
    if let Some(status) = status {
        if status == WorkflowStatus::Closed && close_reason.is_some() {
            // Close via explicit close op to preserve reason.
        } else {
            patch.status = Patch::Set(status);
        }
    }

    if !patch.is_empty() {
        patch.validate()?;
        let req = Request::Update {
            repo: ctx.repo.clone(),
            id: id_str.clone(),
            patch,
            cas: None,
            meta: ctx.mutation_meta(),
        };
        let _ = send(&req)?;
    }

    if !add_labels.is_empty() {
        let req = Request::AddLabels {
            repo: ctx.repo.clone(),
            id: id_str.clone(),
            labels: add_labels,
            meta: ctx.mutation_meta(),
        };
        let _ = send(&req)?;
    }

    if !remove_labels.is_empty() {
        let req = Request::RemoveLabels {
            repo: ctx.repo.clone(),
            id: id_str.clone(),
            labels: remove_labels,
            meta: ctx.mutation_meta(),
        };
        let _ = send(&req)?;
    }

    // Parent relationship (child -> parent edge).
    if let Some(new_parent) = parent_action {
        let req = Request::SetParent {
            repo: ctx.repo.clone(),
            id: id_str.clone(),
            parent: new_parent,
            meta: ctx.mutation_meta(),
        };
        let _ = send(&req)?;
    }

    // Add dependencies
    if !args.deps.is_empty() {
        let dep_specs = normalize_dep_specs(args.deps)?;
        for spec in dep_specs {
            let (kind, to) = if let Some((k, i)) = spec.split_once(':') {
                (DepKind::parse(k).unwrap_or(DepKind::Blocks), i.to_string())
            } else {
                (DepKind::Blocks, spec)
            };
            let _ = send(&Request::AddDep {
                repo: ctx.repo.clone(),
                from: id_str.clone(),
                to,
                kind,
                meta: ctx.mutation_meta(),
            })?;
        }
    }

    // Notes
    if let Some(content) = args.notes {
        let note = Request::AddNote {
            repo: ctx.repo.clone(),
            id: id_str.clone(),
            content,
            meta: ctx.mutation_meta(),
        };
        let _ = send(&note)?;
    }

    // Assignee compat
    if let Some(assignee) = args.assignee {
        if assignee == "none" || assignee == "-" || assignee == "unassigned" {
            let req = Request::Unclaim {
                repo: ctx.repo.clone(),
                id: id_str.clone(),
                meta: ctx.mutation_meta(),
            };
            let _ = send(&req)?;
        } else {
            let current = ctx.actor_string()?;
            if !assignee.is_empty() && assignee != "me" && assignee != "self" && assignee != current
            {
                return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
                    field: "assignee".into(),
                    reason: "cannot assign other actors; run bd as that actor".into(),
                }));
            }
            let req = Request::Claim {
                repo: ctx.repo.clone(),
                id: id_str.clone(),
                lease_secs: 3600,
                meta: ctx.mutation_meta(),
            };
            let _ = send(&req)?;
        }
    }

    if status_closed && close_reason.is_some() {
        let req = Request::Close {
            repo: ctx.repo.clone(),
            id: id_str.clone(),
            reason: close_reason,
            on_branch: None,
            meta: ctx.mutation_meta(),
        };
        let _ = send(&req)?;
    }

    // Emit updated view / summary.
    if ctx.json {
        let issue = fetch_issue(ctx, &id)?;
        print_ok(&ResponsePayload::Query(QueryResult::Issue(issue)), true)?;
    } else {
        println!("{}", render::render_updated(&id_str));
    }
    Ok(())
}

fn normalize_reason(reason: Option<String>) -> Option<String> {
    reason.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty()
            || trimmed == "-"
            || trimmed.eq_ignore_ascii_case("none")
            || trimmed.eq_ignore_ascii_case("null")
        {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn parse_status(raw: &str) -> Result<WorkflowStatus> {
    WorkflowStatus::parse(raw).ok_or_else(|| {
        Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "status".into(),
            reason: format!("unknown status {raw:?}"),
        })
    })
}
