use super::super::render;
use super::super::{
    Ctx, UpdateArgs, fetch_issue, normalize_bead_id, normalize_bead_id_for, normalize_dep_specs,
    print_ok, resolve_description, send,
};
use crate::core::{DepKind, WorkflowStatus};
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::ops::{BeadPatch, Patch};
use crate::api::QueryResult;
use crate::{Error, Result};

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
    let issue = fetch_issue(ctx, &id)?;
    if ctx.json {
        print_ok(&ResponsePayload::Query(QueryResult::Issue(issue)), true)?;
    } else {
        println!("{}", render::render_updated(&issue.id));
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
    match raw {
        "open" => Ok(WorkflowStatus::Open),
        "in_progress" => Ok(WorkflowStatus::InProgress),
        "closed" => Ok(WorkflowStatus::Closed),
        other => Err(Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "status".into(),
            reason: format!("unknown status {other:?}"),
        })),
    }
}
