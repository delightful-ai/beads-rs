use clap::Args;

use super::common::{fetch_issue, normalize_close_reason};
use super::input::{FileTextArg, InlineTextArg, resolve_text_input, text_input_uses_stdin};
use super::{CommandError, CommandResult, print_ok};
use crate::parsers::{parse_bead_type, parse_priority};
use crate::render::print_line;
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::{
    normalize_bead_id, normalize_bead_id_for, normalize_dep_specs, validation_error,
};
use beads_api::QueryResult;
use beads_core::{BeadType, DepKind, IssueStatus, Priority};
use beads_surface::ipc::{
    AddNotePayload, ClaimPayload, ClosePayload, DepPayload, IdPayload, LabelsPayload,
    ParentPayload, Request, ResponsePayload, UpdatePayload,
};
use beads_surface::ops::{BeadPatch, BeadPatchValidationError, Patch};

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

    /// Replace the description/body text.
    #[arg(short = 'd', long, alias = "desc", allow_hyphen_values = true)]
    pub description: Option<String>,

    /// Alias for --description (GitHub CLI convention).
    #[arg(long = "body", hide = true, allow_hyphen_values = true)]
    pub body: Option<String>,

    /// Alias for --description (git commit convention).
    #[arg(short = 'm', long = "message", hide = true, allow_hyphen_values = true)]
    pub message: Option<String>,

    /// Read description from file (use `-` for stdin).
    #[arg(long = "body-file", value_name = "PATH")]
    pub body_file: Option<std::path::PathBuf>,

    /// Alias for --body-file.
    #[arg(long = "description-file", hide = true, value_name = "PATH")]
    pub description_file: Option<std::path::PathBuf>,

    /// Read description from stdin.
    #[arg(long)]
    pub stdin: bool,

    /// Replace the design section.
    #[arg(long, allow_hyphen_values = true)]
    pub design: Option<String>,

    /// Read design text from file (use `-` for stdin).
    #[arg(long = "design-file", value_name = "PATH")]
    pub design_file: Option<std::path::PathBuf>,

    /// Replace the acceptance criteria section.
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

    /// Close reason / terminal state (done, cancelled, duplicate).
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

pub fn handle(ctx: &CliRuntimeCtx, mut args: UpdateArgs) -> CommandResult<()> {
    let id = normalize_bead_id(&args.id)?;
    let id_str = id.as_str().to_string();
    let mut patch = BeadPatch::default();
    let close_reason = normalize_close_reason(args.reason)?;
    let status = match args.status.as_deref() {
        Some(raw) => Some(parse_status(raw)?),
        None => None,
    };
    let status_closed = status.is_some_and(IssueStatus::is_terminal);
    let add_labels = std::mem::take(&mut args.add_label);
    let remove_labels = std::mem::take(&mut args.remove_label);

    if close_reason.is_some() && !status_closed {
        return Err(validation_error("reason", "--reason requires a terminal --status").into());
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
            Some(Some(normalize_bead_id_for("parent", v)?))
        }
    } else {
        None
    };

    if let Some(title) = args.title {
        patch.title = Patch::Set(title);
    }
    let description_sources_inline = [
        InlineTextArg {
            flag: "--description",
            value: args.description.as_deref(),
        },
        InlineTextArg {
            flag: "--body",
            value: args.body.as_deref(),
        },
        InlineTextArg {
            flag: "--message",
            value: args.message.as_deref(),
        },
    ];
    let description_sources_files = [
        FileTextArg {
            flag: "--body-file",
            path: args.body_file.as_deref(),
        },
        FileTextArg {
            flag: "--description-file",
            path: args.description_file.as_deref(),
        },
    ];
    let design_sources_inline = [InlineTextArg {
        flag: "--design",
        value: args.design.as_deref(),
    }];
    let design_sources_files = [FileTextArg {
        flag: "--design-file",
        path: args.design_file.as_deref(),
    }];
    if text_input_uses_stdin(
        &description_sources_inline,
        &description_sources_files,
        args.stdin,
        true,
    ) && text_input_uses_stdin(&design_sources_inline, &design_sources_files, false, false)
    {
        return Err(validation_error(
            "stdin",
            "description and design cannot both read from stdin in the same invocation; use a file for one of them",
        )
        .into());
    }
    if let Some(desc) = resolve_text_input(
        "description",
        &description_sources_inline,
        &description_sources_files,
        args.stdin,
        true,
    )? {
        patch.description = Patch::Set(desc);
    }
    if let Some(design) = resolve_text_input(
        "design",
        &design_sources_inline,
        &design_sources_files,
        false,
        false,
    )? {
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
    if let Some(status) = status
        && !status.is_terminal()
    {
        patch.status = Patch::Set(status);
    }

    if !patch.is_empty() {
        patch
            .validate_for_update()
            .map_err(map_patch_validation_error)?;
        let req = Request::Update {
            ctx: ctx.mutation_ctx(),
            payload: UpdatePayload {
                id: id.clone(),
                patch,
                cas: None,
            },
        };
        let _ = send(&req)?;
    }

    if !add_labels.is_empty() {
        let req = Request::AddLabels {
            ctx: ctx.mutation_ctx(),
            payload: LabelsPayload {
                id: id.clone(),
                labels: add_labels,
            },
        };
        let _ = send(&req)?;
    }

    if !remove_labels.is_empty() {
        let req = Request::RemoveLabels {
            ctx: ctx.mutation_ctx(),
            payload: LabelsPayload {
                id: id.clone(),
                labels: remove_labels,
            },
        };
        let _ = send(&req)?;
    }

    // Parent relationship (child -> parent edge).
    if let Some(new_parent) = parent_action {
        let req = Request::SetParent {
            ctx: ctx.mutation_ctx(),
            payload: ParentPayload {
                id: id.clone(),
                parent: new_parent,
            },
        };
        let _ = send(&req)?;
    }

    // Add dependencies
    if !args.deps.is_empty() {
        let dep_specs = normalize_dep_specs(args.deps)?;
        for spec in dep_specs {
            let (kind, to_raw) = if let Some((k, i)) = spec.split_once(':') {
                (DepKind::parse(k).unwrap_or(DepKind::Blocks), i)
            } else {
                (DepKind::Blocks, spec.as_str())
            };
            let to = normalize_bead_id_for("deps", to_raw)?;
            let _ = send(&Request::AddDep {
                ctx: ctx.mutation_ctx(),
                payload: DepPayload {
                    from: id.clone(),
                    to,
                    kind,
                },
            })?;
        }
    }

    // Notes
    if let Some(content) = args.notes {
        let note = Request::AddNote {
            ctx: ctx.mutation_ctx(),
            payload: AddNotePayload {
                id: id.clone(),
                content,
            },
        };
        let _ = send(&note)?;
    }

    // Assignee compat
    if let Some(assignee) = args.assignee {
        if assignee == "none" || assignee == "-" || assignee == "unassigned" {
            let req = Request::Unclaim {
                ctx: ctx.mutation_ctx(),
                payload: IdPayload { id: id.clone() },
            };
            let _ = send(&req)?;
        } else {
            let current = ctx.actor_string()?;
            if !assignee.is_empty() && assignee != "me" && assignee != "self" && assignee != current
            {
                return Err(validation_error(
                    "assignee",
                    "cannot assign other actors; run bd as that actor",
                )
                .into());
            }
            let req = Request::Claim {
                ctx: ctx.mutation_ctx(),
                payload: ClaimPayload {
                    id: id.clone(),
                    lease_secs: 3600,
                },
            };
            let _ = send(&req)?;
        }
    }

    if status_closed {
        let close_reason = close_reason
            .or_else(|| status.and_then(|status| status.closed_reason().map(str::to_string)));
        if let (Some(status), Some(reason)) = (status, close_reason.as_deref())
            && status.is_terminal()
            && status.closed_reason() != Some(reason)
        {
            return Err(validation_error(
                "reason",
                format!(
                    "--reason {reason:?} conflicts with terminal --status {}",
                    status.as_str()
                ),
            )
            .into());
        }
        let req = Request::Close {
            ctx: ctx.mutation_ctx(),
            payload: ClosePayload {
                id: id.clone(),
                reason: close_reason,
                on_branch: None,
            },
        };
        let _ = send(&req)?;
    }

    // Emit updated view / summary.
    if ctx.json {
        let issue = fetch_issue(ctx, &id)?;
        print_ok(&ResponsePayload::Query(QueryResult::Issue(issue)), true)?;
    } else {
        print_line(&render_updated(&id_str))?;
    }
    Ok(())
}

pub fn render_updated(id: &str) -> String {
    format!("✓ Updated issue: {id}")
}
fn map_patch_validation_error(err: BeadPatchValidationError) -> CommandError {
    match err {
        BeadPatchValidationError::RequiredFieldCleared { field } => {
            validation_error(field.as_str(), "cannot clear required field").into()
        }
        BeadPatchValidationError::RequiredFieldEmpty { field } => {
            validation_error(field.as_str(), "cannot set required field to empty").into()
        }
    }
}

fn parse_status(raw: &str) -> CommandResult<IssueStatus> {
    IssueStatus::parse(raw)
        .ok_or_else(|| validation_error("status", format!("unknown status {raw:?}")))
        .map_err(Into::into)
}

fn parse_status_arg(raw: &str) -> std::result::Result<String, String> {
    crate::parsers::parse_status(raw)
        .map(|status| status.as_str().to_string())
        .map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::CommandError;
    use beads_core::NamespaceId;

    fn sample_ctx() -> CliRuntimeCtx {
        CliRuntimeCtx {
            repo: std::path::PathBuf::from("/tmp/beads"),
            json: false,
            namespace: Some(NamespaceId::core()),
            durability: None,
            client_request_id: None,
            require_min_seen: None,
            wait_timeout_ms: None,
            actor_id: None,
        }
    }

    #[test]
    fn update_rejects_double_stdin_usage() {
        let err = handle(
            &sample_ctx(),
            UpdateArgs {
                id: "beads-rs-k8u3".to_string(),
                parent: None,
                no_parent: false,
                title: None,
                description: None,
                body: None,
                message: None,
                body_file: None,
                description_file: None,
                stdin: true,
                design: None,
                design_file: Some(std::path::PathBuf::from("-")),
                acceptance: None,
                external_ref: None,
                estimate: None,
                status: None,
                reason: None,
                priority: None,
                bead_type: None,
                assignee: None,
                add_label: Vec::new(),
                remove_label: Vec::new(),
                notes: None,
                deps: Vec::new(),
            },
        )
        .expect_err("double stdin usage must fail");

        assert!(matches!(err, CommandError::Validation(_)));
        assert!(
            err.to_string()
                .contains("description and design cannot both read from stdin")
        );
    }

    #[test]
    fn update_rejects_invalid_close_reason() {
        let err = handle(
            &sample_ctx(),
            UpdateArgs {
                id: "beads-rs-k8u3".to_string(),
                parent: None,
                no_parent: false,
                title: None,
                description: None,
                body: None,
                message: None,
                body_file: None,
                description_file: None,
                stdin: false,
                design: None,
                design_file: None,
                acceptance: None,
                external_ref: None,
                estimate: None,
                status: Some("done".to_string()),
                reason: Some("fixed it".to_string()),
                priority: None,
                bead_type: None,
                assignee: None,
                add_label: Vec::new(),
                remove_label: Vec::new(),
                notes: None,
                deps: Vec::new(),
            },
        )
        .expect_err("invalid close reason must fail");

        assert!(matches!(err, CommandError::Validation(_)));
        assert!(err.to_string().contains("valid close reasons"));
    }
}
