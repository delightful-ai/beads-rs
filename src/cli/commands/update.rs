use crate::{Error, Result};

use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::ops::{BeadPatch, Patch};
use crate::daemon::query::QueryResult;

use super::super::{current_actor_string, fetch_issue, print_ok, send, Ctx, UpdateArgs};
use super::super::render;

pub(crate) fn handle(ctx: &Ctx, args: UpdateArgs) -> Result<()> {
    let mut patch = BeadPatch::default();

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
    if let Some(status) = args.status {
        patch.status = Patch::Set(status);
    }

    // Labels add/remove => fetch current labels, then set full list.
    if !args.add_label.is_empty() || !args.remove_label.is_empty() {
        let issue = fetch_issue(ctx, &args.id)?;
        let mut labels = issue.labels;
        for l in args.add_label {
            if !labels.contains(&l) {
                labels.push(l);
            }
        }
        for l in args.remove_label {
            labels.retain(|x| x != &l);
        }
        patch.labels = Patch::Set(labels);
    }

    if !patch.is_empty() {
        patch.validate()?;
        let req = Request::Update {
            repo: ctx.repo.clone(),
            id: args.id.clone(),
            patch,
            cas: None,
        };
        let _ = send(&req)?;
    }

    // Notes
    if let Some(content) = args.notes {
        let note = Request::AddNote {
            repo: ctx.repo.clone(),
            id: args.id.clone(),
            content,
        };
        let _ = send(&note)?;
    }

    // Assignee compat
    if let Some(assignee) = args.assignee {
        if assignee == "none" || assignee == "-" || assignee == "unassigned" {
            let req = Request::Unclaim {
                repo: ctx.repo.clone(),
                id: args.id.clone(),
            };
            let _ = send(&req)?;
        } else {
            let current = current_actor_string();
            if !assignee.is_empty() && assignee != "me" && assignee != "self" && assignee != current
            {
                return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
                    field: "assignee".into(),
                    reason: "cannot assign other actors; run bd as that actor".into(),
                }));
            }
            let req = Request::Claim {
                repo: ctx.repo.clone(),
                id: args.id.clone(),
                lease_secs: 3600,
            };
            let _ = send(&req)?;
        }
    }

    // Emit updated view / summary.
    let issue = fetch_issue(ctx, &args.id)?;
    if ctx.json {
        print_ok(&ResponsePayload::Query(QueryResult::Issue(issue)), true)?;
    } else {
        println!("{}", render::render_updated(&issue.id));
    }
    Ok(())
}

fn resolve_description(description: Option<String>, body: Option<String>) -> Result<Option<String>> {
    match (description, body) {
        (Some(d), Some(b)) => {
            if d != b {
                return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
                    field: "description".into(),
                    reason: format!(
                        "cannot specify both --description and --body with different values (--description={d:?}, --body={b:?})"
                    ),
                }));
            }
            Ok(Some(d))
        }
        (Some(d), None) => Ok(Some(d)),
        (None, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}
