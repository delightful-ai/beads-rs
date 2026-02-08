use beads_api::QueryResult;
use beads_surface::ipc::{ReadyPayload, Request, ResponsePayload};
use clap::Args;

use super::common::fmt_issue_ref;
use super::{CommandResult, print_ok};
use crate::runtime::{CliRuntimeCtx, send};

#[derive(Args, Debug)]
pub struct ReadyArgs {
    /// Limit results.
    #[arg(short = 'n', long)]
    pub limit: Option<usize>,
}

pub fn handle(ctx: &CliRuntimeCtx, args: ReadyArgs) -> CommandResult<()> {
    let req = Request::Ready {
        ctx: ctx.read_ctx(),
        payload: ReadyPayload { limit: args.limit },
    };
    let ok = send(&req)?;

    if ctx.json {
        return print_ok(&ok, true);
    }

    if let ResponsePayload::Query(QueryResult::Ready(result)) = ok {
        crate::render::print_line(&render_ready(
            &result.issues,
            result.blocked_count,
            result.closed_count,
        ))?;
    } else {
        print_ok(&ok, false)?;
    }

    Ok(())
}

pub fn render_ready(
    views: &[beads_api::IssueSummary],
    blocked_count: usize,
    closed_count: usize,
) -> String {
    let mut out = String::new();
    if views.is_empty() {
        out.push_str("\n✨ No ready work found\n");
    } else {
        out.push_str(&format!(
            "\n📋 Ready work ({} issues with no blockers):\n\n",
            views.len()
        ));
        for (i, view) in views.iter().enumerate() {
            out.push_str(&format!(
                "{}. [P{}] {}: {}\n",
                i + 1,
                view.priority,
                fmt_issue_ref(&view.namespace, &view.id),
                view.title
            ));
            if let Some(minutes) = view.estimated_minutes {
                out.push_str(&format!("   Estimate: {} min\n", minutes));
            }
            if let Some(assignee) = &view.assignee
                && !assignee.is_empty()
            {
                out.push_str(&format!("   Assignee: {}\n", assignee));
            }
        }
        out.push('\n');
    }

    out.push_str(&format!(
        "{} blocked, {} closed — run `bd blocked` to see what's stuck\n",
        blocked_count, closed_count
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use beads_core::{BeadType, NamespaceId, WorkflowStatus, WriteStamp};

    fn sample_summary(namespace: &str, id: &str) -> beads_api::IssueSummary {
        beads_api::IssueSummary {
            id: id.to_string(),
            namespace: NamespaceId::parse(namespace).expect("namespace"),
            title: "Title".to_string(),
            description: String::new(),
            design: None,
            acceptance_criteria: None,
            status: WorkflowStatus::Open,
            priority: 1,
            issue_type: BeadType::Task,
            labels: Vec::new(),
            assignee: None,
            assignee_expires: None,
            created_at: WriteStamp::new(0, 0),
            created_by: "tester".to_string(),
            updated_at: WriteStamp::new(0, 0),
            updated_by: "tester".to_string(),
            estimated_minutes: None,
            content_hash: "hash".to_string(),
            note_count: 0,
        }
    }

    #[test]
    fn render_ready_includes_namespace() {
        let summary = sample_summary("wf", "bd-123");
        let output = render_ready(&[summary], 0, 0);
        let expected = concat!(
            "\n📋 Ready work (1 issues with no blockers):\n\n",
            "1. [P1] wf/bd-123: Title\n",
            "\n",
            "0 blocked, 0 closed — run `bd blocked` to see what's stuck\n",
        );
        assert_eq!(output, expected);
    }
}
