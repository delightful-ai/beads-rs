use clap::Args;

use super::super::{Ctx, print_ok, send};
use super::fmt_issue_ref;
use crate::Result;
use crate::api::QueryResult;
use crate::daemon::ipc::{Request, ResponsePayload};

#[derive(Args, Debug)]
pub struct ReadyArgs {
    /// Limit results.
    #[arg(short = 'n', long)]
    pub limit: Option<usize>,
}

pub(crate) fn handle(ctx: &Ctx, args: ReadyArgs) -> Result<()> {
    let req = Request::Ready {
        repo: ctx.repo.clone(),
        limit: args.limit,
        read: ctx.read_consistency(),
    };
    let ok = send(&req)?;
    if ctx.json {
        return print_ok(&ok, true);
    }
    match ok {
        ResponsePayload::Query(QueryResult::Ready(result)) => {
            println!(
                "{}",
                render_ready(&result.issues, result.blocked_count, result.closed_count)
            );
            Ok(())
        }
        other => print_ok(&other, false),
    }
}

pub(crate) fn render_ready(
    views: &[crate::api::IssueSummary],
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
        for (i, v) in views.iter().enumerate() {
            out.push_str(&format!(
                "{}. [P{}] {}: {}\n",
                i + 1,
                v.priority,
                fmt_issue_ref(&v.namespace, &v.id),
                v.title
            ));
            if let Some(m) = v.estimated_minutes {
                out.push_str(&format!("   Estimate: {} min\n", m));
            }
            if let Some(a) = &v.assignee
                && !a.is_empty()
            {
                out.push_str(&format!("   Assignee: {}\n", a));
            }
        }
        out.push('\n');
    }
    // Always show summary footer so agents understand context.
    out.push_str(&format!(
        "{} blocked, {} closed — run `bd blocked` to see what's stuck\n",
        blocked_count, closed_count
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{NamespaceId, WriteStamp};

    fn sample_summary(namespace: &str, id: &str) -> crate::api::IssueSummary {
        crate::api::IssueSummary {
            id: id.to_string(),
            namespace: NamespaceId::parse(namespace).expect("namespace"),
            title: "Title".to_string(),
            description: String::new(),
            design: None,
            acceptance_criteria: None,
            status: "open".to_string(),
            priority: 1,
            issue_type: "task".to_string(),
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
