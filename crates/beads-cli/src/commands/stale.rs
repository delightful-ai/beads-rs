use beads_api::QueryResult;
use beads_surface::ipc::{Request, ResponsePayload, StalePayload};
use clap::Args;

use super::common::fmt_issue_ref;
use super::{CommandResult, print_ok};
use crate::runtime::{CliRuntimeCtx, send};

#[derive(Args, Debug)]
pub struct StaleArgs {
    /// Issues not updated in this many days.
    #[arg(short = 'd', long, default_value_t = 30)]
    pub days: u32,

    /// Filter by status (open|in_progress|blocked).
    #[arg(short = 's', long)]
    pub status: Option<String>,

    /// Maximum issues to show.
    #[arg(short = 'n', long, default_value_t = 50)]
    pub limit: usize,
}

pub fn handle(ctx: &CliRuntimeCtx, args: StaleArgs) -> CommandResult<()> {
    let req = Request::Stale {
        ctx: ctx.read_ctx(),
        payload: StalePayload {
            days: args.days,
            status: args.status.clone(),
            limit: Some(args.limit),
        },
    };
    let ok = send(&req)?;

    if ctx.json {
        return print_ok(&ok, true);
    }

    if let ResponsePayload::Query(QueryResult::Stale(issues)) = ok {
        crate::render::print_line(&render_stale(&issues, args.days))?;
    } else {
        print_ok(&ok, false)?;
    }

    Ok(())
}

pub fn render_stale(issues: &[beads_api::IssueSummary], threshold_days: u32) -> String {
    if issues.is_empty() {
        return "\n✨ No stale issues found (all active)\n".into();
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let mut out = format!(
        "\n⏰ Stale issues ({} not updated in {}+ days):\n\n",
        issues.len(),
        threshold_days
    );

    for (i, issue) in issues.iter().enumerate() {
        let updated_ms = issue.updated_at.wall_ms;
        let days_stale = now_ms
            .saturating_sub(updated_ms)
            .saturating_div(24 * 60 * 60 * 1000);

        out.push_str(&format!(
            "{}. [P{}] {}: {}\n",
            i + 1,
            issue.priority,
            fmt_issue_ref(&issue.namespace, &issue.id),
            issue.title
        ));
        out.push_str(&format!(
            "   Status: {}, Last updated: {} days ago\n",
            issue.status.as_str(),
            days_stale
        ));
        if let Some(assignee) = &issue.assignee
            && !assignee.is_empty()
        {
            out.push_str(&format!("   Assignee: {}\n", assignee));
        }
        out.push('\n');
    }

    out.trim_end().into()
}
