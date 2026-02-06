use clap::{Args, Subcommand};
use serde::Serialize;

use super::super::validation::normalize_bead_id;
use super::super::{Ctx, print_json, print_line, print_ok, send};
use super::fmt_issue_ref;
use crate::Result;
use crate::api::QueryResult;
use crate::daemon::ipc::{ClosePayload, EpicStatusPayload, Request, ResponsePayload};

#[derive(Subcommand, Debug)]
pub enum EpicCmd {
    /// Show epic completion status.
    Status(EpicStatusArgs),
    /// Close epics where all children are complete.
    #[command(name = "close-eligible")]
    CloseEligible(EpicCloseEligibleArgs),
}

#[derive(Args, Debug)]
pub struct EpicStatusArgs {
    /// Show only epics eligible for closure.
    #[arg(long)]
    pub eligible_only: bool,
}

#[derive(Args, Debug)]
pub struct EpicCloseEligibleArgs {
    /// Preview what would be closed without writing.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Serialize)]
struct EpicCloseResult {
    closed: Vec<String>,
    count: usize,
}

pub(crate) fn handle(ctx: &Ctx, cmd: EpicCmd) -> Result<()> {
    match cmd {
        EpicCmd::Status(args) => {
            let req = Request::EpicStatus {
                ctx: ctx.read_ctx(),
                payload: EpicStatusPayload {
                    eligible_only: args.eligible_only,
                },
            };
            let ok = send(&req)?;
            if ctx.json {
                return print_ok(&ok, true);
            }
            match ok {
                ResponsePayload::Query(QueryResult::EpicStatus(statuses)) => {
                    print_line(&render_epic_statuses(&statuses))
                }
                other => print_ok(&other, false),
            }
        }
        EpicCmd::CloseEligible(args) => {
            let req = Request::EpicStatus {
                ctx: ctx.read_ctx(),
                payload: EpicStatusPayload {
                    eligible_only: true,
                },
            };
            let ok = send(&req)?;
            let statuses = match ok {
                ResponsePayload::Query(QueryResult::EpicStatus(statuses)) => statuses,
                other => {
                    if ctx.json {
                        print_ok(&other, true)?;
                    } else {
                        print_ok(&other, false)?;
                    }
                    return Ok(());
                }
            };

            if statuses.is_empty() {
                if ctx.json {
                    print_line("[]")?;
                } else {
                    print_line("No epics eligible for closure")?;
                }
                return Ok(());
            }

            if args.dry_run {
                if ctx.json {
                    // Match go UX: dry-run prints the eligible list.
                    print_ok(
                        &ResponsePayload::Query(QueryResult::EpicStatus(statuses)),
                        true,
                    )?;
                } else {
                    print_line(&render_epic_close_dry_run(&statuses))?;
                }
                return Ok(());
            }

            let mut closed = Vec::new();
            for s in &statuses {
                let epic_id = normalize_bead_id(&s.epic.id)?;
                let req = Request::Close {
                    ctx: ctx.mutation_ctx(),
                    payload: ClosePayload {
                        id: epic_id.clone(),
                        reason: Some("All children completed".into()),
                        on_branch: None,
                    },
                };
                let _ = send(&req)?;
                closed.push(epic_id.as_str().to_string());
            }

            if ctx.json {
                let out = EpicCloseResult {
                    count: closed.len(),
                    closed,
                };
                print_json(&out)?;
            } else {
                print_line(&render_epic_close_result(&closed))?;
            }
            Ok(())
        }
    }
}

pub(crate) fn render_epic_statuses(statuses: &[crate::api::EpicStatus]) -> String {
    if statuses.is_empty() {
        return "No open epics found".into();
    }

    let mut out = String::new();
    for s in statuses {
        let pct = s
            .closed_children
            .saturating_mul(100)
            .checked_div(s.total_children)
            .unwrap_or(0);
        let icon = if s.eligible_for_close { "✓" } else { "○" };
        out.push_str(&format!(
            "{icon} {} {}\n",
            fmt_issue_ref(&s.epic.namespace, &s.epic.id),
            s.epic.title
        ));
        out.push_str(&format!(
            "   Progress: {}/{} children closed ({}%)\n",
            s.closed_children, s.total_children, pct
        ));
        if s.eligible_for_close {
            out.push_str("   Eligible for closure\n");
        }
        out.push('\n');
    }

    out.trim_end().into()
}

pub(crate) fn render_epic_close_dry_run(statuses: &[crate::api::EpicStatus]) -> String {
    let mut out = format!("Would close {} epic(s):\n", statuses.len());
    for s in statuses {
        out.push_str(&format!(
            "  - {}: {}\n",
            fmt_issue_ref(&s.epic.namespace, &s.epic.id),
            s.epic.title
        ));
    }
    out
}

pub(crate) fn render_epic_close_result(closed: &[String]) -> String {
    let mut out = format!("✓ Closed {} epic(s)\n", closed.len());
    for id in closed {
        out.push_str(&format!("  - {id}\n"));
    }
    out.trim_end().into()
}
