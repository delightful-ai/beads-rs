use clap::{Args, Subcommand};
use serde::Serialize;

use super::common::fmt_issue_ref;
use super::{CommandResult, print_ok};
use crate::render::{print_json, print_line};
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::normalize_bead_id;
use beads_api::{EpicStatus, QueryResult};
use beads_surface::ipc::{ClosePayload, EpicStatusPayload, Request, ResponsePayload};

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

pub fn handle(ctx: &CliRuntimeCtx, cmd: EpicCmd) -> CommandResult<()> {
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
                    print_line(&render_epic_statuses(&statuses))?;
                    Ok(())
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
            for status in &statuses {
                let epic_id = normalize_bead_id(&status.epic.id)?;
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

pub fn render_epic_statuses(statuses: &[EpicStatus]) -> String {
    if statuses.is_empty() {
        return "No open epics found".into();
    }

    let mut out = String::new();
    for status in statuses {
        let pct = status
            .closed_children
            .saturating_mul(100)
            .checked_div(status.total_children)
            .unwrap_or(0);
        let icon = if status.eligible_for_close {
            "✓"
        } else {
            "○"
        };
        out.push_str(&format!(
            "{icon} {} {}\n",
            fmt_issue_ref(&status.epic.namespace, &status.epic.id),
            status.epic.title
        ));
        out.push_str(&format!(
            "   Progress: {}/{} children closed ({}%)\n",
            status.closed_children, status.total_children, pct
        ));
        if status.eligible_for_close {
            out.push_str("   Eligible for closure\n");
        }
        out.push('\n');
    }

    out.trim_end().into()
}

pub fn render_epic_close_dry_run(statuses: &[EpicStatus]) -> String {
    let mut out = format!("Would close {} epic(s):\n", statuses.len());
    for status in statuses {
        out.push_str(&format!(
            "  - {}: {}\n",
            fmt_issue_ref(&status.epic.namespace, &status.epic.id),
            status.epic.title
        ));
    }
    out
}

pub fn render_epic_close_result(closed: &[String]) -> String {
    let mut out = format!("✓ Closed {} epic(s)\n", closed.len());
    for id in closed {
        out.push_str(&format!("  - {id}\n"));
    }
    out.trim_end().into()
}
