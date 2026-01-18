use serde::Serialize;

use super::super::render;
use super::super::{Ctx, EpicCmd, print_json, print_ok, send};
use crate::Result;
use crate::api::QueryResult;
use crate::daemon::ipc::{Request, ResponsePayload};

#[derive(Debug, Serialize)]
struct EpicCloseResult {
    closed: Vec<String>,
    count: usize,
}

pub(crate) fn handle(ctx: &Ctx, cmd: EpicCmd) -> Result<()> {
    match cmd {
        EpicCmd::Status(args) => {
            let req = Request::EpicStatus {
                repo: ctx.repo.clone(),
                eligible_only: args.eligible_only,
                read: ctx.read_consistency(),
            };
            let ok = send(&req)?;
            if ctx.json {
                return print_ok(&ok, true);
            }
            match ok {
                ResponsePayload::Query(QueryResult::EpicStatus(statuses)) => {
                    println!("{}", render::render_epic_statuses(&statuses));
                    Ok(())
                }
                other => print_ok(&other, false),
            }
        }
        EpicCmd::CloseEligible(args) => {
            let req = Request::EpicStatus {
                repo: ctx.repo.clone(),
                eligible_only: true,
                read: ctx.read_consistency(),
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
                    println!("[]");
                } else {
                    println!("No epics eligible for closure");
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
                    println!("{}", render::render_epic_close_dry_run(&statuses));
                }
                return Ok(());
            }

            let mut closed = Vec::new();
            for s in &statuses {
                let epic_id = s.epic.id.clone();
                let req = Request::Close {
                    repo: ctx.repo.clone(),
                    id: epic_id.clone(),
                    reason: Some("All children completed".into()),
                    on_branch: None,
                    meta: ctx.mutation_meta(),
                };
                let _ = send(&req)?;
                closed.push(epic_id);
            }

            if ctx.json {
                let out = EpicCloseResult {
                    count: closed.len(),
                    closed,
                };
                print_json(&out)?;
            } else {
                println!("{}", render::render_epic_close_result(&closed));
            }
            Ok(())
        }
    }
}
