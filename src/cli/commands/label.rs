use crate::{Error, Result};

use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::ops::{BeadPatch, Patch};
use crate::daemon::query::{Filters, QueryResult};
use serde::Serialize;

use super::super::{fetch_issue, send, Ctx, LabelBatchArgs, LabelCmd};
use super::super::render;

#[derive(Debug, Clone, Serialize)]
struct LabelChange {
    status: &'static str,
    issue_id: String,
    label: String,
}

#[derive(Debug, Clone, Serialize)]
struct LabelCount {
    label: String,
    count: usize,
}

pub(crate) fn handle(ctx: &Ctx, cmd: LabelCmd) -> Result<()> {
    match cmd {
        LabelCmd::Add(batch) => {
            let (ids, label) = split_label_batch(batch)?;
            let mut results: Vec<LabelChange> = Vec::new();
            for id in ids {
                let issue = fetch_issue(ctx, &id)?;
                let mut labels = issue.labels;
                if !labels.iter().any(|l| l == &label) {
                    labels.push(label.clone());
                }
                let mut patch = BeadPatch::default();
                patch.labels = Patch::Set(labels);
                let req = Request::Update {
                    repo: ctx.repo.clone(),
                    id: id.clone(),
                    patch,
                    cas: None,
                };
                let _ = send(&req)?;

                results.push(LabelChange {
                    status: "added",
                    issue_id: id,
                    label: label.clone(),
                });
            }

            if ctx.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&results).map_err(crate::daemon::IpcError::from)?
                );
                return Ok(());
            }

            for r in results {
                println!("✓ Added label '{}' to {}", r.label, r.issue_id);
            }
            Ok(())
        }
        LabelCmd::Remove(batch) => {
            let (ids, label) = split_label_batch(batch)?;
            let mut results: Vec<LabelChange> = Vec::new();
            for id in ids {
                let issue = fetch_issue(ctx, &id)?;
                let mut labels = issue.labels;
                labels.retain(|l| l != &label);
                let mut patch = BeadPatch::default();
                patch.labels = Patch::Set(labels);
                let req = Request::Update {
                    repo: ctx.repo.clone(),
                    id: id.clone(),
                    patch,
                    cas: None,
                };
                let _ = send(&req)?;

                results.push(LabelChange {
                    status: "removed",
                    issue_id: id,
                    label: label.clone(),
                });
            }

            if ctx.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&results).map_err(crate::daemon::IpcError::from)?
                );
                return Ok(());
            }

            for r in results {
                println!("✓ Removed label '{}' from {}", r.label, r.issue_id);
            }
            Ok(())
        }
        LabelCmd::List { id } => {
            let issue = fetch_issue(ctx, &id)?;
            if ctx.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&issue.labels).map_err(crate::daemon::IpcError::from)?
                );
                return Ok(());
            }
            println!("{}", render::render_label_list(&issue.id, &issue.labels));
            Ok(())
        }
        LabelCmd::ListAll => {
            let req = Request::List {
                repo: ctx.repo.clone(),
                filters: Filters::default(),
            };
            let ok = send(&req)?;
            let mut counts = std::collections::BTreeMap::<String, usize>::new();
            if let ResponsePayload::Query(QueryResult::Issues(views)) = ok {
                for v in views {
                    for l in v.labels {
                        *counts.entry(l).or_insert(0) += 1;
                    }
                }
            }

            if ctx.json {
                let out: Vec<LabelCount> = counts
                    .iter()
                    .map(|(label, count)| LabelCount {
                        label: label.clone(),
                        count: *count,
                    })
                    .collect();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&out).map_err(crate::daemon::IpcError::from)?
                );
                return Ok(());
            }
            println!("{}", render::render_label_list_all(&counts));
            Ok(())
        }
    }
}

fn split_label_batch(batch: LabelBatchArgs) -> Result<(Vec<String>, String)> {
    if batch.args.len() < 2 {
        return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "label".into(),
            reason: "expected: <issue-id...> <label>".into(),
        }));
    }
    let mut parts = batch.args;
    let label = parts.pop().unwrap();
    let ids = parts;
    Ok((ids, label))
}
