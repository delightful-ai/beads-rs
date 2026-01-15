use serde::Serialize;

use super::super::render;
use super::super::{Ctx, LabelBatchArgs, LabelCmd, fetch_issue, normalize_bead_id, send};
use crate::core::BeadId;
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::query::{Filters, QueryResult};
use crate::{Error, Result};

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
                let id_str = id.as_str().to_string();
                let req = Request::AddLabels {
                    repo: ctx.repo.clone(),
                    id: id_str.clone(),
                    labels: vec![label.clone()],
                    meta: ctx.mutation_meta(),
                };
                let _ = send(&req)?;

                results.push(LabelChange {
                    status: "added",
                    issue_id: id_str,
                    label: label.clone(),
                });
            }

            if ctx.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&results)
                        .map_err(crate::daemon::IpcError::from)?
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
                let id_str = id.as_str().to_string();
                let req = Request::RemoveLabels {
                    repo: ctx.repo.clone(),
                    id: id_str.clone(),
                    labels: vec![label.clone()],
                    meta: ctx.mutation_meta(),
                };
                let _ = send(&req)?;

                results.push(LabelChange {
                    status: "removed",
                    issue_id: id_str,
                    label: label.clone(),
                });
            }

            if ctx.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&results)
                        .map_err(crate::daemon::IpcError::from)?
                );
                return Ok(());
            }

            for r in results {
                println!("✓ Removed label '{}' from {}", r.label, r.issue_id);
            }
            Ok(())
        }
        LabelCmd::List { id } => {
            let id = normalize_bead_id(&id)?;
            let issue = fetch_issue(ctx, &id)?;
            if ctx.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&issue.labels)
                        .map_err(crate::daemon::IpcError::from)?
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
                read: ctx.read_consistency(),
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

fn split_label_batch(batch: LabelBatchArgs) -> Result<(Vec<BeadId>, String)> {
    if batch.args.len() < 2 {
        return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "label".into(),
            reason: "expected: <issue-id...> <label>".into(),
        }));
    }
    let mut parts = batch.args;
    let label = parts.pop().ok_or_else(|| {
        Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "label".into(),
            reason: "expected: <issue-id...> <label>".into(),
        })
    })?;
    let mut ids = Vec::with_capacity(parts.len());
    for raw in parts {
        let parsed = BeadId::parse(&raw).map_err(|e| {
            Error::Op(crate::daemon::OpError::ValidationFailed {
                field: "id".into(),
                reason: e.to_string(),
            })
        })?;
        ids.push(parsed);
    }
    Ok((ids, label))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_label_batch_requires_label() {
        let err = split_label_batch(LabelBatchArgs {
            args: vec!["bd-123".into()],
        })
        .unwrap_err();
        match err {
            Error::Op(crate::daemon::OpError::ValidationFailed { field, .. }) => {
                assert_eq!(field, "label");
            }
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    #[test]
    fn split_label_batch_parses_ids_and_label() {
        let (ids, label) = split_label_batch(LabelBatchArgs {
            args: vec!["bd-1".into(), "bd-2".into(), "bug".into()],
        })
        .unwrap();
        assert_eq!(label, "bug");
        let ids = ids
            .into_iter()
            .map(|id| id.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["bd-1".to_string(), "bd-2".to_string()]);
    }
}
