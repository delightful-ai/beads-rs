use clap::{Args, Subcommand};
use serde::Serialize;

use super::CommandResult;
use super::common::fetch_issue;
use crate::render::{print_json, print_line};
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::{normalize_bead_id, validation_error};
use beads_api::QueryResult;
use beads_core::BeadId;
use beads_surface::Filters;
use beads_surface::ipc::{LabelsPayload, ListPayload, Request, ResponsePayload};

#[derive(Subcommand, Debug)]
pub enum LabelCmd {
    /// Add a label to one or more issues.
    Add(LabelBatchArgs),
    /// Remove a label from one or more issues.
    Remove(LabelBatchArgs),
    /// List labels for an issue.
    List { id: String },
    /// List all labels in the repo.
    #[command(name = "list-all")]
    ListAll,
}

#[derive(Args, Debug)]
pub struct LabelBatchArgs {
    /// Arguments in the form: `<issue-id...> <label>` (label is last).
    #[arg(trailing_var_arg = true, num_args = 2..)]
    pub args: Vec<String>,
}

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

pub fn handle(ctx: &CliRuntimeCtx, cmd: LabelCmd) -> CommandResult<()> {
    match cmd {
        LabelCmd::Add(batch) => {
            let (ids, label) = split_label_batch(batch)?;
            let mut results: Vec<LabelChange> = Vec::new();
            for id in ids {
                let id_str = id.as_str().to_string();
                let req = Request::AddLabels {
                    ctx: ctx.mutation_ctx(),
                    payload: LabelsPayload {
                        id: id.clone(),
                        labels: vec![label.clone()],
                    },
                };
                let _ = send(&req)?;

                results.push(LabelChange {
                    status: "added",
                    issue_id: id_str,
                    label: label.clone(),
                });
            }

            if ctx.json {
                print_json(&results)?;
                return Ok(());
            }

            for result in results {
                print_line(&render_label_added(&result.label, &result.issue_id))?;
            }
            Ok(())
        }
        LabelCmd::Remove(batch) => {
            let (ids, label) = split_label_batch(batch)?;
            let mut results: Vec<LabelChange> = Vec::new();
            for id in ids {
                let id_str = id.as_str().to_string();
                let req = Request::RemoveLabels {
                    ctx: ctx.mutation_ctx(),
                    payload: LabelsPayload {
                        id: id.clone(),
                        labels: vec![label.clone()],
                    },
                };
                let _ = send(&req)?;

                results.push(LabelChange {
                    status: "removed",
                    issue_id: id_str,
                    label: label.clone(),
                });
            }

            if ctx.json {
                print_json(&results)?;
                return Ok(());
            }

            for result in results {
                print_line(&render_label_removed(&result.label, &result.issue_id))?;
            }
            Ok(())
        }
        LabelCmd::List { id } => {
            let id = normalize_bead_id(&id)?;
            let issue = fetch_issue(ctx, &id)?;
            if ctx.json {
                print_json(&issue.labels)?;
                return Ok(());
            }
            print_line(&render_label_list(&issue.id, &issue.labels))?;
            Ok(())
        }
        LabelCmd::ListAll => {
            let req = Request::List {
                ctx: ctx.read_ctx(),
                payload: ListPayload {
                    filters: Filters::default(),
                },
            };
            let ok = send(&req)?;
            let mut counts = std::collections::BTreeMap::<String, usize>::new();
            if let ResponsePayload::Query(QueryResult::Issues(views)) = ok {
                for view in views {
                    for label in view.labels {
                        *counts.entry(label).or_insert(0) += 1;
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
                print_json(&out)?;
                return Ok(());
            }
            print_line(&render_label_list_all(&counts))?;
            Ok(())
        }
    }
}

pub fn render_label_list(issue_id: &str, labels: &[String]) -> String {
    if labels.is_empty() {
        return format!("\n{issue_id} has no labels\n");
    }
    let mut out = format!("\n🏷 Labels for {issue_id}:\n");
    for label in labels {
        out.push_str(&format!("  - {label}\n"));
    }
    out.push('\n');
    out
}

pub fn render_label_list_all(counts: &std::collections::BTreeMap<String, usize>) -> String {
    if counts.is_empty() {
        return "\nNo labels found in database".into();
    }

    let max_len = counts.keys().map(|s| s.len()).max().unwrap_or(0);
    let mut out = format!("\n🏷 All labels ({} unique):\n", counts.len());
    for (label, count) in counts {
        let padding = " ".repeat(max_len.saturating_sub(label.len()));
        out.push_str(&format!("  {label}{padding}  ({count} issues)\n"));
    }
    out.push('\n');
    out
}

pub fn render_label_added(label: &str, issue_id: &str) -> String {
    format!("✓ Added label '{label}' to {issue_id}")
}

pub fn render_label_removed(label: &str, issue_id: &str) -> String {
    format!("✓ Removed label '{label}' from {issue_id}")
}

fn split_label_batch(batch: LabelBatchArgs) -> CommandResult<(Vec<BeadId>, String)> {
    if batch.args.len() < 2 {
        return Err(validation_error("label", "expected: <issue-id...> <label>").into());
    }
    let mut parts = batch.args;
    let label = parts
        .pop()
        .ok_or_else(|| validation_error("label", "expected: <issue-id...> <label>"))?;
    let mut ids = Vec::with_capacity(parts.len());
    for raw in parts {
        let parsed = BeadId::parse(&raw).map_err(|err| validation_error("id", err.to_string()))?;
        ids.push(parsed);
    }
    Ok((ids, label))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::ValidationError;

    #[test]
    fn split_label_batch_requires_label() {
        let err = split_label_batch(LabelBatchArgs {
            args: vec!["bd-123".into()],
        })
        .expect_err("validation error");
        match err {
            super::super::CommandError::Validation(ValidationError::Field { field, .. }) => {
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
        .expect("split");
        assert_eq!(label, "bug");
        let ids = ids
            .into_iter()
            .map(|id| id.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["bd-1".to_string(), "bd-2".to_string()]);
    }
}
