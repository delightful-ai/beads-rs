use beads_surface::Filters;
use beads_surface::ipc::{CountPayload, Request};
use clap::Args;

use super::CommandResult;
use super::print_ok;
use crate::filters::{CommonFilterArgs, apply_common_filters};
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::{normalize_bead_id_for, validation_error};

#[derive(Args, Debug)]
pub struct CountArgs {
    #[command(flatten)]
    pub common: CommonFilterArgs,

    /// Filter by title text (case-insensitive substring match).
    #[arg(long)]
    pub title: Option<String>,

    /// Filter by specific issue IDs (comma-separated).
    #[arg(long)]
    pub id: Option<String>,

    /// Group count by status.
    #[arg(long = "by-status")]
    pub by_status: bool,

    /// Group count by priority.
    #[arg(long = "by-priority")]
    pub by_priority: bool,

    /// Group count by issue type.
    #[arg(long = "by-type")]
    pub by_type: bool,

    /// Group count by assignee.
    #[arg(long = "by-assignee")]
    pub by_assignee: bool,

    /// Group count by label.
    #[arg(long = "by-label")]
    pub by_label: bool,
}

pub fn handle(ctx: &CliRuntimeCtx, args: CountArgs) -> CommandResult<()> {
    let mut filters = Filters::default();
    apply_common_filters(&args.common, &mut filters)?;

    if let Some(title) = args.title.clone()
        && !title.trim().is_empty()
    {
        filters.title = Some(title);
    }

    if let Some(ids_raw) = args.id.clone() {
        let mut ids = Vec::new();
        for part in ids_raw.split(',') {
            let s = part.trim();
            if s.is_empty() {
                continue;
            }
            ids.push(normalize_bead_id_for("id", s)?);
        }
        if !ids.is_empty() {
            filters.ids = Some(ids);
        }
    }

    let group_by = resolve_group_by(&args)?;

    let req = Request::Count {
        ctx: ctx.read_ctx(),
        payload: CountPayload { filters, group_by },
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}

pub fn render_count(result: &beads_api::CountResult) -> String {
    match result {
        beads_api::CountResult::Simple { count } => format!("{count}"),
        beads_api::CountResult::Grouped { total, groups } => {
            let mut out = format!("Total: {total}\n\n");
            for group in groups {
                out.push_str(&format!("{}: {}\n", group.group, group.count));
            }
            out.trim_end().into()
        }
    }
}

fn resolve_group_by(args: &CountArgs) -> CommandResult<Option<String>> {
    let mut group_by: Option<&'static str> = None;

    let candidates = [
        (args.by_status, "status"),
        (args.by_priority, "priority"),
        (args.by_type, "type"),
        (args.by_assignee, "assignee"),
        (args.by_label, "label"),
    ];

    for (flag, name) in candidates {
        if !flag {
            continue;
        }
        if group_by.is_some() {
            return Err(validation_error("by-*", "only one --by-* flag can be specified").into());
        }
        group_by = Some(name);
    }

    Ok(group_by.map(|name| name.to_string()))
}
