use clap::Args;

use super::super::{
    CommonFilterArgs, Ctx, normalize_bead_id_for, print_ok, send, validation_error,
};
use crate::Result;
use crate::daemon::ipc::{CountPayload, Request};
use crate::daemon::query::Filters;

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

pub(crate) fn handle(ctx: &Ctx, args: CountArgs) -> Result<()> {
    let mut filters = Filters::default();
    args.common.apply(&mut filters)?;

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

pub(crate) fn render_count(result: &crate::api::CountResult) -> String {
    match result {
        crate::api::CountResult::Simple { count } => format!("{count}"),
        crate::api::CountResult::Grouped { total, groups } => {
            let mut out = format!("Total: {total}\n\n");
            for g in groups {
                out.push_str(&format!("{}: {}\n", g.group, g.count));
            }
            out.trim_end().into()
        }
    }
}

fn resolve_group_by(args: &CountArgs) -> Result<Option<String>> {
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
            return Err(validation_error(
                "by-*",
                "only one --by-* flag can be specified",
            ));
        }
        group_by = Some(name);
    }

    Ok(group_by.map(|s| s.to_string()))
}
