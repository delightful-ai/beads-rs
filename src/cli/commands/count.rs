use clap::Args;

use super::super::{Ctx, apply_common_filters, normalize_bead_id_for, print_ok, send};
use crate::cli::parse::{parse_bead_type, parse_priority, parse_time_ms_opt};
use crate::core::{BeadType, Priority};
use crate::daemon::ipc::Request;
use crate::daemon::query::Filters;
use crate::{Error, Result};

#[derive(Args, Debug)]
pub struct CountArgs {
    /// Filter by status (open, in_progress, blocked, closed).
    #[arg(short = 's', long)]
    pub status: Option<String>,

    /// Filter by priority (0-4).
    #[arg(short = 'p', long, value_parser = parse_priority)]
    pub priority: Option<Priority>,

    /// Filter by assignee.
    #[arg(short = 'a', long)]
    pub assignee: Option<String>,

    /// Filter by type (bug, feature, task, epic, chore).
    #[arg(short = 't', long = "type", alias = "issue-type", value_parser = parse_bead_type)]
    pub bead_type: Option<BeadType>,

    /// Filter by labels (AND: must have ALL). Repeat or comma-separated.
    #[arg(short = 'l', long = "label", value_delimiter = ',', num_args = 0..)]
    pub labels: Vec<String>,

    /// Filter by labels (OR: must have AT LEAST ONE). Repeat or comma-separated.
    #[arg(long = "label-any", value_delimiter = ',', num_args = 0..)]
    pub labels_any: Vec<String>,

    /// Filter by title text (case-insensitive substring match).
    #[arg(long)]
    pub title: Option<String>,

    /// Filter by specific issue IDs (comma-separated).
    #[arg(long)]
    pub id: Option<String>,

    /// Filter by title substring.
    #[arg(long = "title-contains")]
    pub title_contains: Option<String>,

    /// Filter by description substring.
    #[arg(long = "desc-contains")]
    pub desc_contains: Option<String>,

    /// Filter by notes substring.
    #[arg(long = "notes-contains")]
    pub notes_contains: Option<String>,

    /// Filter issues created after date (YYYY-MM-DD or RFC3339).
    #[arg(long = "created-after")]
    pub created_after: Option<String>,

    /// Filter issues created before date (YYYY-MM-DD or RFC3339).
    #[arg(long = "created-before")]
    pub created_before: Option<String>,

    /// Filter issues updated after date (YYYY-MM-DD or RFC3339).
    #[arg(long = "updated-after")]
    pub updated_after: Option<String>,

    /// Filter issues updated before date (YYYY-MM-DD or RFC3339).
    #[arg(long = "updated-before")]
    pub updated_before: Option<String>,

    /// Filter issues closed after date (YYYY-MM-DD or RFC3339).
    #[arg(long = "closed-after")]
    pub closed_after: Option<String>,

    /// Filter issues closed before date (YYYY-MM-DD or RFC3339).
    #[arg(long = "closed-before")]
    pub closed_before: Option<String>,

    /// Filter issues with empty description.
    #[arg(long = "empty-description")]
    pub empty_description: bool,

    /// Filter issues with no assignee.
    #[arg(long = "no-assignee")]
    pub no_assignee: bool,

    /// Filter issues with no labels.
    #[arg(long = "no-labels")]
    pub no_labels: bool,

    /// Filter by minimum priority (inclusive).
    #[arg(long = "priority-min", value_parser = parse_priority)]
    pub priority_min: Option<Priority>,

    /// Filter by maximum priority (inclusive).
    #[arg(long = "priority-max", value_parser = parse_priority)]
    pub priority_max: Option<Priority>,

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
    apply_common_filters(
        &mut filters,
        args.status.clone(),
        args.priority,
        args.bead_type,
        args.assignee.clone(),
        args.labels.clone(),
    )?;
    filters.priority_min = args.priority_min;
    filters.priority_max = args.priority_max;
    if !args.labels_any.is_empty() {
        filters.labels_any = Some(args.labels_any.clone());
    }

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

    filters.title_contains = args.title_contains.clone().filter(|s| !s.trim().is_empty());
    filters.desc_contains = args.desc_contains.clone().filter(|s| !s.trim().is_empty());
    filters.notes_contains = args.notes_contains.clone().filter(|s| !s.trim().is_empty());

    filters.created_after = parse_time_ms_opt(args.created_after.as_deref())?;
    filters.created_before = parse_time_ms_opt(args.created_before.as_deref())?;
    filters.updated_after = parse_time_ms_opt(args.updated_after.as_deref())?;
    filters.updated_before = parse_time_ms_opt(args.updated_before.as_deref())?;
    filters.closed_after = parse_time_ms_opt(args.closed_after.as_deref())?;
    filters.closed_before = parse_time_ms_opt(args.closed_before.as_deref())?;

    filters.empty_description = args.empty_description;
    filters.no_assignee = args.no_assignee;
    filters.no_labels = args.no_labels;

    let group_by = resolve_group_by(&args)?;

    let req = Request::Count {
        repo: ctx.repo.clone(),
        filters,
        group_by,
        read: ctx.read_consistency(),
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
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
            return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
                field: "by-*".into(),
                reason: "only one --by-* flag can be specified".into(),
            }));
        }
        group_by = Some(name);
    }

    Ok(group_by.map(|s| s.to_string()))
}
