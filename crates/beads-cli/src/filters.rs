use beads_core::{ActorId, BeadType, CoreError, Priority};
use beads_surface::Filters;
use clap::Args;

use super::parsers::{parse_bead_type, parse_priority, parse_time_ms};

pub type Result<T> = std::result::Result<T, FilterError>;

#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    #[error(transparent)]
    Core(#[from] CoreError),

    #[error("validation failed for field {field}: {reason}")]
    Validation { field: String, reason: String },
}

pub fn validation_error(field: impl Into<String>, reason: impl Into<String>) -> FilterError {
    FilterError::Validation {
        field: field.into(),
        reason: reason.into(),
    }
}

#[derive(Args, Debug, Clone)]
pub struct CommonFilterArgs {
    /// Filter by status (open, in_progress, blocked, closed).
    #[arg(short = 's', long)]
    pub status: Option<String>,

    /// Filter by priority (0-4).
    #[arg(short = 'p', long, value_parser = parse_priority)]
    pub priority: Option<Priority>,

    /// Minimum priority (inclusive).
    #[arg(long = "priority-min", value_parser = parse_priority)]
    pub priority_min: Option<Priority>,

    /// Maximum priority (inclusive).
    #[arg(long = "priority-max", value_parser = parse_priority)]
    pub priority_max: Option<Priority>,

    /// Filter by type (bug, feature, task, epic, chore).
    #[arg(short = 't', long = "type", alias = "issue-type", value_parser = parse_bead_type)]
    pub bead_type: Option<BeadType>,

    /// Filter by assignee.
    #[arg(short = 'a', long)]
    pub assignee: Option<String>,

    /// Filter by labels (AND: must have ALL). Repeat or comma-separated.
    #[arg(short = 'l', long = "label", alias = "labels", value_delimiter = ',', num_args = 0..)]
    pub labels: Vec<String>,

    /// Filter by labels (OR: must have AT LEAST ONE). Repeat or comma-separated.
    #[arg(long = "label-any", value_delimiter = ',', num_args = 0..)]
    pub labels_any: Vec<String>,

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
}

pub fn apply_common_filters(args: &CommonFilterArgs, filters: &mut Filters) -> Result<()> {
    filters.status = args.status.clone();
    filters.priority = args.priority;
    filters.priority_min = args.priority_min;
    filters.priority_max = args.priority_max;
    filters.bead_type = args.bead_type;
    filters.assignee = args.assignee.as_deref().map(ActorId::new).transpose()?;
    filters.labels = if args.labels.is_empty() {
        None
    } else {
        Some(args.labels.clone())
    };
    if !args.labels_any.is_empty() {
        filters.labels_any = Some(args.labels_any.clone());
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
    Ok(())
}

fn parse_time_ms_opt(raw: Option<&str>) -> Result<Option<u64>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    parse_time_ms(trimmed)
        .map(Some)
        .map_err(|err| validation_error("date", err.to_string()))
}
