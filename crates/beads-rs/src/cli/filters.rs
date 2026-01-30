use clap::Args;

use crate::Result;
use crate::cli::parsers::{parse_bead_type, parse_priority, parse_time_ms_opt};
use crate::core::{ActorId, BeadType, Priority};
use crate::daemon::query::Filters;

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

impl CommonFilterArgs {
    pub fn apply(&self, filters: &mut Filters) -> Result<()> {
        filters.status = self.status.clone();
        filters.priority = self.priority;
        filters.priority_min = self.priority_min;
        filters.priority_max = self.priority_max;
        filters.bead_type = self.bead_type;
        filters.assignee = self.assignee.as_deref().map(ActorId::new).transpose()?;
        filters.labels = if self.labels.is_empty() {
            None
        } else {
            Some(self.labels.clone())
        };
        if !self.labels_any.is_empty() {
            filters.labels_any = Some(self.labels_any.clone());
        }
        filters.title_contains = self.title_contains.clone().filter(|s| !s.trim().is_empty());
        filters.desc_contains = self.desc_contains.clone().filter(|s| !s.trim().is_empty());
        filters.notes_contains = self.notes_contains.clone().filter(|s| !s.trim().is_empty());
        filters.created_after = parse_time_ms_opt(self.created_after.as_deref())?;
        filters.created_before = parse_time_ms_opt(self.created_before.as_deref())?;
        filters.updated_after = parse_time_ms_opt(self.updated_after.as_deref())?;
        filters.updated_before = parse_time_ms_opt(self.updated_before.as_deref())?;
        filters.closed_after = parse_time_ms_opt(self.closed_after.as_deref())?;
        filters.closed_before = parse_time_ms_opt(self.closed_before.as_deref())?;
        filters.empty_description = self.empty_description;
        filters.no_assignee = self.no_assignee;
        filters.no_labels = self.no_labels;
        Ok(())
    }
}
