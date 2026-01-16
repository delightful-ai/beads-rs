//! CLI surface for beads-rs.
//!
//! Goal:
//! - Beads-go parity for the "main set" commands we care about now
//! - Extensible command tree + thin handlers
//! - LLM-robust parsing (aliases, boolish flags, case/dash tolerance)

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use clap::{ArgAction, Args, Parser, Subcommand, builder::BoolishValueParser};
use time::format_description::well_known::Rfc3339;
use time::{Date, OffsetDateTime, Time};

use crate::config::{Config, apply_env_overrides, load_for_repo};
use crate::core::{
    ActorId, Applied, BeadId, BeadSlug, BeadType, ClientRequestId, DepKind, DurabilityClass,
    NamespaceId, Priority, Watermarks,
};
use crate::daemon::ipc::{
    MutationMeta, ReadConsistency, Request, Response, ResponsePayload, send_request,
};
use crate::api::QueryResult;
use crate::daemon::query::SortField;
use crate::{Error, Result};

mod commands;
mod render;

// =============================================================================
// Entry + global options
// =============================================================================

#[derive(Parser, Debug)]
#[command(
    name = "bd",
    version,
    about = "Beads distributed issue tracker",
    infer_subcommands = true,
    infer_long_args = true,
    arg_required_else_help = true
)]
pub struct Cli {
    /// Machine-readable JSON output (default: false; use `--json` for scripting).
    #[arg(
        long,
        global = true,
        default_value_t = false,
        num_args = 0..=1,
        value_parser = BoolishValueParser::new()
    )]
    pub json: bool,

    /// Repository path (default: discover from cwd).
    #[arg(long, global = true, value_name = "PATH")]
    pub repo: Option<PathBuf>,

    /// Actor identity (overrides BD_ACTOR for this invocation).
    #[arg(long, global = true, value_name = "ACTOR")]
    pub actor: Option<String>,

    /// Namespace for mutation and query requests (default: core).
    #[arg(long, global = true, value_name = "NAMESPACE")]
    pub namespace: Option<String>,

    /// Durability class for mutation requests (default: local_fsync).
    #[arg(long, global = true, value_name = "DURABILITY")]
    pub durability: Option<String>,

    /// Client request id (UUID) for idempotency on retries.
    #[arg(long, global = true, value_name = "UUID")]
    pub client_request_id: Option<String>,

    /// Require applied watermarks before serving reads (JSON).
    #[arg(long = "require-min-seen", global = true, value_name = "JSON")]
    pub require_min_seen: Option<String>,

    /// Optional wait timeout for require-min-seen (ms).
    #[arg(long = "wait-timeout-ms", global = true, value_name = "MS")]
    pub wait_timeout_ms: Option<u64>,

    /// Errors only.
    #[arg(
        short = 'q',
        long,
        global = true,
        default_value_t = false,
        num_args = 0..=1,
        value_parser = BoolishValueParser::new()
    )]
    pub quiet: bool,

    /// Debug output (repeat for more).
    #[arg(short = 'v', long, global = true, action = ArgAction::Count)]
    pub verbose: u8,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Initialize beads store in this repo.
    Init,

    /// Create a new bead.
    #[command(alias = "new")]
    Create(CreateArgs),

    /// Show a bead.
    Show(ShowArgs),

    /// List beads.
    #[command(alias = "ls")]
    List(ListArgs),

    /// Search beads by text (alias for list QUERY).
    Search(SearchArgs),

    /// List beads that are ready to work on.
    Ready(ReadyArgs),

    /// Show blocked issues.
    Blocked,

    /// Show stale issues (not updated recently).
    Stale(StaleArgs),

    /// Count issues matching filters.
    Count(CountArgs),

    /// Show deleted (tombstoned) issues.
    Deleted(DeletedArgs),

    /// Wait for debounced sync flush to complete.
    Sync,

    /// Subscribe to realtime events.
    Subscribe(SubscribeArgs),

    /// Store operations.
    Store {
        #[command(subcommand)]
        cmd: StoreCmd,
    },

    /// Admin / introspection operations.
    Admin {
        #[command(subcommand)]
        cmd: AdminCmd,
    },

    /// Upgrade bd to the latest version.
    Upgrade(UpgradeArgs),

    /// Update a bead.
    Update(UpdateArgs),

    /// Close a bead.
    Close(CloseArgs),

    /// Reopen a bead.
    Reopen { id: String },

    /// Delete a bead.
    Delete(DeleteArgs),

    /// Claim a bead for work (lease).
    Claim(ClaimArgs),

    /// Release claim on a bead.
    Unclaim { id: String },

    /// Comments (alias for notes).
    #[command(alias = "notes")]
    Comments(CommentsArgs),

    /// Add a comment (compat alias).
    #[command(alias = "note")]
    Comment(CommentAddArgs),

    /// Dependency operations.
    #[command(alias = "deps", alias = "dependencies")]
    Dep {
        #[command(subcommand)]
        cmd: DepCmd,
    },

    /// Label operations.
    Label {
        #[command(subcommand)]
        cmd: LabelCmd,
    },

    /// Epic operations.
    Epic {
        #[command(subcommand)]
        cmd: EpicCmd,
    },

    /// Status overview.
    Status,

    /// Output AI-optimized workflow context.
    Prime,

    /// Display instructions for configuring AGENTS.md.
    Onboard(OnboardArgs),

    /// Setup integration with AI editors.
    Setup {
        #[command(subcommand)]
        cmd: SetupCmd,
    },

    /// Internal migration tooling (hidden from global help).
    #[command(hide = true)]
    Migrate {
        #[command(subcommand)]
        cmd: MigrateCmd,
    },

    /// Daemon control (hidden). `bd daemon run` starts the service.
    #[command(hide = true)]
    Daemon {
        #[command(subcommand)]
        cmd: DaemonCmd,
    },
}

#[derive(Subcommand, Debug)]
pub enum DaemonCmd {
    /// Run the daemon in the foreground (internal).
    Run,
}

// =============================================================================
// Per-command args
// =============================================================================

#[derive(Args, Debug)]
pub struct CreateArgs {
    /// Title (positional).
    #[arg(value_name = "TITLE", required = false)]
    pub title: Option<String>,

    /// Title (compat: `--title`).
    #[arg(long = "title", alias = "title-flag", value_name = "TITLE")]
    pub title_flag: Option<String>,

    /// Create multiple issues from a markdown file.
    #[arg(short = 'f', long = "file", value_name = "PATH")]
    pub file: Option<PathBuf>,

    /// Issue type.
    #[arg(short = 't', long = "type", alias = "issue-type", value_parser = parse_bead_type)]
    pub bead_type: Option<BeadType>,

    /// Priority 0-4 or words like "high".
    #[arg(short = 'p', long, value_parser = parse_priority)]
    pub priority: Option<Priority>,

    /// Description.
    #[arg(short = 'd', long, allow_hyphen_values = true)]
    pub description: Option<String>,

    /// Alias for --description (GitHub CLI convention).
    #[arg(long = "body", hide = true, allow_hyphen_values = true)]
    pub body: Option<String>,

    /// Assignee (compat; only supports current actor).
    #[arg(short = 'a', long)]
    pub assignee: Option<String>,

    /// Labels (comma-separated or repeat).
    #[arg(short = 'l', long = "labels", value_delimiter = ',', num_args = 0..)]
    pub labels: Vec<String>,

    /// Alias for --labels.
    #[arg(long = "label", hide = true, value_delimiter = ',', num_args = 0..)]
    pub label: Vec<String>,

    /// Design text.
    #[arg(long, allow_hyphen_values = true)]
    pub design: Option<String>,

    /// Acceptance criteria text.
    #[arg(
        long = "acceptance",
        alias = "acceptance-criteria",
        allow_hyphen_values = true
    )]
    pub acceptance: Option<String>,

    /// External reference (e.g., "gh-9", "jira-ABC").
    #[arg(long = "external-ref")]
    pub external_ref: Option<String>,

    /// Explicit issue ID (partitioning).
    #[arg(long)]
    pub id: Option<String>,

    /// Parent bead id (adds `parent` dep).
    #[arg(long)]
    pub parent: Option<String>,

    /// Dependencies (repeat or comma-separated): "type:id" or "id" (defaults to blocks).
    #[arg(long = "deps", value_delimiter = ',', num_args = 0..)]
    pub deps: Vec<String>,

    /// Time estimate in minutes.
    #[arg(short = 'e', long)]
    pub estimate: Option<u32>,

    /// No-op (compat). beads-rs doesn't require forcing.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args, Debug)]
pub struct UpgradeArgs {
    /// Run upgrade in the background (internal).
    #[arg(long, hide = true, default_value_t = false)]
    pub background: bool,
}

#[derive(Args, Debug)]
pub struct ShowArgs {
    pub id: String,

    /// No-op for compatibility (children are always shown).
    #[arg(long, hide = true)]
    pub children: bool,
}

#[derive(Args, Debug)]
pub struct ListArgs {
    /// Status filter (open, in_progress, closed).
    #[arg(short = 's', long, value_parser = parse_status)]
    pub status: Option<String>,

    /// Type filter.
    #[arg(short = 't', long = "type", alias = "issue-type", value_parser = parse_bead_type)]
    pub bead_type: Option<BeadType>,

    /// Priority filter.
    #[arg(short = 'p', long, value_parser = parse_priority)]
    pub priority: Option<Priority>,

    /// Minimum priority (inclusive).
    #[arg(long = "priority-min", value_parser = parse_priority)]
    pub priority_min: Option<Priority>,

    /// Maximum priority (inclusive).
    #[arg(long = "priority-max", value_parser = parse_priority)]
    pub priority_max: Option<Priority>,

    /// Assignee filter.
    #[arg(short = 'a', long)]
    pub assignee: Option<String>,

    /// Label filter (repeat or comma-separated).
    #[arg(short = 'l', long = "label", alias = "labels", value_delimiter = ',', num_args = 0..)]
    pub labels: Vec<String>,

    /// Label filter (OR: must have AT LEAST ONE). Repeat or comma-separated.
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

    /// Show labels in output.
    #[arg(short = 'L', long = "show-labels")]
    pub show_labels: bool,

    /// Limit results.
    #[arg(short = 'n', long)]
    pub limit: Option<usize>,

    /// Sort field (priority|created|updated|title) with optional :asc/:desc.
    #[arg(long)]
    pub sort: Option<String>,

    /// Filter by parent epic ID (shows children of this epic).
    #[arg(long)]
    pub parent: Option<String>,

    /// Optional text query (matches title/description).
    #[arg(value_name = "QUERY", num_args = 0..)]
    pub query: Vec<String>,
}

#[derive(Args, Debug)]
pub struct SearchArgs {
    /// Search query (multiple words allowed).
    #[arg(num_args = 1..)]
    pub query: Vec<String>,

    /// Limit results.
    #[arg(short = 'n', long)]
    pub limit: Option<usize>,
}

#[derive(Args, Debug)]
pub struct ReadyArgs {
    /// Limit results.
    #[arg(short = 'n', long)]
    pub limit: Option<usize>,
}

#[derive(Args, Debug)]
pub struct StaleArgs {
    /// Issues not updated in this many days.
    #[arg(short = 'd', long, default_value_t = 30)]
    pub days: u32,

    /// Filter by status (open|in_progress|blocked).
    #[arg(short = 's', long)]
    pub status: Option<String>,

    /// Maximum issues to show.
    #[arg(short = 'n', long, default_value_t = 50)]
    pub limit: usize,
}

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

#[derive(Args, Debug)]
pub struct DeletedArgs {
    /// Optional issue id to show details.
    #[arg(value_name = "ISSUE_ID", required = false)]
    pub id: Option<String>,

    /// Show deletions within this time range (e.g., 7d, 30d, 2w).
    #[arg(long, default_value = "7d")]
    pub since: String,

    /// Show all tracked deletions.
    #[arg(long)]
    pub all: bool,
}

#[derive(Args, Debug, Default)]
pub struct SubscribeArgs {}

#[derive(Args, Debug)]
pub struct UpdateArgs {
    pub id: String,

    /// Reparent the bead (adds/removes `parent` dependency).
    #[arg(long)]
    pub parent: Option<String>,

    /// Remove any existing parent relationship.
    #[arg(long = "no-parent", conflicts_with = "parent")]
    pub no_parent: bool,

    #[arg(long)]
    pub title: Option<String>,

    #[arg(short = 'd', long, allow_hyphen_values = true)]
    pub description: Option<String>,

    /// Alias for --description (GitHub CLI convention).
    #[arg(long = "body", hide = true, allow_hyphen_values = true)]
    pub body: Option<String>,

    #[arg(long, allow_hyphen_values = true)]
    pub design: Option<String>,

    #[arg(
        long = "acceptance",
        alias = "acceptance-criteria",
        allow_hyphen_values = true
    )]
    pub acceptance: Option<String>,

    /// External reference (e.g., "gh-9", "jira-ABC").
    #[arg(long = "external-ref")]
    pub external_ref: Option<String>,

    /// Time estimate in minutes.
    #[arg(short = 'e', long)]
    pub estimate: Option<u32>,

    #[arg(short = 's', long, value_parser = parse_status)]
    pub status: Option<String>,

    /// Close reason (only valid with --status=closed).
    #[arg(long, allow_hyphen_values = true)]
    pub reason: Option<String>,

    #[arg(short = 'p', long, value_parser = parse_priority)]
    pub priority: Option<Priority>,

    /// Change the issue type (bug, feature, task, epic, chore).
    #[arg(short = 't', long = "type", alias = "issue-type", value_parser = parse_bead_type)]
    pub bead_type: Option<BeadType>,

    /// Compat: assignee/claim.
    #[arg(short = 'a', long)]
    pub assignee: Option<String>,

    #[arg(long = "add-label", alias = "add_label", value_delimiter = ',', num_args = 0..)]
    pub add_label: Vec<String>,

    #[arg(long = "remove-label", alias = "remove_label", value_delimiter = ',', num_args = 0..)]
    pub remove_label: Vec<String>,

    /// Add a note.
    #[arg(long = "notes", alias = "note", allow_hyphen_values = true)]
    pub notes: Option<String>,

    /// Dependencies to add (repeat or comma-separated): "type:id" or "id" (defaults to blocks).
    #[arg(long = "deps", value_delimiter = ',', num_args = 0..)]
    pub deps: Vec<String>,
}

#[derive(Args, Debug)]
pub struct CloseArgs {
    pub id: String,

    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Args, Debug)]
pub struct DeleteArgs {
    /// One or more issue IDs to delete.
    #[arg(required = true, num_args = 1..)]
    pub ids: Vec<String>,

    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Args, Debug)]
pub struct ClaimArgs {
    pub id: String,

    /// Lease duration in seconds.
    #[arg(long, default_value_t = 3600)]
    pub lease_secs: u64,
}

#[derive(Args, Debug)]
pub struct CommentsArgs {
    /// Optional subcommand.
    #[command(subcommand)]
    pub cmd: Option<CommentsCmd>,

    /// Issue ID (lists comments when provided without a subcommand).
    #[arg(value_name = "ID", required = false)]
    pub id: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum CommentsCmd {
    /// Add a comment to an issue.
    Add(CommentAddArgs),
}

#[derive(Args, Debug)]
pub struct CommentAddArgs {
    pub id: String,

    /// Comment content (rest of args). If empty, reads stdin.
    #[arg(trailing_var_arg = true, num_args = 0..)]
    pub content: Vec<String>,
}

#[derive(Subcommand, Debug)]
pub enum DepCmd {
    /// Add a dependency: FROM depends on TO (FROM waits for TO to complete).
    Add(DepAddArgs),
    /// Remove a dependency between two issues.
    Rm(DepRmArgs),
    /// Show dependency tree for an issue.
    Tree { id: String },
    /// List dependency cycles.
    Cycles,
}

#[derive(Args, Debug)]
pub struct DepAddArgs {
    /// Issue that depends on another (waits for TO to complete).
    pub from: String,
    /// Issue that must complete first (blocks FROM).
    pub to: String,
    #[arg(long, alias = "type", value_parser = parse_dep_kind)]
    pub kind: Option<DepKind>,
}

#[derive(Args, Debug)]
pub struct DepRmArgs {
    pub from: String,
    pub to: String,
    #[arg(long, alias = "type", value_parser = parse_dep_kind)]
    pub kind: Option<DepKind>,
}

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

#[derive(Subcommand, Debug)]
pub enum EpicCmd {
    /// Show epic completion status.
    Status(EpicStatusArgs),
    /// Close epics where all children are complete.
    #[command(name = "close-eligible")]
    CloseEligible(EpicCloseEligibleArgs),
}

#[derive(Subcommand, Debug)]
pub enum StoreCmd {
    /// Unlock a store lock file.
    Unlock(StoreUnlockArgs),
    /// Offline WAL verification and repair.
    Fsck(StoreFsckArgs),
}

#[derive(Subcommand, Debug)]
pub enum AdminCmd {
    /// Show admin status snapshot.
    Status,
    /// Show admin metrics snapshot.
    Metrics,
    /// Run admin doctor checks.
    Doctor(AdminDoctorArgs),
    /// Run admin scrub now checks.
    #[command(name = "scrub")]
    Scrub(AdminScrubArgs),
    /// Flush WAL for a namespace.
    Flush(AdminFlushArgs),
    /// Show admin fingerprint for divergence detection.
    Fingerprint(AdminFingerprintArgs),
    /// Reload namespace policies from namespaces.toml.
    #[command(name = "reload-policies")]
    ReloadPolicies,
    /// Rotate the local replica id.
    #[command(name = "rotate-replica-id")]
    RotateReplicaId,
    /// Toggle maintenance mode.
    Maintenance {
        #[command(subcommand)]
        cmd: AdminMaintenanceCmd,
    },
    /// Rebuild WAL index from segments.
    #[command(name = "rebuild-index")]
    RebuildIndex,
}

#[derive(Subcommand, Debug)]
pub enum AdminMaintenanceCmd {
    /// Enable maintenance mode.
    On,
    /// Disable maintenance mode.
    Off,
}

#[derive(Args, Debug)]
pub struct AdminDoctorArgs {
    /// Max records to sample per namespace.
    #[arg(long = "max-records", default_value_t = 200)]
    pub max_records: u64,
}

#[derive(Args, Debug)]
pub struct AdminScrubArgs {
    /// Max records to sample per namespace.
    #[arg(long = "max-records", default_value_t = 200)]
    pub max_records: u64,
    /// Verify checkpoint cache entries.
    #[arg(long)]
    pub verify_checkpoint_cache: bool,
}

#[derive(Args, Debug)]
pub struct AdminFlushArgs {
    /// Trigger checkpoint immediately for matching groups.
    #[arg(long = "checkpoint-now")]
    pub checkpoint_now: bool,
}

#[derive(Args, Debug)]
pub struct AdminFingerprintArgs {
    /// Sample N shard indices per namespace.
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..=256))]
    pub sample: Option<u16>,
    /// Sampling nonce (auto-generated if omitted).
    #[arg(long, requires = "sample")]
    pub nonce: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum SetupCmd {
    /// Setup Claude Code integration (hooks for SessionStart/PreCompact).
    Claude(SetupClaudeArgs),
    /// Setup Cursor IDE integration (rules file).
    Cursor(SetupCursorArgs),
    /// Setup Aider integration (config + instructions).
    Aider(SetupAiderArgs),
}

#[derive(Args, Debug)]
pub struct SetupClaudeArgs {
    /// Install for this project only (not globally).
    #[arg(long)]
    pub project: bool,

    /// Check if Claude integration is installed.
    #[arg(long)]
    pub check: bool,

    /// Remove bd hooks from Claude settings.
    #[arg(long)]
    pub remove: bool,
}

#[derive(Args, Debug)]
pub struct SetupCursorArgs {
    /// Check if Cursor integration is installed.
    #[arg(long)]
    pub check: bool,

    /// Remove bd rules from Cursor.
    #[arg(long)]
    pub remove: bool,
}

#[derive(Args, Debug)]
pub struct SetupAiderArgs {
    /// Check if Aider integration is installed.
    #[arg(long)]
    pub check: bool,

    /// Remove bd config from Aider.
    #[arg(long)]
    pub remove: bool,
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

#[derive(Args, Debug)]
pub struct StoreUnlockArgs {
    /// Store ID to unlock.
    #[arg(long = "store-id", alias = "id", value_name = "STORE_ID")]
    pub store_id: String,

    /// Force unlock even if a daemon appears to be running.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args, Debug)]
pub struct StoreFsckArgs {
    /// Store ID to check.
    #[arg(long = "store-id", alias = "id", value_name = "STORE_ID")]
    pub store_id: String,

    /// Apply safe repairs (tail truncation, quarantine, wal.sqlite rebuild).
    #[arg(long)]
    pub repair: bool,
}

#[derive(Args, Debug)]
pub struct OnboardArgs {
    /// Generate BD_GUIDE.md at the specified path instead of printing instructions.
    #[arg(long, value_name = "PATH")]
    pub output: Option<PathBuf>,
}

// =============================================================================
// Migrate (hidden/internal)
// =============================================================================

#[derive(Subcommand, Debug)]
pub enum MigrateCmd {
    /// Show current store format and whether migration is needed.
    Detect,

    /// Migrate canonical store to a target format version.
    To(MigrateToArgs),

    /// Import/migrate from beads-go export.
    FromGo(MigrateFromGoArgs),
}

#[derive(Args, Debug)]
pub struct MigrateToArgs {
    /// Target format version.
    pub to: u32,

    /// Preview changes without writing commits.
    #[arg(long)]
    pub dry_run: bool,

    /// Skip safety checks.
    #[arg(long)]
    pub force: bool,

    /// Do not push to remote.
    #[arg(long)]
    pub no_push: bool,
}

#[derive(Args, Debug)]
pub struct MigrateFromGoArgs {
    /// Path to beads-go issues.jsonl (or export bundle).
    #[arg(long, value_name = "PATH")]
    pub input: PathBuf,

    /// Override the bead ID root slug during import (e.g. `myrepo` for `myrepo-abc123`).
    ///
    /// When omitted, the importer preserves whatever slug is present in the export IDs.
    #[arg(long, value_name = "SLUG")]
    pub root_slug: Option<String>,

    /// Preview without writing.
    #[arg(long)]
    pub dry_run: bool,

    /// Skip safety checks.
    #[arg(long)]
    pub force: bool,

    /// Do not push to remote.
    #[arg(long)]
    pub no_push: bool,
}

// =============================================================================
// Public API
// =============================================================================

/// Parse CLI from raw args, applying normalization for LLM robustness.
pub fn parse_from<I, T>(args: I) -> Cli
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let raw: Vec<OsString> = args.into_iter().map(|t| t.into()).collect();
    Cli::parse_from(normalize_args(raw))
}

/// Run the CLI (used by bin).
pub fn run(cli: Cli) -> Result<()> {
    if !matches!(cli.command, Commands::Daemon { .. } | Commands::Upgrade(_)) {
        crate::upgrade::maybe_spawn_auto_upgrade();
    }

    let actor_override = resolve_actor_override(cli.actor.as_deref())?;

    match cli.command {
        Commands::Daemon { cmd } => match cmd {
            DaemonCmd::Run => crate::daemon::run_daemon(),
        },
        // Prime is special: doesn't require repo, silently succeeds if not in beads project
        Commands::Prime => commands::prime::handle(),
        // Setup doesn't require an initialized beads repo
        Commands::Setup { cmd } => match cmd {
            SetupCmd::Claude(args) => {
                commands::setup::handle_claude(args.project, args.check, args.remove)
            }
            SetupCmd::Cursor(args) => commands::setup::handle_cursor(args.check, args.remove),
            SetupCmd::Aider(args) => commands::setup::handle_aider(args.check, args.remove),
        },
        // Onboard doesn't require an initialized beads repo
        Commands::Onboard(args) => commands::onboard::handle(args.output.as_deref()),
        // Store operations are global (no repo required).
        Commands::Store { cmd } => commands::store::handle(cli.json, cmd),
        Commands::Upgrade(args) => commands::upgrade::handle(cli.json, args.background),
        cmd => {
            let repo = resolve_repo(cli.repo)?;
            let config = load_cli_config(&repo);
            let ctx = Ctx {
                repo,
                json: cli.json,
                namespace: resolve_namespace(cli.namespace.as_deref(), &config)?,
                durability: resolve_durability(cli.durability.as_deref(), &config)?,
                client_request_id: normalize_optional_client_request_id(
                    cli.client_request_id.as_deref(),
                )?,
                require_min_seen: parse_require_min_seen(cli.require_min_seen.as_deref())?,
                wait_timeout_ms: cli.wait_timeout_ms,
                actor_id: actor_override.clone(),
            };

            match cmd {
                Commands::Init => commands::init::handle(&ctx),
                Commands::Create(args) => commands::create::handle(&ctx, args),
                Commands::Show(args) => commands::show::handle(&ctx, args),
                Commands::List(args) => commands::list::handle_list(&ctx, args),
                Commands::Search(args) => commands::list::handle_search(&ctx, args),
                Commands::Ready(args) => commands::ready::handle(&ctx, args),
                Commands::Blocked => commands::blocked::handle(&ctx),
                Commands::Stale(args) => commands::stale::handle(&ctx, args),
                Commands::Count(args) => commands::count::handle(&ctx, args),
                Commands::Deleted(args) => commands::deleted::handle(&ctx, args),
                Commands::Sync => commands::sync::handle(&ctx),
                Commands::Subscribe(args) => commands::subscribe::handle(&ctx, args),
                Commands::Update(args) => commands::update::handle(&ctx, args),
                Commands::Close(args) => commands::close::handle(&ctx, args),
                Commands::Reopen { id } => commands::reopen::handle(&ctx, id),
                Commands::Delete(args) => commands::delete::handle(&ctx, args),
                Commands::Claim(args) => commands::claim::handle(&ctx, args),
                Commands::Unclaim { id } => commands::unclaim::handle(&ctx, id),
                Commands::Comments(args) => commands::comments::handle_comments(&ctx, args),
                Commands::Comment(args) => commands::comments::handle_comment_add(&ctx, args),
                Commands::Dep { cmd } => commands::dep::handle(&ctx, cmd),
                Commands::Label { cmd } => commands::label::handle(&ctx, cmd),
                Commands::Epic { cmd } => commands::epic::handle(&ctx, cmd),
                Commands::Status => commands::status::handle(&ctx),
                Commands::Admin { cmd } => commands::admin::handle(&ctx, cmd),
                Commands::Migrate { cmd } => commands::migrate::handle(&ctx, cmd),
                // Daemon, Prime, Setup, Onboard, and Upgrade handled above.
                Commands::Daemon { .. }
                | Commands::Prime
                | Commands::Setup { .. }
                | Commands::Onboard(_)
                | Commands::Store { .. }
                | Commands::Upgrade(_) => Ok(()),
            }
        }
    }
}

// =============================================================================
// Context + helpers
// =============================================================================

#[derive(Clone)]
struct Ctx {
    repo: PathBuf,
    json: bool,
    namespace: Option<NamespaceId>,
    durability: Option<String>,
    client_request_id: Option<ClientRequestId>,
    require_min_seen: Option<Watermarks<Applied>>,
    wait_timeout_ms: Option<u64>,
    actor_id: Option<ActorId>,
}

impl Ctx {
    fn mutation_meta(&self) -> MutationMeta {
        MutationMeta {
            namespace: self.namespace.as_ref().map(|ns| ns.as_str().to_string()),
            durability: self.durability.clone(),
            client_request_id: self.client_request_id.as_ref().map(|id| id.to_string()),
            actor_id: self
                .actor_id
                .as_ref()
                .map(|actor| actor.as_str().to_string()),
        }
    }

    fn read_consistency(&self) -> ReadConsistency {
        ReadConsistency {
            namespace: self.namespace.as_ref().map(|ns| ns.as_str().to_string()),
            require_min_seen: self.require_min_seen.clone(),
            wait_timeout_ms: self.wait_timeout_ms,
        }
    }

    fn actor_id(&self) -> Result<ActorId> {
        self.actor_id
            .clone()
            .map(Ok)
            .unwrap_or_else(current_actor_id)
    }

    fn actor_string(&self) -> Result<String> {
        Ok(self.actor_id()?.as_str().to_string())
    }
}

fn resolve_repo(repo: Option<PathBuf>) -> Result<PathBuf> {
    let p = if let Some(p) = repo {
        p
    } else {
        let (_repo, path) = crate::repo::discover()?;
        path
    };

    let abs = if p.is_absolute() {
        p
    } else {
        let cwd = std::env::current_dir().map_err(|e| {
            Error::Ipc(crate::daemon::IpcError::DaemonUnavailable(format!(
                "failed to get cwd: {e}"
            )))
        })?;
        cwd.join(p)
    };

    Ok(std::fs::canonicalize(&abs).unwrap_or(abs))
}

fn load_cli_config(repo: &Path) -> Config {
    match load_for_repo(Some(repo)) {
        Ok(cfg) => cfg,
        Err(err) => {
            tracing::warn!("config load failed, using defaults: {err}");
            let mut cfg = Config::default();
            apply_env_overrides(&mut cfg);
            cfg
        }
    }
}

fn resolve_namespace(cli_value: Option<&str>, config: &Config) -> Result<Option<NamespaceId>> {
    if let Some(raw) = cli_value {
        normalize_optional_namespace(Some(raw))
    } else {
        Ok(config.defaults.namespace.clone())
    }
}

fn resolve_durability(cli_value: Option<&str>, config: &Config) -> Result<Option<String>> {
    let raw = cli_value
        .map(str::to_string)
        .or_else(|| config.defaults.durability.as_ref().map(ToString::to_string));
    let Some(raw) = raw else {
        return Ok(None);
    };
    let parsed = DurabilityClass::parse(&raw).map_err(|err| {
        Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "durability".into(),
            reason: err.to_string(),
        })
    })?;
    Ok(Some(parsed.to_string()))
}

pub(super) fn normalize_bead_id(id: &str) -> Result<BeadId> {
    normalize_bead_id_for("id", id)
}

pub(super) fn normalize_bead_id_for(field: &str, id: &str) -> Result<BeadId> {
    BeadId::parse(id).map_err(|e| {
        Error::Op(crate::daemon::OpError::ValidationFailed {
            field: field.into(),
            reason: e.to_string(),
        })
    })
}

pub(super) fn normalize_bead_ids(ids: Vec<String>) -> Result<Vec<BeadId>> {
    ids.into_iter().map(|id| normalize_bead_id(&id)).collect()
}

pub(super) fn normalize_bead_slug_for(field: &str, slug: &str) -> Result<BeadSlug> {
    BeadSlug::parse(slug).map_err(|e| {
        Error::Op(crate::daemon::OpError::ValidationFailed {
            field: field.into(),
            reason: e.to_string(),
        })
    })
}

pub(super) fn resolve_description(
    description: Option<String>,
    body: Option<String>,
) -> Result<Option<String>> {
    match (description, body) {
        (Some(d), Some(b)) => {
            if d != b {
                return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
                    field: "description".into(),
                    reason: format!(
                        "cannot specify both --description and --body with different values (--description={d:?}, --body={b:?})"
                    ),
                }));
            }
            Ok(Some(d))
        }
        (Some(d), None) => Ok(Some(d)),
        (None, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}

fn normalize_optional_namespace(raw: Option<&str>) -> Result<Option<NamespaceId>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "namespace".into(),
            reason: "namespace cannot be empty".into(),
        }));
    }
    NamespaceId::parse(trimmed.to_string())
        .map(Some)
        .map_err(|e| {
            Error::Op(crate::daemon::OpError::ValidationFailed {
                field: "namespace".into(),
                reason: e.to_string(),
            })
        })
}

fn normalize_optional_client_request_id(raw: Option<&str>) -> Result<Option<ClientRequestId>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "client_request_id".into(),
            reason: "client_request_id cannot be empty".into(),
        }));
    }
    ClientRequestId::parse_str(trimmed).map(Some).map_err(|e| {
        Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "client_request_id".into(),
            reason: e.to_string(),
        })
    })
}

fn parse_require_min_seen(raw: Option<&str>) -> Result<Option<Watermarks<Applied>>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    serde_json::from_str(raw).map(Some).map_err(|err| {
        Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "require_min_seen".into(),
            reason: err.to_string(),
        })
    })
}

fn validate_actor_id(raw: &str) -> Result<ActorId> {
    let trimmed = raw.trim();
    ActorId::new(trimmed).map_err(|e| {
        Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "actor".into(),
            reason: e.to_string(),
        })
    })
}

fn resolve_actor_override(raw: Option<&str>) -> Result<Option<ActorId>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    validate_actor_id(raw).map(Some)
}

pub(super) fn current_actor_id() -> Result<ActorId> {
    if let Ok(actor) = std::env::var("BD_ACTOR")
        && !actor.trim().is_empty()
    {
        return validate_actor_id(&actor);
    }
    let username = whoami::username();
    let hostname = whoami::fallible::hostname().unwrap_or_else(|_| "unknown".into());
    validate_actor_id(&format!("{}@{}", username, hostname))
}

pub(super) fn apply_common_filters(
    filters: &mut crate::daemon::query::Filters,
    status: Option<String>,
    priority: Option<Priority>,
    bead_type: Option<BeadType>,
    assignee: Option<String>,
    labels: Vec<String>,
) -> Result<()> {
    filters.status = status;
    filters.priority = priority;
    filters.bead_type = bead_type;
    filters.assignee = assignee.map(crate::core::ActorId::new).transpose()?;
    filters.labels = if labels.is_empty() {
        None
    } else {
        Some(labels)
    };
    Ok(())
}

pub(super) fn normalize_dep_specs(specs: Vec<String>) -> Result<Vec<String>> {
    let parsed = crate::core::DepSpec::parse_list(&specs).map_err(|e| {
        Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "deps".into(),
            reason: e.to_string(),
        })
    })?;

    Ok(parsed
        .into_iter()
        .map(|spec| spec.to_spec_string())
        .collect())
}

fn print_ok(payload: &ResponsePayload, json: bool) -> Result<()> {
    let s = if json {
        serde_json::to_string_pretty(payload).map_err(crate::daemon::IpcError::from)?
    } else {
        render::render_human(payload)
    };

    use std::io::Write;
    let mut stdout = std::io::stdout().lock();
    if let Err(e) = writeln!(stdout, "{s}")
        && e.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(crate::daemon::IpcError::from(e).into());
    }
    Ok(())
}

fn send(req: &Request) -> Result<ResponsePayload> {
    match send_request(req) {
        Ok(Response::Ok { ok }) => Ok(ok),
        Ok(Response::Err { err }) => {
            tracing::error!("error: {} - {}", err.code, err.message);
            if let Some(details) = err.details {
                tracing::error!("details: {}", details);
            }
            std::process::exit(1);
        }
        Err(e) => Err(e.into()),
    }
}

fn fetch_issue(ctx: &Ctx, id: &BeadId) -> Result<crate::api::Issue> {
    let req = Request::Show {
        repo: ctx.repo.clone(),
        id: id.as_str().to_string(),
        read: ctx.read_consistency(),
    };
    match send(&req)? {
        ResponsePayload::Query(QueryResult::Issue(issue)) => Ok(issue),
        other => Err(Error::Ipc(crate::daemon::IpcError::DaemonUnavailable(
            format!("unexpected response for show: {other:?}"),
        ))),
    }
}

fn fetch_issue_summaries(ctx: &Ctx, ids: Vec<String>) -> Result<Vec<crate::api::IssueSummary>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let req = Request::ShowMultiple {
        repo: ctx.repo.clone(),
        ids,
        read: ctx.read_consistency(),
    };
    match send(&req)? {
        ResponsePayload::Query(QueryResult::Issues(summaries)) => Ok(summaries),
        _ => Ok(Vec::new()),
    }
}

// =============================================================================
// Parsing helpers (LLM robust)
// =============================================================================

fn normalize_args(mut raw: Vec<OsString>) -> Vec<OsString> {
    if raw.is_empty() {
        return raw;
    }

    let mut out = Vec::with_capacity(raw.len());
    out.push(raw.remove(0)); // program name

    for arg in raw {
        let s = arg.to_string_lossy();
        if s.starts_with("--") {
            let mut pieces = s.splitn(2, '=');
            let flag = pieces.next().unwrap_or("");
            let val = pieces.next();
            let mut canon = flag.to_lowercase().replace('_', "-");
            canon = canonical_flag(&canon).to_string();
            if let Some(v) = val {
                out.push(OsString::from(format!("{canon}={v}")));
            } else {
                out.push(OsString::from(canon));
            }
        } else {
            out.push(arg);
        }
    }
    out
}

fn canonical_flag(flag: &str) -> &str {
    match flag {
        "--issue-type" | "--bead-type" => "--type",
        "--prio" => "--priority",
        "--acceptance-criteria" => "--acceptance",
        "--addlabel" => "--add-label",
        "--removelabel" => "--remove-label",
        other => other,
    }
}

fn parse_bead_type(raw: &str) -> std::result::Result<BeadType, String> {
    let s = raw.trim().to_lowercase();
    match s.as_str() {
        "bug" | "bugs" => Ok(BeadType::Bug),
        "feature" | "feat" | "features" => Ok(BeadType::Feature),
        "task" | "todo" | "tasks" => Ok(BeadType::Task),
        "epic" | "epics" => Ok(BeadType::Epic),
        "chore" | "chores" | "maintenance" => Ok(BeadType::Chore),
        _ => Err(format!("unknown bead type `{raw}`")),
    }
}

fn parse_priority(raw: &str) -> std::result::Result<Priority, String> {
    let s = raw.trim().to_lowercase();

    // Numeric, allow p1/P2 forms.
    let num_str = s.trim_start_matches('p');
    if let Ok(n) = num_str.parse::<u8>() {
        return Priority::new(n).map_err(|e| e.to_string());
    }

    match s.as_str() {
        "critical" | "crit" => Ok(Priority::CRITICAL),
        "high" => Ok(Priority::HIGH),
        "medium" | "med" => Ok(Priority::MEDIUM),
        "low" => Ok(Priority::LOW),
        "backlog" | "lowest" => Ok(Priority::LOWEST),
        _ => Err(format!("invalid priority `{raw}`")),
    }
}

fn parse_status(raw: &str) -> std::result::Result<String, String> {
    let s = raw.trim().to_lowercase().replace(['-', ' '], "_");
    let canon = match s.as_str() {
        "open" | "todo" => "open",
        "inprogress" | "in_progress" | "doing" | "wip" => "in_progress",
        "closed" | "done" | "complete" => "closed",
        other => other,
    };
    Ok(canon.to_string())
}

fn parse_dep_kind(raw: &str) -> std::result::Result<DepKind, String> {
    DepKind::parse(raw).map_err(|e| e.to_string())
}

fn parse_sort(raw: &str) -> std::result::Result<(SortField, bool), String> {
    let mut s = raw.trim().to_lowercase();
    let mut ascending = false;

    if s.starts_with('-') {
        s = s.trim_start_matches('-').to_string();
        ascending = false;
    }

    let tmp = s.clone();
    if let Some((field, dir)) = tmp.split_once(':') {
        s = field.to_string();
        ascending = matches!(dir, "asc" | "ascending");
    }

    let field = match s.as_str() {
        "priority" | "prio" => SortField::Priority,
        "created" | "created_at" | "createdat" => SortField::CreatedAt,
        "updated" | "updated_at" | "updatedat" => SortField::UpdatedAt,
        "title" | "name" => SortField::Title,
        _ => return Err(format!("invalid sort field `{raw}`")),
    };
    Ok((field, ascending))
}

fn parse_time_ms_opt(s: Option<&str>) -> Result<Option<u64>> {
    let Some(s) = s else { return Ok(None) };
    let s = s.trim();
    if s.is_empty() {
        return Ok(None);
    }

    Ok(Some(parse_time_ms(s).map_err(|msg| {
        Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "date".into(),
            reason: msg,
        })
    })?))
}

fn parse_time_ms(s: &str) -> std::result::Result<u64, String> {
    // RFC3339
    if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
        return Ok(dt.unix_timestamp_nanos() as u64 / 1_000_000);
    }

    // YYYY-MM-DD (midnight UTC)
    let fmt_date =
        time::format_description::parse("[year]-[month]-[day]").map_err(|e| e.to_string())?;
    if let Ok(date) = Date::parse(s, &fmt_date) {
        let dt = date.with_time(Time::MIDNIGHT).assume_utc();
        return Ok(dt.unix_timestamp_nanos() as u64 / 1_000_000);
    }

    // YYYY-MM-DD HH:MM:SS (UTC)
    let fmt_dt = time::format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]")
        .map_err(|e| e.to_string())?;
    if let Ok(dt) = time::PrimitiveDateTime::parse(s, &fmt_dt) {
        let dt = dt.assume_utc();
        return Ok(dt.unix_timestamp_nanos() as u64 / 1_000_000);
    }

    Err(format!(
        "unsupported date format: {s:?} (use YYYY-MM-DD or RFC3339)"
    ))
}

fn parse_dep_edge(
    kind_flag: Option<DepKind>,
    from_raw: &str,
    to_raw: &str,
) -> std::result::Result<(DepKind, BeadId, BeadId), String> {
    let (kind_from, from_raw) = split_kind_id(from_raw)?;
    let (kind_to, to_raw) = split_kind_id(to_raw)?;

    let kind = kind_flag
        .or(kind_from)
        .or(kind_to)
        .unwrap_or(DepKind::Blocks);

    let from =
        BeadId::parse(&from_raw).map_err(|e| format!("invalid from id {from_raw:?}: {e}"))?;
    let to = BeadId::parse(&to_raw).map_err(|e| format!("invalid to id {to_raw:?}: {e}"))?;

    Ok((kind, from, to))
}

fn split_kind_id(raw: &str) -> std::result::Result<(Option<DepKind>, String), String> {
    if let Some((k, id)) = raw.split_once(':') {
        Ok((Some(parse_dep_kind(k)?), id.trim().to_string()))
    } else {
        Ok((None, raw.trim().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DefaultsConfig;
    use crate::core::{DurabilityClass, HeadStatus, ReplicaId, Seq0};
    use crate::daemon::OpError;
    use std::num::NonZeroU32;
    use std::path::PathBuf;
    use uuid::Uuid;

    fn assert_validation_failed(err: Error, field: &str) {
        match err {
            Error::Op(OpError::ValidationFailed { field: got, .. }) => {
                assert_eq!(got, field);
            }
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    #[test]
    fn normalize_bead_id_rejects_empty() {
        let err = normalize_bead_id("").unwrap_err();
        assert_validation_failed(err, "id");
    }

    #[test]
    fn normalize_bead_id_canonicalizes() {
        let id = normalize_bead_id("BeAd-ABC123").expect("valid bead id");
        assert_eq!(id.as_str(), "bead-abc123");
    }

    #[test]
    fn normalize_bead_slug_rejects_invalid() {
        let err = normalize_bead_slug_for("root_slug", "-bad-").unwrap_err();
        assert_validation_failed(err, "root_slug");
    }

    #[test]
    fn normalize_optional_namespace_rejects_empty() {
        let err = normalize_optional_namespace(Some("   ")).unwrap_err();
        assert_validation_failed(err, "namespace");
    }

    #[test]
    fn normalize_optional_namespace_accepts_core() {
        let ns = normalize_optional_namespace(Some("core")).expect("valid namespace");
        assert_eq!(ns.unwrap().as_str(), "core");
    }

    #[test]
    fn resolve_defaults_use_config_when_no_flags() {
        let mut config = Config::default();
        config.defaults = DefaultsConfig {
            namespace: Some(NamespaceId::parse("wf").unwrap()),
            durability: Some(DurabilityClass::ReplicatedFsync {
                k: NonZeroU32::new(2).unwrap(),
            }),
            ..DefaultsConfig::default()
        };

        let namespace = resolve_namespace(None, &config).expect("namespace");
        assert_eq!(namespace, Some(NamespaceId::parse("wf").unwrap()));
        let durability = resolve_durability(None, &config).expect("durability");
        assert_eq!(durability, Some("replicated_fsync(2)".to_string()));
    }

    #[test]
    fn resolve_defaults_cli_overrides_config() {
        let mut config = Config::default();
        config.defaults = DefaultsConfig {
            namespace: Some(NamespaceId::parse("wf").unwrap()),
            durability: Some(DurabilityClass::LocalFsync),
            ..DefaultsConfig::default()
        };

        let namespace = resolve_namespace(Some("core"), &config).expect("namespace");
        assert_eq!(namespace, Some(NamespaceId::core()));
        let durability =
            resolve_durability(Some("replicated_fsync(3)"), &config).expect("durability");
        assert_eq!(durability, Some("replicated_fsync(3)".to_string()));
    }

    #[test]
    fn normalize_optional_client_request_id_rejects_empty() {
        let err = normalize_optional_client_request_id(Some("")).unwrap_err();
        assert_validation_failed(err, "client_request_id");
    }

    #[test]
    fn normalize_optional_client_request_id_accepts_uuid() {
        let id = normalize_optional_client_request_id(Some("00000000-0000-0000-0000-000000000000"))
            .expect("valid client request id");
        assert!(id.is_some());
    }

    #[test]
    fn validate_actor_id_rejects_blank() {
        let err = validate_actor_id("   ").unwrap_err();
        assert_validation_failed(err, "actor");
    }

    #[test]
    fn resolve_actor_override_accepts_actor() {
        let actor = resolve_actor_override(Some("alice@example.com")).expect("actor");
        assert_eq!(actor, Some(ActorId::new("alice@example.com").unwrap()));
    }

    #[test]
    fn mutation_meta_includes_actor_override() {
        let actor = ActorId::new("alice@example.com").unwrap();
        let ctx = Ctx {
            repo: PathBuf::from("/tmp/beads"),
            json: false,
            namespace: None,
            durability: None,
            client_request_id: None,
            require_min_seen: None,
            wait_timeout_ms: None,
            actor_id: Some(actor.clone()),
        };
        let meta = ctx.mutation_meta();
        assert_eq!(meta.actor_id, Some(actor.as_str().to_string()));
    }

    #[test]
    fn parse_require_min_seen_accepts_json() {
        let origin = ReplicaId::new(Uuid::from_bytes([3u8; 16]));
        let mut watermarks = Watermarks::<Applied>::new();
        watermarks
            .observe_at_least(
                &NamespaceId::core(),
                &origin,
                Seq0::new(1),
                HeadStatus::Known([1u8; 32]),
            )
            .expect("watermark");
        let json = serde_json::to_string(&watermarks).expect("json");

        let parsed = parse_require_min_seen(Some(&json)).expect("parse");
        assert_eq!(parsed, Some(watermarks));
    }

    #[test]
    fn parse_require_min_seen_rejects_invalid_json() {
        let err = parse_require_min_seen(Some("{not-json")).unwrap_err();
        assert_validation_failed(err, "require_min_seen");
    }

    #[test]
    fn read_consistency_includes_read_gating() {
        let origin = ReplicaId::new(Uuid::from_bytes([4u8; 16]));
        let mut watermarks = Watermarks::<Applied>::new();
        watermarks
            .observe_at_least(
                &NamespaceId::core(),
                &origin,
                Seq0::new(2),
                HeadStatus::Known([2u8; 32]),
            )
            .expect("watermark");
        let ctx = Ctx {
            repo: PathBuf::from("/tmp/beads"),
            json: false,
            namespace: None,
            durability: None,
            client_request_id: None,
            require_min_seen: Some(watermarks.clone()),
            wait_timeout_ms: Some(50),
            actor_id: None,
        };
        let read = ctx.read_consistency();
        assert_eq!(read.require_min_seen, Some(watermarks));
        assert_eq!(read.wait_timeout_ms, Some(50));
    }
}
