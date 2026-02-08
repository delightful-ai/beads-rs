use clap::Subcommand;
use std::sync::LazyLock;

pub(super) mod admin;
pub(super) mod claim;
pub(super) mod close;
pub(super) mod comments;
pub(super) mod create;
pub(super) mod daemon;
pub(super) mod delete;
pub(super) mod deleted;
pub(super) mod dep;
pub(super) mod epic;
pub(super) mod init;
pub(super) mod label;
pub(super) mod migrate;
pub(super) mod reopen;
pub(super) mod show;
pub(super) mod status;
pub(super) mod store;
pub(super) mod subscribe;
pub(super) mod sync;
pub(super) mod unclaim;
pub(super) mod update;
pub(super) mod upgrade;

pub(super) fn fmt_issue_ref(namespace: &crate::core::NamespaceId, id: &str) -> String {
    format!("{}/{}", namespace.as_str(), id)
}

pub(super) fn fmt_labels(labels: &[String]) -> String {
    let mut out = String::from("[");
    for (i, l) in labels.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(l);
    }
    out.push(']');
    out
}

pub(super) fn fmt_metric_labels(labels: &[crate::api::AdminMetricLabel]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut out = String::from(" {");
    for (i, label) in labels.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(label.key.as_str());
        out.push('=');
        out.push_str(label.value.as_str());
    }
    out.push('}');
    out
}

static WALL_MS_FORMAT: LazyLock<Option<Vec<time::format_description::FormatItem<'static>>>> =
    LazyLock::new(|| time::format_description::parse("[year]-[month]-[day] [hour]:[minute]").ok());

pub(super) fn fmt_wall_ms(ms: u64) -> String {
    use time::OffsetDateTime;

    let dt = OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    match WALL_MS_FORMAT.as_deref() {
        Some(fmt) => dt.format(fmt).unwrap_or_else(|_| ms.to_string()),
        None => ms.to_string(),
    }
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Initialize beads store in this repo.
    Init,

    /// Create a new bead.
    #[command(alias = "new")]
    Create(create::CreateArgs),

    /// Show a bead.
    Show(show::ShowArgs),

    /// List beads.
    #[command(alias = "ls")]
    List(beads_cli::commands::list::ListArgs),

    /// Search beads by text (alias for list QUERY).
    Search(beads_cli::commands::search::SearchArgs),

    /// List beads that are ready to work on.
    Ready(beads_cli::commands::ready::ReadyArgs),

    /// Show blocked issues.
    Blocked,

    /// Show stale issues (not updated recently).
    Stale(beads_cli::commands::stale::StaleArgs),

    /// Count issues matching filters.
    Count(beads_cli::commands::count::CountArgs),

    /// Show deleted (tombstoned) issues.
    Deleted(deleted::DeletedArgs),

    /// Wait for debounced sync flush to complete.
    Sync,

    /// Subscribe to realtime events.
    Subscribe(subscribe::SubscribeArgs),

    /// Store operations.
    Store {
        #[command(subcommand)]
        cmd: store::StoreCmd,
    },

    /// Admin / introspection operations.
    Admin {
        #[command(subcommand)]
        cmd: admin::AdminCmd,
    },

    /// Upgrade bd to the latest version.
    Upgrade(upgrade::UpgradeArgs),

    /// Update a bead.
    Update(update::UpdateArgs),

    /// Close a bead.
    Close(close::CloseArgs),

    /// Reopen a bead.
    Reopen(reopen::ReopenArgs),

    /// Delete a bead.
    Delete(delete::DeleteArgs),

    /// Claim a bead for work (lease).
    Claim(claim::ClaimArgs),

    /// Release claim on a bead.
    Unclaim(unclaim::UnclaimArgs),

    /// Comments (alias for notes).
    #[command(alias = "notes")]
    Comments(comments::CommentsArgs),

    /// Add a comment (compat alias).
    #[command(alias = "note")]
    Comment(comments::CommentAddArgs),

    /// Dependency operations.
    #[command(alias = "deps", alias = "dependencies")]
    Dep {
        #[command(subcommand)]
        cmd: dep::DepCmd,
    },

    /// Label operations.
    Label {
        #[command(subcommand)]
        cmd: label::LabelCmd,
    },

    /// Epic operations.
    Epic {
        #[command(subcommand)]
        cmd: epic::EpicCmd,
    },

    /// Status overview.
    Status,

    /// Output AI-optimized workflow context.
    Prime,

    /// Display instructions for configuring AGENTS.md.
    Onboard(beads_cli::commands::onboard::OnboardArgs),

    /// Setup integration with AI editors.
    Setup {
        #[command(subcommand)]
        cmd: beads_cli::commands::setup::SetupCmd,
    },

    /// Internal migration tooling (hidden from global help).
    #[command(hide = true)]
    Migrate {
        #[command(subcommand)]
        cmd: migrate::MigrateCmd,
    },

    /// Daemon control (hidden). `bd daemon run` starts the service.
    #[command(hide = true)]
    Daemon {
        #[command(subcommand)]
        cmd: beads_cli::commands::daemon::DaemonCmd,
    },
}

pub(crate) fn command_name(command: &Command) -> String {
    match command {
        Command::Init => "init".to_string(),
        Command::Create(_) => "create".to_string(),
        Command::Show(_) => "show".to_string(),
        Command::List(_) => "list".to_string(),
        Command::Search(_) => "search".to_string(),
        Command::Ready(_) => "ready".to_string(),
        Command::Blocked => "blocked".to_string(),
        Command::Stale(_) => "stale".to_string(),
        Command::Count(_) => "count".to_string(),
        Command::Deleted(_) => "deleted".to_string(),
        Command::Sync => "sync".to_string(),
        Command::Subscribe(_) => "subscribe".to_string(),
        Command::Store { cmd } => format!("store.{}", store_cmd_name(cmd)),
        Command::Admin { cmd } => match cmd {
            admin::AdminCmd::Maintenance { cmd } => {
                format!("admin.maintenance.{}", maintenance_cmd_name(cmd))
            }
            _ => format!("admin.{}", admin_cmd_name(cmd)),
        },
        Command::Upgrade(_) => "upgrade".to_string(),
        Command::Update(_) => "update".to_string(),
        Command::Close(_) => "close".to_string(),
        Command::Reopen(_) => "reopen".to_string(),
        Command::Delete(_) => "delete".to_string(),
        Command::Claim(_) => "claim".to_string(),
        Command::Unclaim(_) => "unclaim".to_string(),
        Command::Comments(args) => match &args.cmd {
            Some(comments::CommentsCmd::Add(_)) => "comments.add".to_string(),
            None => "comments".to_string(),
        },
        Command::Comment(_) => "comment".to_string(),
        Command::Dep { cmd } => format!("dep.{}", dep_cmd_name(cmd)),
        Command::Label { cmd } => format!("label.{}", label_cmd_name(cmd)),
        Command::Epic { cmd } => format!("epic.{}", epic_cmd_name(cmd)),
        Command::Status => "status".to_string(),
        Command::Prime => "prime".to_string(),
        Command::Onboard(_) => "onboard".to_string(),
        Command::Setup { cmd } => format!("setup.{}", setup_cmd_name(cmd)),
        Command::Migrate { cmd } => format!("migrate.{}", migrate_cmd_name(cmd)),
        Command::Daemon { cmd } => format!("daemon.{}", daemon_cmd_name(cmd)),
    }
}

fn dep_cmd_name(cmd: &dep::DepCmd) -> &'static str {
    match cmd {
        dep::DepCmd::Add(_) => "add",
        dep::DepCmd::Rm(_) => "rm",
        dep::DepCmd::Tree { .. } => "tree",
        dep::DepCmd::Cycles => "cycles",
    }
}

fn label_cmd_name(cmd: &label::LabelCmd) -> &'static str {
    match cmd {
        label::LabelCmd::Add(_) => "add",
        label::LabelCmd::Remove(_) => "remove",
        label::LabelCmd::List { .. } => "list",
        label::LabelCmd::ListAll => "list-all",
    }
}

fn epic_cmd_name(cmd: &epic::EpicCmd) -> &'static str {
    match cmd {
        epic::EpicCmd::Status(_) => "status",
        epic::EpicCmd::CloseEligible(_) => "close-eligible",
    }
}

fn store_cmd_name(cmd: &store::StoreCmd) -> &'static str {
    match cmd {
        store::StoreCmd::Unlock(_) => "unlock",
        store::StoreCmd::Fsck(_) => "fsck",
    }
}

fn admin_cmd_name(cmd: &admin::AdminCmd) -> &'static str {
    match cmd {
        admin::AdminCmd::Status => "status",
        admin::AdminCmd::Metrics => "metrics",
        admin::AdminCmd::Doctor(_) => "doctor",
        admin::AdminCmd::Scrub(_) => "scrub",
        admin::AdminCmd::Flush(_) => "flush",
        admin::AdminCmd::Fingerprint(_) => "fingerprint",
        admin::AdminCmd::ReloadPolicies => "reload-policies",
        admin::AdminCmd::ReloadLimits => "reload-limits",
        admin::AdminCmd::RotateReplicaId => "rotate-replica-id",
        admin::AdminCmd::Maintenance { .. } => "maintenance",
        admin::AdminCmd::RebuildIndex => "rebuild-index",
    }
}

fn maintenance_cmd_name(cmd: &admin::AdminMaintenanceCmd) -> &'static str {
    match cmd {
        admin::AdminMaintenanceCmd::On => "on",
        admin::AdminMaintenanceCmd::Off => "off",
    }
}

fn setup_cmd_name(cmd: &beads_cli::commands::setup::SetupCmd) -> &'static str {
    match cmd {
        beads_cli::commands::setup::SetupCmd::Claude(_) => "claude",
        beads_cli::commands::setup::SetupCmd::Cursor(_) => "cursor",
        beads_cli::commands::setup::SetupCmd::Aider(_) => "aider",
    }
}

fn migrate_cmd_name(cmd: &migrate::MigrateCmd) -> &'static str {
    match cmd {
        migrate::MigrateCmd::Detect => "detect",
        migrate::MigrateCmd::To(_) => "to",
        migrate::MigrateCmd::FromGo(_) => "from-go",
    }
}

fn daemon_cmd_name(cmd: &beads_cli::commands::daemon::DaemonCmd) -> &'static str {
    match cmd {
        beads_cli::commands::daemon::DaemonCmd::Run => "run",
    }
}
