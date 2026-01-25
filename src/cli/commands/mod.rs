use clap::Subcommand;

pub(super) mod admin;
pub(super) mod blocked;
pub(super) mod claim;
pub(super) mod close;
pub(super) mod comments;
pub(super) mod count;
pub(super) mod create;
pub(super) mod daemon;
pub(super) mod delete;
pub(super) mod deleted;
pub(super) mod dep;
pub(super) mod epic;
pub(super) mod init;
pub(super) mod label;
pub(super) mod list;
pub(super) mod migrate;
pub(super) mod onboard;
pub(super) mod prime;
pub(super) mod ready;
pub(super) mod reopen;
pub(super) mod setup;
pub(super) mod show;
pub(super) mod stale;
pub(super) mod status;
pub(super) mod store;
pub(super) mod subscribe;
pub(super) mod sync;
pub(super) mod unclaim;
pub(super) mod update;
pub(super) mod upgrade;

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Initialize beads store in this repo.
    Init,

    /// Create a new bead.
    #[command(alias = "new")]
    Create(create::CreateArgs),

    /// Show a bead.
    Show(show::ShowArgs),

    /// List beads.
    #[command(alias = "ls")]
    List(list::ListArgs),

    /// Search beads by text (alias for list QUERY).
    Search(list::SearchArgs),

    /// List beads that are ready to work on.
    Ready(ready::ReadyArgs),

    /// Show blocked issues.
    Blocked,

    /// Show stale issues (not updated recently).
    Stale(stale::StaleArgs),

    /// Count issues matching filters.
    Count(count::CountArgs),

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
    Onboard(onboard::OnboardArgs),

    /// Setup integration with AI editors.
    Setup {
        #[command(subcommand)]
        cmd: setup::SetupCmd,
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
        cmd: daemon::DaemonCmd,
    },
}

pub fn command_name(command: &Commands) -> String {
    match command {
        Commands::Init => "init".to_string(),
        Commands::Create(_) => "create".to_string(),
        Commands::Show(_) => "show".to_string(),
        Commands::List(_) => "list".to_string(),
        Commands::Search(_) => "search".to_string(),
        Commands::Ready(_) => "ready".to_string(),
        Commands::Blocked => "blocked".to_string(),
        Commands::Stale(_) => "stale".to_string(),
        Commands::Count(_) => "count".to_string(),
        Commands::Deleted(_) => "deleted".to_string(),
        Commands::Sync => "sync".to_string(),
        Commands::Subscribe(_) => "subscribe".to_string(),
        Commands::Store { cmd } => format!("store.{}", store_cmd_name(cmd)),
        Commands::Admin { cmd } => match cmd {
            admin::AdminCmd::Maintenance { cmd } => {
                format!("admin.maintenance.{}", maintenance_cmd_name(cmd))
            }
            _ => format!("admin.{}", admin_cmd_name(cmd)),
        },
        Commands::Upgrade(_) => "upgrade".to_string(),
        Commands::Update(_) => "update".to_string(),
        Commands::Close(_) => "close".to_string(),
        Commands::Reopen(_) => "reopen".to_string(),
        Commands::Delete(_) => "delete".to_string(),
        Commands::Claim(_) => "claim".to_string(),
        Commands::Unclaim(_) => "unclaim".to_string(),
        Commands::Comments(args) => match &args.cmd {
            Some(comments::CommentsCmd::Add(_)) => "comments.add".to_string(),
            None => "comments".to_string(),
        },
        Commands::Comment(_) => "comment".to_string(),
        Commands::Dep { cmd } => format!("dep.{}", dep_cmd_name(cmd)),
        Commands::Label { cmd } => format!("label.{}", label_cmd_name(cmd)),
        Commands::Epic { cmd } => format!("epic.{}", epic_cmd_name(cmd)),
        Commands::Status => "status".to_string(),
        Commands::Prime => "prime".to_string(),
        Commands::Onboard(_) => "onboard".to_string(),
        Commands::Setup { cmd } => format!("setup.{}", setup_cmd_name(cmd)),
        Commands::Migrate { cmd } => format!("migrate.{}", migrate_cmd_name(cmd)),
        Commands::Daemon { cmd } => format!("daemon.{}", daemon_cmd_name(cmd)),
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

fn setup_cmd_name(cmd: &setup::SetupCmd) -> &'static str {
    match cmd {
        setup::SetupCmd::Claude(_) => "claude",
        setup::SetupCmd::Cursor(_) => "cursor",
        setup::SetupCmd::Aider(_) => "aider",
    }
}

fn migrate_cmd_name(cmd: &migrate::MigrateCmd) -> &'static str {
    match cmd {
        migrate::MigrateCmd::Detect => "detect",
        migrate::MigrateCmd::To(_) => "to",
        migrate::MigrateCmd::FromGo(_) => "from-go",
    }
}

fn daemon_cmd_name(cmd: &daemon::DaemonCmd) -> &'static str {
    match cmd {
        daemon::DaemonCmd::Run => "run",
    }
}
