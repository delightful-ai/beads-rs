use std::ffi::OsString;
use std::path::{Path, PathBuf};

use clap::{ArgAction, Parser, Subcommand, builder::BoolishValueParser};

use crate::backend::CliHostBackend;
use crate::commands;
use crate::commands::CommandError;
use crate::commands::setup::{SetupCmd, handle_aider, handle_claude, handle_cursor};
use crate::runtime::CliRuntimeCtx;
use beads_surface::ipc::{EmptyPayload, IpcError, RepoCtx, Request, Response, send_request};

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
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Initialize beads store in this repo.
    Init,

    /// Create a new bead.
    #[command(alias = "new")]
    Create(commands::create::CreateArgs),

    /// Show a bead.
    Show(commands::show::ShowArgs),

    /// List beads.
    #[command(alias = "ls")]
    List(commands::list::ListArgs),

    /// Search beads by text (alias for list QUERY).
    Search(commands::search::SearchArgs),

    /// List beads that are ready to work on.
    Ready(commands::ready::ReadyArgs),

    /// Show blocked issues.
    Blocked,

    /// Show stale issues (not updated recently).
    Stale(commands::stale::StaleArgs),

    /// Count issues matching filters.
    Count(commands::count::CountArgs),

    /// Show deleted (tombstoned) issues.
    Deleted(commands::deleted::DeletedArgs),

    /// Wait for debounced sync flush to complete.
    Sync,

    /// Subscribe to realtime events.
    Subscribe(commands::subscribe::SubscribeArgs),

    /// Store operations.
    Store {
        #[command(subcommand)]
        cmd: commands::store::StoreCmd,
    },

    /// Admin / introspection operations.
    Admin {
        #[command(subcommand)]
        cmd: commands::admin::AdminCmd,
    },

    /// Upgrade bd to the latest version.
    Upgrade(commands::upgrade::UpgradeArgs),

    /// Update a bead.
    Update(commands::update::UpdateArgs),

    /// Close a bead.
    Close(commands::close::CloseArgs),

    /// Reopen a bead.
    Reopen(commands::reopen::ReopenArgs),

    /// Delete a bead.
    Delete(commands::delete::DeleteArgs),

    /// Claim a bead for work (lease).
    Claim(commands::claim::ClaimArgs),

    /// Release claim on a bead.
    Unclaim(commands::unclaim::UnclaimArgs),

    /// Comments (alias for notes).
    #[command(alias = "notes")]
    Comments(commands::comments::CommentsArgs),

    /// Add a comment (compat alias).
    #[command(alias = "note")]
    Comment(commands::comments::CommentAddArgs),

    /// Dependency operations.
    #[command(alias = "deps", alias = "dependencies")]
    Dep {
        #[command(subcommand)]
        cmd: commands::dep::DepCmd,
    },

    /// Label operations.
    Label {
        #[command(subcommand)]
        cmd: commands::label::LabelCmd,
    },

    /// Epic operations.
    Epic {
        #[command(subcommand)]
        cmd: commands::epic::EpicCmd,
    },

    /// Status overview.
    Status,

    /// Output AI-optimized workflow context.
    Prime,

    /// Display instructions for configuring AGENTS.md.
    Onboard(commands::onboard::OnboardArgs),

    /// Setup integration with AI editors.
    Setup {
        #[command(subcommand)]
        cmd: SetupCmd,
    },

    /// Internal migration tooling (hidden from global help).
    #[command(hide = true)]
    Migrate {
        #[command(subcommand)]
        cmd: commands::migrate::MigrateCmd,
    },

    /// Daemon control (hidden). `bd daemon run` starts the service.
    #[command(hide = true)]
    Daemon {
        #[command(subcommand)]
        cmd: commands::daemon::DaemonCmd,
    },
}

#[derive(Debug, Clone)]
pub struct RuntimeBuildArgs {
    pub json: bool,
    pub repo: Option<PathBuf>,
    pub actor: Option<String>,
    pub namespace: Option<String>,
    pub durability: Option<String>,
    pub client_request_id: Option<String>,
    pub require_min_seen: Option<String>,
    pub wait_timeout_ms: Option<u64>,
}

pub trait CliHost {
    type Error;

    fn maybe_spawn_auto_upgrade(&self);

    fn run_daemon_command(&self) -> std::result::Result<(), Self::Error>;

    fn build_runtime(
        &self,
        args: RuntimeBuildArgs,
    ) -> std::result::Result<CliRuntimeCtx, Self::Error>;

    fn in_beads_repo(&self) -> bool;

    fn discover_repo(&self) -> Option<PathBuf>;
}

pub fn parse_from<I, T>(args: I) -> Cli
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let raw: Vec<OsString> = args.into_iter().map(|arg| arg.into()).collect();
    Cli::parse_from(normalize_args(raw))
}

pub fn command_name(command: &Command) -> String {
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
            commands::admin::AdminCmd::Maintenance { cmd } => {
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
            Some(commands::comments::CommentsCmd::Add(_)) => "comments.add".to_string(),
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

pub fn run<H, B>(cli: Cli, host: &H, backend: &B) -> std::result::Result<(), H::Error>
where
    H: CliHost,
    B: CliHostBackend<Error = H::Error>,
    H::Error: From<CommandError>
        + From<commands::setup::SetupError>
        + From<IpcError>
        + From<crate::migrate::GoImportError>,
{
    if !matches!(cli.command, Command::Daemon { .. } | Command::Upgrade(_)) {
        host.maybe_spawn_auto_upgrade();
    }

    let Cli {
        json,
        repo,
        actor,
        namespace,
        durability,
        client_request_id,
        require_min_seen,
        wait_timeout_ms,
        quiet: _,
        verbose: _,
        command,
    } = cli;

    match command {
        Command::Daemon { cmd } => match cmd {
            commands::daemon::DaemonCmd::Run => host.run_daemon_command(),
        },
        Command::Prime => handle_prime(host),
        Command::Setup { cmd } => handle_setup::<H>(cmd),
        Command::Onboard(args) => handle_onboard(host, args.output.as_deref()),
        Command::Store { cmd } => commands::store::handle(json, cmd, backend),
        Command::Upgrade(args) => commands::upgrade::handle(json, args.background, backend),
        cmd => {
            let runtime = host.build_runtime(RuntimeBuildArgs {
                json,
                repo,
                actor,
                namespace,
                durability,
                client_request_id,
                require_min_seen,
                wait_timeout_ms,
            })?;
            dispatch_with_runtime::<H, B>(cmd, &runtime, backend)
        }
    }
}

fn dispatch_with_runtime<H, B>(
    command: Command,
    ctx: &CliRuntimeCtx,
    backend: &B,
) -> std::result::Result<(), H::Error>
where
    H: CliHost,
    B: CliHostBackend<Error = H::Error>,
    H::Error: From<CommandError> + From<crate::migrate::GoImportError>,
{
    match command {
        Command::Init => commands::init::handle(ctx).map_err(Into::into),
        Command::Create(args) => commands::create::handle(ctx, args).map_err(Into::into),
        Command::Show(args) => commands::show::handle(ctx, args).map_err(Into::into),
        Command::List(args) => commands::list::handle_list(ctx, args).map_err(Into::into),
        Command::Search(args) => commands::search::handle(ctx, args).map_err(Into::into),
        Command::Ready(args) => commands::ready::handle(ctx, args).map_err(Into::into),
        Command::Blocked => commands::blocked::handle(ctx).map_err(Into::into),
        Command::Stale(args) => commands::stale::handle(ctx, args).map_err(Into::into),
        Command::Count(args) => commands::count::handle(ctx, args).map_err(Into::into),
        Command::Deleted(args) => commands::deleted::handle(ctx, args).map_err(Into::into),
        Command::Sync => commands::sync::handle(ctx).map_err(Into::into),
        Command::Subscribe(args) => commands::subscribe::handle(ctx, args).map_err(Into::into),
        Command::Update(args) => commands::update::handle(ctx, args).map_err(Into::into),
        Command::Close(args) => commands::close::handle(ctx, args).map_err(Into::into),
        Command::Reopen(args) => commands::reopen::handle(ctx, args).map_err(Into::into),
        Command::Delete(args) => commands::delete::handle(ctx, args).map_err(Into::into),
        Command::Claim(args) => commands::claim::handle(ctx, args).map_err(Into::into),
        Command::Unclaim(args) => commands::unclaim::handle(ctx, args).map_err(Into::into),
        Command::Comments(args) => {
            commands::comments::handle_comments(ctx, args).map_err(Into::into)
        }
        Command::Comment(args) => {
            commands::comments::handle_comment_add(ctx, args).map_err(Into::into)
        }
        Command::Dep { cmd } => commands::dep::handle(ctx, cmd).map_err(Into::into),
        Command::Label { cmd } => commands::label::handle(ctx, cmd).map_err(Into::into),
        Command::Epic { cmd } => commands::epic::handle(ctx, cmd).map_err(Into::into),
        Command::Status => commands::status::handle(ctx).map_err(Into::into),
        Command::Admin { cmd } => commands::admin::handle(ctx, cmd).map_err(Into::into),
        Command::Migrate { cmd } => commands::migrate::handle(ctx, cmd, backend),
        Command::Daemon { .. }
        | Command::Prime
        | Command::Setup { .. }
        | Command::Onboard(_)
        | Command::Store { .. }
        | Command::Upgrade(_) => Ok(()),
    }
}

fn handle_setup<H>(cmd: SetupCmd) -> std::result::Result<(), H::Error>
where
    H: CliHost,
    H::Error: From<commands::setup::SetupError>,
{
    match cmd {
        SetupCmd::Claude(args) => handle_claude(args.project, args.check, args.remove),
        SetupCmd::Cursor(args) => handle_cursor(args.check, args.remove),
        SetupCmd::Aider(args) => handle_aider(args.check, args.remove),
    }
    .map_err(Into::into)
}

fn handle_prime<H>(host: &H) -> std::result::Result<(), H::Error>
where
    H: CliHost,
    H::Error: From<IpcError>,
{
    let mut stdout = std::io::stdout().lock();
    if let Err(err) = commands::prime::write_context_if(&mut stdout, host.in_beads_repo())
        && err.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(IpcError::from(err).into());
    }

    Ok(())
}

fn handle_onboard<H>(host: &H, output: Option<&Path>) -> std::result::Result<(), H::Error>
where
    H: CliHost,
    H::Error: From<IpcError>,
{
    let init_result = try_init(host);

    if let Some(path) = output {
        commands::onboard::generate_guide(path);
        crate::render::print_line(&format!("Generated {}", path.display()))?;
        crate::render::print_line("This file is auto-generated - do not edit manually")?;
    } else {
        commands::onboard::render_instructions(init_result);
    }

    Ok(())
}

fn try_init<H>(host: &H) -> bool
where
    H: CliHost,
{
    let Some(path) = host.discover_repo() else {
        return false;
    };

    let req = Request::Init {
        ctx: RepoCtx::new(path),
        payload: EmptyPayload {},
    };
    matches!(send_request(&req), Ok(Response::Ok { .. }))
}

fn normalize_args(mut raw: Vec<OsString>) -> Vec<OsString> {
    if raw.is_empty() {
        return raw;
    }

    let mut out = Vec::with_capacity(raw.len());
    out.push(raw.remove(0));

    for arg in raw {
        let s = arg.to_string_lossy();
        if s.starts_with("--") {
            let mut pieces = s.splitn(2, '=');
            let flag = pieces.next().unwrap_or("");
            let val = pieces.next();
            let mut canon = flag.to_lowercase().replace('_', "-");
            canon = canonical_flag(&canon).to_string();
            if let Some(value) = val {
                out.push(OsString::from(format!("{canon}={value}")));
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

fn dep_cmd_name(cmd: &commands::dep::DepCmd) -> &'static str {
    match cmd {
        commands::dep::DepCmd::Add(_) => "add",
        commands::dep::DepCmd::Rm(_) => "rm",
        commands::dep::DepCmd::Tree { .. } => "tree",
        commands::dep::DepCmd::Cycles => "cycles",
    }
}

fn label_cmd_name(cmd: &commands::label::LabelCmd) -> &'static str {
    match cmd {
        commands::label::LabelCmd::Add(_) => "add",
        commands::label::LabelCmd::Remove(_) => "remove",
        commands::label::LabelCmd::List { .. } => "list",
        commands::label::LabelCmd::ListAll => "list-all",
    }
}

fn epic_cmd_name(cmd: &commands::epic::EpicCmd) -> &'static str {
    match cmd {
        commands::epic::EpicCmd::Status(_) => "status",
        commands::epic::EpicCmd::CloseEligible(_) => "close-eligible",
    }
}

fn store_cmd_name(cmd: &commands::store::StoreCmd) -> &'static str {
    match cmd {
        commands::store::StoreCmd::Unlock(_) => "unlock",
        commands::store::StoreCmd::Fsck(_) => "fsck",
    }
}

fn admin_cmd_name(cmd: &commands::admin::AdminCmd) -> &'static str {
    match cmd {
        commands::admin::AdminCmd::Status => "status",
        commands::admin::AdminCmd::Metrics => "metrics",
        commands::admin::AdminCmd::Doctor(_) => "doctor",
        commands::admin::AdminCmd::Scrub(_) => "scrub",
        commands::admin::AdminCmd::Flush(_) => "flush",
        commands::admin::AdminCmd::Fingerprint(_) => "fingerprint",
        commands::admin::AdminCmd::ReloadPolicies => "reload-policies",
        commands::admin::AdminCmd::ReloadLimits => "reload-limits",
        commands::admin::AdminCmd::RotateReplicaId => "rotate-replica-id",
        commands::admin::AdminCmd::Maintenance { .. } => "maintenance",
        commands::admin::AdminCmd::RebuildIndex => "rebuild-index",
    }
}

fn maintenance_cmd_name(cmd: &commands::admin::AdminMaintenanceCmd) -> &'static str {
    match cmd {
        commands::admin::AdminMaintenanceCmd::On => "on",
        commands::admin::AdminMaintenanceCmd::Off => "off",
    }
}

fn setup_cmd_name(cmd: &SetupCmd) -> &'static str {
    match cmd {
        SetupCmd::Claude(_) => "claude",
        SetupCmd::Cursor(_) => "cursor",
        SetupCmd::Aider(_) => "aider",
    }
}

fn migrate_cmd_name(cmd: &commands::migrate::MigrateCmd) -> &'static str {
    match cmd {
        commands::migrate::MigrateCmd::Detect => "detect",
        commands::migrate::MigrateCmd::To(_) => "to",
        commands::migrate::MigrateCmd::FromGo(_) => "from-go",
    }
}

fn daemon_cmd_name(cmd: &commands::daemon::DaemonCmd) -> &'static str {
    match cmd {
        commands::daemon::DaemonCmd::Run => "run",
    }
}
