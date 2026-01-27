//! CLI surface for beads-rs.
//!
//! Goal:
//! - Beads-go parity for the "main set" commands we care about now
//! - Extensible command tree + thin handlers
//! - LLM-robust parsing (aliases, boolish flags, case/dash tolerance)

use std::cell::RefCell;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use clap::{ArgAction, Parser, builder::BoolishValueParser};

use crate::api::QueryResult;
use crate::config::{Config, apply_env_overrides, load_for_repo};
use crate::core::{
    ActorId, Applied, BeadId, BeadSlug, ClientRequestId, DurabilityClass, NamespaceId, Watermarks,
};
use crate::daemon::ipc::{
    IdPayload, IpcClient, IpcConnection, MutationCtx, MutationMeta, ReadConsistency, ReadCtx,
    RepoCtx, Request, Response, ResponsePayload,
};
use crate::{Error, Result};

pub use commands::Command;
pub use commands::daemon::DaemonCmd;
pub(super) use filters::CommonFilterArgs;

mod commands;
mod filters;
mod parse;

thread_local! {
    static COMMAND_CONNECTION: RefCell<Option<IpcConnection>> = const { RefCell::new(None) };
}

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
    pub command: commands::Command,
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

pub fn command_name(command: &Command) -> String {
    commands::command_name(command)
}

/// Run the CLI (used by bin).
pub fn run(cli: Cli) -> Result<()> {
    if !matches!(cli.command, Command::Daemon { .. } | Command::Upgrade(_)) {
        crate::upgrade::maybe_spawn_auto_upgrade();
    }

    let actor_override = resolve_actor_override(cli.actor.as_deref())?;

    match cli.command {
        Command::Daemon { cmd } => match cmd {
            commands::daemon::DaemonCmd::Run => crate::daemon::run_daemon(),
        },
        // Prime is special: doesn't require repo, silently succeeds if not in beads project
        Command::Prime => commands::prime::handle(),
        // Setup doesn't require an initialized beads repo
        Command::Setup { cmd } => match cmd {
            commands::setup::SetupCmd::Claude(args) => {
                commands::setup::handle_claude(args.project, args.check, args.remove)
            }
            commands::setup::SetupCmd::Cursor(args) => {
                commands::setup::handle_cursor(args.check, args.remove)
            }
            commands::setup::SetupCmd::Aider(args) => {
                commands::setup::handle_aider(args.check, args.remove)
            }
        },
        // Onboard doesn't require an initialized beads repo
        Command::Onboard(args) => commands::onboard::handle(args.output.as_deref()),
        // Store operations are global (no repo required).
        Command::Store { cmd } => commands::store::handle(cli.json, cmd),
        Command::Upgrade(args) => commands::upgrade::handle(cli.json, args.background),
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
                Command::Init => commands::init::handle(&ctx),
                Command::Create(args) => commands::create::handle(&ctx, args),
                Command::Show(args) => commands::show::handle(&ctx, args),
                Command::List(args) => commands::list::handle_list(&ctx, args),
                Command::Search(args) => commands::search::handle(&ctx, args),
                Command::Ready(args) => commands::ready::handle(&ctx, args),
                Command::Blocked => commands::blocked::handle(&ctx),
                Command::Stale(args) => commands::stale::handle(&ctx, args),
                Command::Count(args) => commands::count::handle(&ctx, args),
                Command::Deleted(args) => commands::deleted::handle(&ctx, args),
                Command::Sync => commands::sync::handle(&ctx),
                Command::Subscribe(args) => commands::subscribe::handle(&ctx, args),
                Command::Update(args) => commands::update::handle(&ctx, args),
                Command::Close(args) => commands::close::handle(&ctx, args),
                Command::Reopen(args) => commands::reopen::handle(&ctx, args),
                Command::Delete(args) => commands::delete::handle(&ctx, args),
                Command::Claim(args) => commands::claim::handle(&ctx, args),
                Command::Unclaim(args) => commands::unclaim::handle(&ctx, args),
                Command::Comments(args) => commands::comments::handle_comments(&ctx, args),
                Command::Comment(args) => commands::comments::handle_comment_add(&ctx, args),
                Command::Dep { cmd } => commands::dep::handle(&ctx, cmd),
                Command::Label { cmd } => commands::label::handle(&ctx, cmd),
                Command::Epic { cmd } => commands::epic::handle(&ctx, cmd),
                Command::Status => commands::status::handle(&ctx),
                Command::Admin { cmd } => commands::admin::handle(&ctx, cmd),
                Command::Migrate { cmd } => commands::migrate::handle(&ctx, cmd),
                // Daemon, Prime, Setup, Onboard, and Upgrade handled above.
                Command::Daemon { .. }
                | Command::Prime
                | Command::Setup { .. }
                | Command::Onboard(_)
                | Command::Store { .. }
                | Command::Upgrade(_) => Ok(()),
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
    durability: Option<DurabilityClass>,
    client_request_id: Option<ClientRequestId>,
    require_min_seen: Option<Watermarks<Applied>>,
    wait_timeout_ms: Option<u64>,
    actor_id: Option<ActorId>,
}

impl Ctx {
    fn mutation_meta(&self) -> MutationMeta {
        MutationMeta {
            namespace: self.namespace.clone(),
            durability: self.durability,
            client_request_id: self.client_request_id,
            actor_id: self.actor_id.clone(),
        }
    }

    fn mutation_ctx(&self) -> MutationCtx {
        MutationCtx::new(self.repo.clone(), self.mutation_meta())
    }

    fn read_consistency(&self) -> ReadConsistency {
        ReadConsistency {
            namespace: self.namespace.clone(),
            require_min_seen: self.require_min_seen.clone(),
            wait_timeout_ms: self.wait_timeout_ms,
        }
    }

    fn read_ctx(&self) -> ReadCtx {
        ReadCtx::new(self.repo.clone(), self.read_consistency())
    }

    fn repo_ctx(&self) -> RepoCtx {
        RepoCtx::new(self.repo.clone())
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
    let cfg = match load_for_repo(Some(repo)) {
        Ok(cfg) => cfg,
        Err(err) => {
            tracing::warn!("config load failed, using defaults: {err}");
            let mut cfg = Config::default();
            apply_env_overrides(&mut cfg);
            cfg
        }
    };
    crate::paths::init_from_config(&cfg.paths);
    cfg
}

fn resolve_namespace(cli_value: Option<&str>, config: &Config) -> Result<Option<NamespaceId>> {
    if let Some(raw) = cli_value {
        normalize_optional_namespace(Some(raw))
    } else {
        Ok(config.defaults.namespace.clone())
    }
}

fn resolve_durability(cli_value: Option<&str>, config: &Config) -> Result<Option<DurabilityClass>> {
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
    Ok(Some(parsed))
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
    if json {
        return print_json(payload);
    }
    let s = render_human(payload);
    use std::io::Write;
    let mut stdout = std::io::stdout().lock();
    if let Err(e) = writeln!(stdout, "{s}")
        && e.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(crate::daemon::IpcError::from(e).into());
    }
    Ok(())
}

fn render_human(payload: &ResponsePayload) -> String {
    match payload {
        ResponsePayload::Op(op) => render_op(&op.result),
        ResponsePayload::Query(q) => render_query(q),
        ResponsePayload::Synced(_) => "synced".into(),
        ResponsePayload::Refreshed(_) => "refreshed".into(),
        ResponsePayload::Initialized(_) => "initialized".into(),
        ResponsePayload::ShuttingDown(_) => "shutting down".into(),
        ResponsePayload::Subscribed(sub) => {
            format!("subscribed to {}", sub.subscribed.namespace.as_str())
        }
        ResponsePayload::Event(ev) => {
            format!("event {}", ev.event.event_id.origin_seq.get())
        }
    }
}

fn render_op(op: &crate::daemon::ops::OpResult) -> String {
    match op {
        crate::daemon::ops::OpResult::Created { id } => {
            commands::create::render_created(id.as_str())
        }
        crate::daemon::ops::OpResult::Updated { id } => {
            commands::update::render_updated(id.as_str())
        }
        crate::daemon::ops::OpResult::Closed { id } => commands::close::render_closed(id.as_str()),
        crate::daemon::ops::OpResult::Reopened { id } => {
            commands::reopen::render_reopened(id.as_str())
        }
        crate::daemon::ops::OpResult::Deleted { id } => {
            commands::delete::render_deleted_op(id.as_str())
        }
        crate::daemon::ops::OpResult::DepAdded { from, to } => {
            commands::dep::render_dep_added(from.as_str(), to.as_str())
        }
        crate::daemon::ops::OpResult::DepRemoved { from, to } => {
            commands::dep::render_dep_removed(from.as_str(), to.as_str())
        }
        crate::daemon::ops::OpResult::NoteAdded { bead_id, .. } => {
            commands::comments::render_comment_added(bead_id.as_str())
        }
        crate::daemon::ops::OpResult::Claimed { id, expires } => {
            commands::claim::render_claimed(id.as_str(), expires.0)
        }
        crate::daemon::ops::OpResult::Unclaimed { id } => {
            commands::unclaim::render_unclaimed(id.as_str())
        }
        crate::daemon::ops::OpResult::ClaimExtended { id, expires } => {
            commands::claim::render_claim_extended(id.as_str(), expires.0)
        }
    }
}

fn render_query(q: &QueryResult) -> String {
    match q {
        QueryResult::Issue(issue) => commands::show::render_issue_detail(issue),
        QueryResult::Issues(views) => commands::list::render_issue_list_opts(views, false),
        QueryResult::DepTree { root, edges } => {
            commands::dep::render_dep_tree(root.as_str(), edges)
        }
        QueryResult::Deps { incoming, outgoing } => commands::dep::render_deps(incoming, outgoing),
        QueryResult::DepCycles(out) => commands::dep::render_dep_cycles(out),
        QueryResult::Notes(notes) => commands::comments::render_notes(notes),
        QueryResult::Status(out) => commands::status::render_status(out),
        QueryResult::Blocked(blocked) => commands::blocked::render_blocked(blocked),
        QueryResult::Ready(result) => {
            commands::ready::render_ready(&result.issues, result.blocked_count, result.closed_count)
        }
        QueryResult::Stale(issues) => commands::list::render_issue_list_opts(issues, false),
        QueryResult::Count(result) => commands::count::render_count(result),
        QueryResult::Deleted(tombs) => commands::deleted::render_deleted(tombs),
        QueryResult::DeletedLookup(out) => commands::deleted::render_deleted_lookup(out),
        QueryResult::EpicStatus(statuses) => commands::epic::render_epic_statuses(statuses),
        QueryResult::DaemonInfo(info) => commands::daemon::render_daemon_info(info),
        QueryResult::AdminStatus(status) => commands::admin::render_admin_status(status),
        QueryResult::AdminMetrics(metrics) => commands::admin::render_admin_metrics(metrics),
        QueryResult::AdminDoctor(out) => commands::admin::render_admin_doctor(out),
        QueryResult::AdminScrub(out) => commands::admin::render_admin_scrub(out),
        QueryResult::AdminFlush(out) => commands::admin::render_admin_flush(out),
        QueryResult::AdminCheckpoint(out) => commands::admin::render_admin_checkpoint(out),
        QueryResult::AdminFingerprint(out) => commands::admin::render_admin_fingerprint(out),
        QueryResult::AdminReloadPolicies(out) => commands::admin::render_admin_reload_policies(out),
        QueryResult::AdminReloadReplication(out) => {
            commands::admin::render_admin_reload_replication(out)
        }
        QueryResult::AdminReloadLimits(out) => commands::admin::render_admin_reload_limits(out),
        QueryResult::AdminRotateReplicaId(out) => {
            commands::admin::render_admin_rotate_replica_id(out)
        }
        QueryResult::AdminMaintenanceMode(out) => commands::admin::render_admin_maintenance(out),
        QueryResult::AdminRebuildIndex(out) => commands::admin::render_admin_rebuild_index(out),
        QueryResult::Validation { warnings } => {
            if warnings.is_empty() {
                "ok".into()
            } else {
                warnings.join("\n")
            }
        }
    }
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    use std::io::Write;
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut stdout, value).map_err(crate::daemon::IpcError::from)?;
    if let Err(e) = writeln!(stdout)
        && e.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(crate::daemon::IpcError::from(e).into());
    }
    Ok(())
}

fn send_raw(req: &Request) -> Result<Response> {
    let response = send_raw_once(req).or_else(|err| {
        if err.transience().is_retryable() {
            COMMAND_CONNECTION.with(|conn| {
                *conn.borrow_mut() = None;
            });
            send_raw_once(req)
        } else {
            Err(err)
        }
    })?;
    Ok(response)
}

fn send_raw_once(req: &Request) -> std::result::Result<Response, crate::daemon::ipc::IpcError> {
    COMMAND_CONNECTION.with(|conn| {
        let mut conn = conn.borrow_mut();
        if conn.is_none() {
            let client = IpcClient::new();
            *conn = Some(client.connect()?);
        }
        conn.as_mut()
            .expect("command connection initialized")
            .send_request(req)
    })
}

fn send(req: &Request) -> Result<ResponsePayload> {
    match send_raw(req)? {
        Response::Ok { ok } => Ok(ok),
        Response::Err { err } => {
            tracing::error!("error: {} - {}", err.code, err.message);
            if let Some(details) = err.details {
                tracing::error!("details: {}", details);
            }
            std::process::exit(1);
        }
    }
}

fn fetch_issue(ctx: &Ctx, id: &BeadId) -> Result<crate::api::Issue> {
    let req = Request::Show {
        ctx: ctx.read_ctx(),
        payload: IdPayload { id: id.clone() },
    };
    match send(&req)? {
        ResponsePayload::Query(QueryResult::Issue(issue)) => Ok(issue),
        other => Err(Error::Ipc(crate::daemon::IpcError::DaemonUnavailable(
            format!("unexpected response for show: {other:?}"),
        ))),
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
        let config = Config {
            defaults: DefaultsConfig {
                namespace: Some(NamespaceId::parse("wf").unwrap()),
                durability: Some(DurabilityClass::ReplicatedFsync {
                    k: NonZeroU32::new(2).unwrap(),
                }),
                ..DefaultsConfig::default()
            },
            ..Default::default()
        };

        let namespace = resolve_namespace(None, &config).expect("namespace");
        assert_eq!(namespace, Some(NamespaceId::parse("wf").unwrap()));
        let durability = resolve_durability(None, &config).expect("durability");
        assert_eq!(durability, Some("replicated_fsync(2)".to_string()));
    }

    #[test]
    fn resolve_defaults_cli_overrides_config() {
        let config = Config {
            defaults: DefaultsConfig {
                namespace: Some(NamespaceId::parse("wf").unwrap()),
                durability: Some(DurabilityClass::LocalFsync),
                ..DefaultsConfig::default()
            },
            ..Default::default()
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
