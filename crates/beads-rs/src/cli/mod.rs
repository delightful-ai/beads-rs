//! CLI surface for beads-rs.
//!
//! Goal:
//! - Beads-go parity for the "main set" commands we care about now
//! - Extensible command tree + thin handlers
//! - LLM-robust parsing (aliases, boolish flags, case/dash tolerance)

use std::ffi::OsString;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use clap::{ArgAction, Parser, builder::BoolishValueParser};

use crate::api::QueryResult;
use crate::config::{Config, apply_env_overrides, load_for_repo};
use crate::core::{ActorId, Applied, BeadId, DurabilityClass, NamespaceId, Watermarks};
use crate::{Error, Result};
use beads_cli::commands::onboard::{generate_guide, render_instructions};
use beads_cli::commands::prime::write_context_if;
use beads_cli::commands::setup::{SetupCmd, handle_aider, handle_claude, handle_cursor};
use beads_cli::runtime::{
    CliRuntimeCtx, resolve_description as cli_resolve_description, send as cli_send,
    send_raw as cli_send_raw, validate_actor_id as cli_validate_actor_id,
};
use beads_cli::validation::{normalize_optional_client_request_id, normalize_optional_namespace};
use beads_surface::ipc::{
    EmptyPayload, IdPayload, RepoCtx, Request, Response, ResponsePayload, send_request,
};

pub use beads_cli::commands::daemon::DaemonCmd;
pub use commands::Command;

mod backend;
mod commands;

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
    let backend = backend::BeadsRsCliBackend;

    match cli.command {
        Command::Daemon { cmd } => match cmd {
            beads_cli::commands::daemon::DaemonCmd::Run => crate::run_daemon_command(),
        },
        // Prime is special: doesn't require repo, silently succeeds if not in beads project
        Command::Prime => handle_prime(),
        // Setup doesn't require an initialized beads repo
        Command::Setup { cmd } => handle_setup(cmd),
        // Onboard doesn't require an initialized beads repo
        Command::Onboard(args) => handle_onboard(args.output.as_deref()),
        // Store operations are global (no repo required).
        Command::Store { cmd } => commands::store::handle(cli.json, cmd, &backend),
        Command::Upgrade(args) => commands::upgrade::handle(cli.json, args.background, &backend),
        cmd => {
            let repo = resolve_repo(cli.repo)?;
            let config = load_cli_config(&repo);
            let ctx = Ctx::new(
                CliRuntimeCtx {
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
                },
                backend,
            );

            match cmd {
                Command::Init => commands::init::handle(&ctx),
                Command::Create(args) => commands::create::handle(&ctx, args),
                Command::Show(args) => commands::show::handle(&ctx, args),
                Command::List(args) => {
                    beads_cli::commands::list::handle_list(&ctx, args).map_err(Into::into)
                }
                Command::Search(args) => {
                    beads_cli::commands::search::handle(&ctx, args).map_err(Into::into)
                }
                Command::Ready(args) => {
                    beads_cli::commands::ready::handle(&ctx, args).map_err(Into::into)
                }
                Command::Blocked => beads_cli::commands::blocked::handle(&ctx).map_err(Into::into),
                Command::Stale(args) => {
                    beads_cli::commands::stale::handle(&ctx, args).map_err(Into::into)
                }
                Command::Count(args) => {
                    beads_cli::commands::count::handle(&ctx, args).map_err(Into::into)
                }
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

fn handle_prime() -> Result<()> {
    let in_beads_repo = match crate::repo::discover() {
        Ok((repo, _)) => repo.refname_to_id("refs/heads/beads/store").is_ok(),
        Err(_) => false,
    };

    let mut stdout = std::io::stdout().lock();
    if let Err(e) = write_context_if(&mut stdout, in_beads_repo)
        && e.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(beads_surface::IpcError::from(e).into());
    }

    Ok(())
}

fn handle_setup(cmd: SetupCmd) -> Result<()> {
    match cmd {
        SetupCmd::Claude(args) => {
            handle_claude(args.project, args.check, args.remove).map_err(Into::into)
        }
        SetupCmd::Cursor(args) => handle_cursor(args.check, args.remove).map_err(Into::into),
        SetupCmd::Aider(args) => handle_aider(args.check, args.remove).map_err(Into::into),
    }
}

fn handle_onboard(output: Option<&Path>) -> Result<()> {
    let init_result = try_init();

    if let Some(path) = output {
        generate_guide(path);
        print_line(&format!("Generated {}", path.display()))?;
        print_line("This file is auto-generated - do not edit manually")?;
    } else {
        render_instructions(init_result);
    }
    Ok(())
}

fn try_init() -> bool {
    if let Ok((_repo, path)) = crate::repo::discover() {
        let req = Request::Init {
            ctx: RepoCtx::new(path),
            payload: EmptyPayload {},
        };
        matches!(send_request(&req), Ok(Response::Ok { .. }))
    } else {
        false
    }
}

// =============================================================================
// Context + helpers
// =============================================================================

type Ctx = CliCtx<backend::BeadsRsCliBackend>;

#[derive(Debug, Clone)]
struct CliCtx<B> {
    runtime: CliRuntimeCtx,
    backend: B,
}

impl<B> CliCtx<B> {
    fn new(runtime: CliRuntimeCtx, backend: B) -> Self {
        Self { runtime, backend }
    }

    fn backend(&self) -> &B {
        &self.backend
    }
}

impl<B> Deref for CliCtx<B> {
    type Target = CliRuntimeCtx;

    fn deref(&self) -> &Self::Target {
        &self.runtime
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
            Error::Ipc(beads_surface::IpcError::DaemonUnavailable(format!(
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
        normalize_optional_namespace(Some(raw)).map_err(Into::into)
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
        Error::Op(crate::OpError::ValidationFailed {
            field: "durability".into(),
            reason: err.to_string(),
        })
    })?;
    Ok(Some(parsed))
}

pub(super) fn resolve_description(
    description: Option<String>,
    body: Option<String>,
) -> Result<Option<String>> {
    cli_resolve_description(description, body).map_err(Into::into)
}

fn parse_require_min_seen(raw: Option<&str>) -> Result<Option<Watermarks<Applied>>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    serde_json::from_str(raw).map(Some).map_err(|err| {
        Error::Op(crate::OpError::ValidationFailed {
            field: "require_min_seen".into(),
            reason: err.to_string(),
        })
    })
}

fn validate_actor_id(raw: &str) -> Result<ActorId> {
    cli_validate_actor_id(raw).map_err(Into::into)
}

fn resolve_actor_override(raw: Option<&str>) -> Result<Option<ActorId>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    validate_actor_id(raw).map(Some)
}

fn print_ok(payload: &ResponsePayload, json: bool) -> Result<()> {
    if json {
        return print_json(payload);
    }
    let s = render_human(payload);
    print_line(&s)
}

pub(super) fn print_line(line: &str) -> Result<()> {
    use std::io::Write;
    let mut stdout = std::io::stdout().lock();
    if let Err(e) = writeln!(stdout, "{line}")
        && e.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(beads_surface::IpcError::from(e).into());
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

fn render_op(op: &beads_surface::OpResult) -> String {
    match op {
        beads_surface::OpResult::Created { id } => commands::create::render_created(id.as_str()),
        beads_surface::OpResult::Updated { id } => commands::update::render_updated(id.as_str()),
        beads_surface::OpResult::Closed { id } => commands::close::render_closed(id.as_str()),
        beads_surface::OpResult::Reopened { id } => commands::reopen::render_reopened(id.as_str()),
        beads_surface::OpResult::Deleted { id } => commands::delete::render_deleted_op(id.as_str()),
        beads_surface::OpResult::DepAdded { from, to } => {
            commands::dep::render_dep_added(from.as_str(), to.as_str())
        }
        beads_surface::OpResult::DepRemoved { from, to } => {
            commands::dep::render_dep_removed(from.as_str(), to.as_str())
        }
        beads_surface::OpResult::NoteAdded { bead_id, .. } => {
            commands::comments::render_comment_added(bead_id.as_str())
        }
        beads_surface::OpResult::Claimed { id, expires } => {
            commands::claim::render_claimed(id.as_str(), expires.0)
        }
        beads_surface::OpResult::Unclaimed { id } => {
            commands::unclaim::render_unclaimed(id.as_str())
        }
        beads_surface::OpResult::ClaimExtended { id, expires } => {
            commands::claim::render_claim_extended(id.as_str(), expires.0)
        }
    }
}

fn render_query(q: &QueryResult) -> String {
    match q {
        QueryResult::Issue(issue) => commands::show::render_issue_detail(issue),
        QueryResult::Issues(views) => {
            beads_cli::commands::list::render_issue_list_opts(views, false)
        }
        QueryResult::DepTree { root, edges } => {
            commands::dep::render_dep_tree(root.as_str(), edges)
        }
        QueryResult::Deps { incoming, outgoing } => commands::dep::render_deps(incoming, outgoing),
        QueryResult::DepCycles(out) => commands::dep::render_dep_cycles(out),
        QueryResult::Notes(notes) => commands::comments::render_notes(notes),
        QueryResult::Status(out) => commands::status::render_status(out),
        QueryResult::Blocked(blocked) => beads_cli::commands::blocked::render_blocked(blocked),
        QueryResult::Ready(result) => beads_cli::commands::ready::render_ready(
            &result.issues,
            result.blocked_count,
            result.closed_count,
        ),
        QueryResult::Stale(issues) => {
            beads_cli::commands::list::render_issue_list_opts(issues, false)
        }
        QueryResult::Count(result) => beads_cli::commands::count::render_count(result),
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
        QueryResult::AdminFsck(out) => commands::store::render_admin_fsck(out),
        QueryResult::AdminStoreUnlock(out) => commands::store::render_admin_store_unlock(out),
        QueryResult::AdminStoreLockInfo(out) => commands::store::render_admin_store_lock_info(out),
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
    serde_json::to_writer_pretty(&mut stdout, value).map_err(beads_surface::IpcError::from)?;
    if let Err(e) = writeln!(stdout)
        && e.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(beads_surface::IpcError::from(e).into());
    }
    Ok(())
}

fn send_raw(req: &Request) -> Result<Response> {
    cli_send_raw(req).map_err(Error::from)
}

fn send(req: &Request) -> Result<ResponsePayload> {
    cli_send(req).map_err(Error::from)
}

fn fetch_issue(ctx: &Ctx, id: &BeadId) -> Result<crate::api::Issue> {
    let req = Request::Show {
        ctx: ctx.read_ctx(),
        payload: IdPayload { id: id.clone() },
    };
    match send(&req)? {
        ResponsePayload::Query(QueryResult::Issue(issue)) => Ok(issue),
        other => Err(Error::Ipc(beads_surface::IpcError::DaemonUnavailable(
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
    use crate::OpError;
    use crate::config::DefaultsConfig;
    use crate::core::{DurabilityClass, HeadStatus, ReplicaId, Seq0};
    use beads_cli::validation::{
        normalize_bead_id, normalize_bead_slug_for, normalize_optional_client_request_id,
        normalize_optional_namespace,
    };
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
        let err: Error = normalize_bead_id("").unwrap_err().into();
        assert_validation_failed(err, "id");
    }

    #[test]
    fn normalize_bead_id_canonicalizes() {
        let id = normalize_bead_id("BeAd-ABC123").expect("valid bead id");
        assert_eq!(id.as_str(), "bead-abc123");
    }

    #[test]
    fn normalize_bead_slug_rejects_invalid() {
        let err: Error = normalize_bead_slug_for("root_slug", "-bad-")
            .unwrap_err()
            .into();
        assert_validation_failed(err, "root_slug");
    }

    #[test]
    fn normalize_optional_namespace_rejects_empty() {
        let err: Error = normalize_optional_namespace(Some("   "))
            .unwrap_err()
            .into();
        assert_validation_failed(err, "namespace");
    }

    #[test]
    fn normalize_optional_namespace_accepts_core() {
        let ns = normalize_optional_namespace(Some("core")).expect("valid namespace");
        assert_eq!(ns.unwrap().as_str(), "core");
    }

    #[test]
    fn normalize_optional_namespace_rejects_invalid_chars() {
        let err: Error = normalize_optional_namespace(Some("core name"))
            .unwrap_err()
            .into();
        assert_validation_failed(err, "namespace");
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
        assert_eq!(
            durability,
            Some(DurabilityClass::ReplicatedFsync {
                k: NonZeroU32::new(2).expect("k")
            })
        );
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
        assert_eq!(
            durability,
            Some(DurabilityClass::ReplicatedFsync {
                k: NonZeroU32::new(3).expect("k")
            })
        );
    }

    #[test]
    fn normalize_optional_client_request_id_rejects_empty() {
        let err: Error = normalize_optional_client_request_id(Some(""))
            .unwrap_err()
            .into();
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
        let ctx = Ctx::new(
            CliRuntimeCtx {
                repo: PathBuf::from("/tmp/beads"),
                json: false,
                namespace: None,
                durability: None,
                client_request_id: None,
                require_min_seen: None,
                wait_timeout_ms: None,
                actor_id: Some(actor.clone()),
            },
            backend::BeadsRsCliBackend,
        );
        let meta = ctx.mutation_meta();
        assert_eq!(meta.actor_id, Some(actor));
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
        let ctx = Ctx::new(
            CliRuntimeCtx {
                repo: PathBuf::from("/tmp/beads"),
                json: false,
                namespace: None,
                durability: None,
                client_request_id: None,
                require_min_seen: Some(watermarks.clone()),
                wait_timeout_ms: Some(50),
                actor_id: None,
            },
            backend::BeadsRsCliBackend,
        );
        let read = ctx.read_consistency();
        assert_eq!(read.require_min_seen, Some(watermarks));
        assert_eq!(read.wait_timeout_ms, Some(50));
    }
}
