//! CLI surface for beads-rs.
//!
//! `beads-rs` keeps host-specific orchestration (repo/config/daemon hooks) while
//! `beads-cli` owns top-level parse and dispatch.

use std::path::{Path, PathBuf};

use crate::config::{Config, apply_env_overrides, load_for_repo};
use crate::core::{ActorId, Applied, DurabilityClass, NamespaceId, Watermarks};
use crate::{Error, Result};
use beads_cli::cli::{CliHost, RuntimeBuildArgs};
use beads_cli::runtime::{CliRuntimeCtx, validate_actor_id as cli_validate_actor_id};
use beads_cli::validation::{normalize_optional_client_request_id, normalize_optional_namespace};

pub use beads_cli::cli::{Cli, Command, command_name, parse_from};
pub use beads_cli::commands::daemon::DaemonCmd;

mod backend;

#[derive(Debug, Clone, Copy, Default)]
struct BeadsRsCliHost;

impl CliHost for BeadsRsCliHost {
    type Error = Error;

    fn maybe_spawn_auto_upgrade(&self) {
        crate::upgrade::maybe_spawn_auto_upgrade();
    }

    fn run_daemon_command(&self) -> Result<()> {
        crate::run_daemon_command()
    }

    fn build_runtime(&self, args: RuntimeBuildArgs) -> Result<CliRuntimeCtx> {
        let repo = resolve_repo(args.repo)?;
        let config = load_cli_config(&repo);
        Ok(CliRuntimeCtx {
            repo,
            json: args.json,
            namespace: resolve_namespace(args.namespace.as_deref(), &config)?,
            durability: resolve_durability(args.durability.as_deref(), &config)?,
            client_request_id: normalize_optional_client_request_id(
                args.client_request_id.as_deref(),
            )?,
            require_min_seen: parse_require_min_seen(args.require_min_seen.as_deref())?,
            wait_timeout_ms: args.wait_timeout_ms,
            actor_id: resolve_actor_override(args.actor.as_deref())?,
        })
    }

    fn in_beads_repo(&self) -> bool {
        match crate::repo::discover() {
            Ok((repo, _)) => repo.refname_to_id("refs/heads/beads/store").is_ok(),
            Err(_) => false,
        }
    }

    fn discover_repo(&self) -> Option<PathBuf> {
        crate::repo::discover().ok().map(|(_, path)| path)
    }
}

/// Run the CLI (used by bin).
pub fn run(cli: Cli) -> Result<()> {
    let backend = backend::BeadsRsCliBackend;
    let host = BeadsRsCliHost;
    beads_cli::cli::run(cli, &host, &backend)
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
        let cwd = std::env::current_dir().map_err(|err| {
            Error::Ipc(beads_surface::IpcError::DaemonUnavailable(format!(
                "failed to get cwd: {err}"
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OpError;
    use crate::config::DefaultsConfig;
    use crate::core::{HeadStatus, ReplicaId, Seq0};
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
        let ctx = CliRuntimeCtx {
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
        let ctx = CliRuntimeCtx {
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
