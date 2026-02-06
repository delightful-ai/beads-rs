use std::io::Write;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use serde::Serialize;

use crate::api::QueryResult;
use crate::core::StoreId;
use crate::OpError;
use crate::daemon::ipc::{Request, Response, ResponsePayload, send_request_no_autostart};
use crate::daemon::store_lock::{StoreLockError, StoreLockMeta, read_lock_meta, remove_lock_file};
use crate::daemon::store_runtime::StoreRuntimeError;
use crate::daemon::wal::fsck::{FsckOptions, FsckReport, FsckStatus, fsck_store};
use crate::paths;
use crate::{Error, Result};

use super::super::print_json;

#[derive(Subcommand, Debug)]
pub enum StoreCmd {
    /// Unlock a store lock file.
    Unlock(StoreUnlockArgs),
    /// Offline WAL verification and repair.
    Fsck(StoreFsckArgs),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PidState {
    Missing,
    Alive,
    Unknown,
}

crate::enum_str! {
    impl PidState {
        fn as_str(&self) -> &'static str;
        fn parse_str(raw: &str) -> Option<Self>;
        variants {
            Missing => ["missing"],
            Alive => ["alive"],
            Unknown => ["unknown"],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PidCheck {
    state: PidState,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum UnlockReason {
    PidMissing,
    PidReuse,
    LiveDaemon,
    PidUnknown,
}

impl UnlockReason {
    fn as_str(self) -> &'static str {
        match self {
            UnlockReason::PidMissing => "pid_missing",
            UnlockReason::PidReuse => "pid_reuse",
            UnlockReason::LiveDaemon => "live_daemon",
            UnlockReason::PidUnknown => "pid_unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum StoreUnlockAction {
    NoLock,
    Removed {
        forced: bool,
        reason: UnlockReason,
        removed: bool,
    },
    RequireForce {
        reason: UnlockReason,
    },
}

impl StoreUnlockAction {
    fn describe(&self) -> String {
        match self {
            StoreUnlockAction::NoLock => "no lock file found".to_string(),
            StoreUnlockAction::Removed {
                forced,
                reason,
                removed,
            } => {
                let mut out = if *removed {
                    "removed".to_string()
                } else {
                    "already removed".to_string()
                };
                if *forced {
                    out.push_str(" (forced)");
                }
                out.push_str(&format!(" [reason={}]", reason.as_str()));
                out
            }
            StoreUnlockAction::RequireForce { reason } => {
                format!("requires --force [reason={}]", reason.as_str())
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct StoreUnlockReport {
    store_id: StoreId,
    lock_path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    meta: Option<StoreLockMeta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid_state: Option<PidState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    daemon_pid: Option<u32>,
    result: StoreUnlockAction,
}

pub(crate) fn handle(json: bool, cmd: StoreCmd) -> Result<()> {
    match cmd {
        StoreCmd::Unlock(args) => handle_unlock(json, args),
        StoreCmd::Fsck(args) => handle_fsck(json, args),
    }
}

fn handle_unlock(json: bool, args: StoreUnlockArgs) -> Result<()> {
    let store_id = StoreId::parse_str(&args.store_id)?;
    let daemon_pid = daemon_pid();
    let report = unlock_store_with(store_id, args.force, daemon_pid, pid_check)?;
    write_report(&report, json)?;

    if let StoreUnlockAction::RequireForce { reason } = report.result {
        return Err(Error::Op(OpError::InvalidRequest {
            field: Some("force".into()),
            reason: format!("lock appears active ({})", reason.as_str()),
        }));
    }
    Ok(())
}

fn handle_fsck(json: bool, args: StoreFsckArgs) -> Result<()> {
    let store_id = StoreId::parse_str(&args.store_id)?;
    let config = crate::config::load_or_init();
    let options = FsckOptions::new(args.repair, config.limits);
    let report = fsck_store(store_id, options).map_err(|err| {
        Error::Op(OpError::InvalidRequest {
            field: Some("store-id".into()),
            reason: err.to_string(),
        })
    })?;
    write_fsck_report(&report, json)?;
    Ok(())
}

fn unlock_store_with<F>(
    store_id: StoreId,
    force: bool,
    daemon_pid: Option<u32>,
    pid_check: F,
) -> Result<StoreUnlockReport>
where
    F: FnOnce(u32) -> PidCheck,
{
    let lock_path = paths::store_lock_path(store_id);
    let meta = read_lock_meta(store_id).map_err(store_lock_error)?;
    let Some(meta) = meta else {
        return Ok(StoreUnlockReport {
            store_id,
            lock_path,
            meta: None,
            pid_state: None,
            pid_error: None,
            daemon_pid,
            result: StoreUnlockAction::NoLock,
        });
    };

    let check = pid_check(meta.pid);
    let mut result = decide_unlock(check.state, daemon_pid, meta.pid, force);
    if let StoreUnlockAction::Removed { removed, .. } = &mut result {
        let removed_flag = remove_lock_file(store_id).map_err(store_lock_error)?;
        *removed = removed_flag;
        tracing::info!(
            store_id = %store_id,
            lock_path = %lock_path.display(),
            pid = meta.pid,
            forced = matches!(&result, StoreUnlockAction::Removed { forced: true, .. }),
            reason = %result.describe(),
            "store lock removed"
        );
    }

    Ok(StoreUnlockReport {
        store_id,
        lock_path,
        meta: Some(meta),
        pid_state: Some(check.state),
        pid_error: check.error,
        daemon_pid,
        result,
    })
}

fn decide_unlock(
    pid_state: PidState,
    daemon_pid: Option<u32>,
    lock_pid: u32,
    force: bool,
) -> StoreUnlockAction {
    match pid_state {
        PidState::Missing => StoreUnlockAction::Removed {
            forced: false,
            reason: UnlockReason::PidMissing,
            removed: false,
        },
        PidState::Alive => {
            if daemon_pid == Some(lock_pid) {
                if force {
                    StoreUnlockAction::Removed {
                        forced: true,
                        reason: UnlockReason::LiveDaemon,
                        removed: false,
                    }
                } else {
                    StoreUnlockAction::RequireForce {
                        reason: UnlockReason::LiveDaemon,
                    }
                }
            } else {
                StoreUnlockAction::Removed {
                    forced: false,
                    reason: UnlockReason::PidReuse,
                    removed: false,
                }
            }
        }
        PidState::Unknown => {
            if force {
                StoreUnlockAction::Removed {
                    forced: true,
                    reason: UnlockReason::PidUnknown,
                    removed: false,
                }
            } else {
                StoreUnlockAction::RequireForce {
                    reason: UnlockReason::PidUnknown,
                }
            }
        }
    }
}

fn daemon_pid() -> Option<u32> {
    let resp = send_request_no_autostart(&Request::Ping).ok()?;
    match resp {
        Response::Ok {
            ok: ResponsePayload::Query(QueryResult::DaemonInfo(info)),
        } => Some(info.pid),
        _ => None,
    }
}

fn pid_check(pid: u32) -> PidCheck {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let nix_pid = Pid::from_raw(pid as i32);
    match kill(nix_pid, None) {
        Ok(()) => PidCheck {
            state: PidState::Alive,
            error: None,
        },
        Err(Errno::ESRCH) => PidCheck {
            state: PidState::Missing,
            error: None,
        },
        Err(Errno::EPERM) => PidCheck {
            state: PidState::Alive,
            error: None,
        },
        Err(e) => PidCheck {
            state: PidState::Unknown,
            error: Some(e.to_string()),
        },
    }
}

fn write_report(report: &StoreUnlockReport, json: bool) -> Result<()> {
    if json {
        return print_json(report);
    }
    let output = render_human(report);

    let mut stdout = std::io::stdout().lock();
    if let Err(e) = writeln!(stdout, "{output}")
        && e.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(crate::daemon::IpcError::from(e).into());
    }
    Ok(())
}

fn write_fsck_report(report: &FsckReport, json: bool) -> Result<()> {
    if json {
        return print_json(report);
    }
    let output = render_fsck_human(report);

    let mut stdout = std::io::stdout().lock();
    if let Err(e) = writeln!(stdout, "{output}")
        && e.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(crate::daemon::IpcError::from(e).into());
    }
    Ok(())
}

fn render_human(report: &StoreUnlockReport) -> String {
    let mut out = String::new();
    out.push_str("Store lock:\n");
    out.push_str(&format!("  store_id: {}\n", report.store_id));
    out.push_str(&format!("  lock_path: {}\n", report.lock_path.display()));

    if let Some(meta) = &report.meta {
        out.push_str(&format!("  replica_id: {}\n", meta.replica_id));
        out.push_str(&format!("  pid: {}\n", meta.pid));
        out.push_str(&format!("  started_at_ms: {}\n", meta.started_at_ms));
        out.push_str(&format!("  daemon_version: {}\n", meta.daemon_version));
        if let Some(heartbeat) = meta.last_heartbeat_ms {
            out.push_str(&format!("  last_heartbeat_ms: {}\n", heartbeat));
        } else {
            out.push_str("  last_heartbeat_ms: none\n");
        }
    } else {
        out.push_str("  meta: none\n");
    }

    if let Some(pid_state) = report.pid_state {
        out.push_str(&format!("  pid_state: {}\n", pid_state.as_str()));
    }
    if let Some(pid_error) = &report.pid_error {
        out.push_str(&format!("  pid_error: {}\n", pid_error));
    }
    if let Some(daemon_pid) = report.daemon_pid {
        out.push_str(&format!("  daemon_pid: {}\n", daemon_pid));
    }
    out.push_str(&format!("  action: {}\n", report.result.describe()));
    out
}

fn render_fsck_human(report: &FsckReport) -> String {
    let mut out = String::new();
    out.push_str("Store fsck:\n");
    out.push_str(&format!("  store_id: {}\n", report.store_id));
    out.push_str(&format!("  checked_at_ms: {}\n", report.checked_at_ms));
    out.push_str(&format!(
        "  stats: namespaces={} segments={} records={}\n",
        report.stats.namespaces, report.stats.segments, report.stats.records
    ));
    out.push_str(&format!(
        "  summary: risk={:?} safe_to_accept_writes={} safe_to_prune_wal={} safe_to_rebuild_index={}\n",
        report.summary.risk,
        report.summary.safe_to_accept_writes,
        report.summary.safe_to_prune_wal,
        report.summary.safe_to_rebuild_index
    ));

    if !report.repairs.is_empty() {
        out.push_str("  repairs:\n");
        for repair in &report.repairs {
            let path = repair
                .path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "none".to_string());
            out.push_str(&format!(
                "    - {:?}: {} ({path})\n",
                repair.kind, repair.detail
            ));
        }
    }

    out.push_str("  checks:\n");
    for check in &report.checks {
        out.push_str(&format!(
            "    - {:?}: {:?} (severity={:?}, issues={})\n",
            check.id,
            check.status,
            check.severity,
            check.evidence.len()
        ));
        if check.status != FsckStatus::Pass {
            for evidence in &check.evidence {
                let path = evidence
                    .path
                    .as_ref()
                    .map(|p| format!(" path={}", p.display()))
                    .unwrap_or_default();
                let namespace = evidence
                    .namespace
                    .as_ref()
                    .map(|ns| format!(" namespace={}", ns.as_str()))
                    .unwrap_or_default();
                let origin = evidence
                    .origin
                    .as_ref()
                    .map(|id| format!(" origin={id}"))
                    .unwrap_or_default();
                let seq = evidence
                    .seq
                    .map(|seq| format!(" seq={seq}"))
                    .unwrap_or_default();
                let offset = evidence
                    .offset
                    .map(|offset| format!(" offset={offset}"))
                    .unwrap_or_default();
                out.push_str(&format!(
                    "      * {:?}: {}{path}{namespace}{origin}{seq}{offset}\n",
                    evidence.code, evidence.message
                ));
            }
            if !check.suggested_actions.is_empty() {
                out.push_str("      actions:\n");
                for action in &check.suggested_actions {
                    out.push_str(&format!("        - {action}\n"));
                }
            }
        }
    }
    out
}

fn store_lock_error(err: StoreLockError) -> Error {
    Error::Op(OpError::from(StoreRuntimeError::Lock(err)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{NamespaceId, ReplicaId};
    use crate::daemon::wal::fsck::{
        FsckCheck, FsckCheckId, FsckEvidence, FsckEvidenceCode, FsckRepair, FsckRepairKind,
        FsckRisk, FsckSeverity, FsckStats, FsckSummary,
    };
    use std::path::Path;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use uuid::Uuid;

    #[test]
    fn unlock_decision_stale_pid_removes() {
        let action = decide_unlock(PidState::Missing, None, 42, false);
        assert!(matches!(
            action,
            StoreUnlockAction::Removed {
                forced: false,
                reason: UnlockReason::PidMissing,
                ..
            }
        ));
    }

    #[test]
    fn unlock_decision_live_daemon_requires_force() {
        let action = decide_unlock(PidState::Alive, Some(42), 42, false);
        assert!(matches!(
            action,
            StoreUnlockAction::RequireForce {
                reason: UnlockReason::LiveDaemon
            }
        ));
    }

    #[test]
    fn unlock_removes_stale_lock_file() {
        with_test_data_dir(|_temp| {
            let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
            let lock_path = paths::store_lock_path(store_id);
            let meta = StoreLockMeta {
                store_id,
                replica_id: crate::core::ReplicaId::new(Uuid::from_bytes([2u8; 16])),
                pid: 99_999,
                started_at_ms: 1,
                daemon_version: "test".to_string(),
                last_heartbeat_ms: Some(2),
            };
            write_lock_meta(&lock_path, &meta);
            let report = unlock_store_with(store_id, false, None, |_| PidCheck {
                state: PidState::Missing,
                error: None,
            })
            .unwrap();
            assert!(!lock_path.exists());
            assert!(matches!(report.result, StoreUnlockAction::Removed { .. }));
        });
    }

    #[test]
    fn unlock_requires_force_for_live_daemon() {
        with_test_data_dir(|_temp| {
            let store_id = StoreId::new(Uuid::from_bytes([3u8; 16]));
            let lock_path = paths::store_lock_path(store_id);
            let meta = StoreLockMeta {
                store_id,
                replica_id: crate::core::ReplicaId::new(Uuid::from_bytes([4u8; 16])),
                pid: 4242,
                started_at_ms: 1,
                daemon_version: "test".to_string(),
                last_heartbeat_ms: Some(2),
            };
            write_lock_meta(&lock_path, &meta);
            let report = unlock_store_with(store_id, false, Some(4242), |_| PidCheck {
                state: PidState::Alive,
                error: None,
            })
            .unwrap();
            assert!(lock_path.exists());
            assert!(matches!(
                report.result,
                StoreUnlockAction::RequireForce {
                    reason: UnlockReason::LiveDaemon
                }
            ));
        });
    }

    fn sample_fsck_report() -> FsckReport {
        let store_id = StoreId::new(Uuid::from_bytes([9u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([7u8; 16]));
        FsckReport {
            store_id,
            checked_at_ms: 12345,
            stats: FsckStats {
                namespaces: 1,
                segments: 2,
                records: 3,
            },
            summary: FsckSummary {
                risk: FsckRisk::High,
                safe_to_accept_writes: false,
                safe_to_prune_wal: false,
                safe_to_rebuild_index: true,
            },
            repairs: vec![FsckRepair {
                kind: FsckRepairKind::TruncateTail,
                path: Some(PathBuf::from("/tmp/wal/segment-1")),
                detail: "truncated tail corruption".to_string(),
            }],
            checks: vec![
                FsckCheck {
                    id: FsckCheckId::SegmentFrames,
                    status: FsckStatus::Fail,
                    severity: FsckSeverity::High,
                    evidence: vec![FsckEvidence {
                        code: FsckEvidenceCode::FrameCrcMismatch,
                        message: "crc mismatch".to_string(),
                        path: Some(PathBuf::from("/tmp/wal/segment-1")),
                        namespace: Some(NamespaceId::core()),
                        origin: Some(replica_id),
                        seq: Some(5),
                        offset: Some(128),
                    }],
                    suggested_actions: vec![
                        "run `bd store fsck --repair` to truncate tail corruption".to_string(),
                    ],
                },
                FsckCheck {
                    id: FsckCheckId::IndexOffsets,
                    status: FsckStatus::Pass,
                    severity: FsckSeverity::Low,
                    evidence: Vec::new(),
                    suggested_actions: Vec::new(),
                },
            ],
        }
    }

    #[test]
    fn render_fsck_human_golden() {
        let report = sample_fsck_report();
        let output = render_fsck_human(&report);
        let expected = concat!(
            "Store fsck:\n",
            "  store_id: 09090909-0909-0909-0909-090909090909\n",
            "  checked_at_ms: 12345\n",
            "  stats: namespaces=1 segments=2 records=3\n",
            "  summary: risk=High safe_to_accept_writes=false safe_to_prune_wal=false safe_to_rebuild_index=true\n",
            "  repairs:\n",
            "    - TruncateTail: truncated tail corruption (/tmp/wal/segment-1)\n",
            "  checks:\n",
            "    - SegmentFrames: Fail (severity=High, issues=1)\n",
            "      * FrameCrcMismatch: crc mismatch path=/tmp/wal/segment-1 namespace=core origin=07070707-0707-0707-0707-070707070707 seq=5 offset=128\n",
            "      actions:\n",
            "        - run `bd store fsck --repair` to truncate tail corruption\n",
            "    - IndexOffsets: Pass (severity=Low, issues=0)\n",
        );
        assert_eq!(output, expected);
    }

    fn with_test_data_dir<F>(f: F)
    where
        F: FnOnce(&TempDir),
    {
        let temp = TempDir::new().unwrap();
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));
        f(&temp);
    }

    fn write_lock_meta(path: &Path, meta: &StoreLockMeta) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let data = serde_json::to_vec(meta).unwrap();
        std::fs::write(path, data).unwrap();
    }
}
