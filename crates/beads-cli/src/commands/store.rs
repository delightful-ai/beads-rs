use std::io::Write;

use clap::{Args, Subcommand};

use beads_api::{
    AdminFsckOutput, AdminStoreLockInfoOutput, AdminStoreUnlockOutput, FsckStatus,
    StoreLockMetaOutput,
};
use beads_core::StoreId;

use super::CommandError;
use crate::backend::{CliHostBackend, StoreFsckRequest, StoreUnlockRequest};
use crate::render::print_json;

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

pub fn handle<B>(json: bool, cmd: StoreCmd, backend: &B) -> std::result::Result<(), B::Error>
where
    B: CliHostBackend,
    B::Error: From<CommandError>,
{
    match cmd {
        StoreCmd::Unlock(args) => handle_unlock(json, args, backend),
        StoreCmd::Fsck(args) => handle_fsck(json, args, backend),
    }
}

fn handle_fsck<B>(json: bool, args: StoreFsckArgs, backend: &B) -> std::result::Result<(), B::Error>
where
    B: CliHostBackend,
    B::Error: From<CommandError>,
{
    let store_id = StoreId::parse_str(&args.store_id)
        .map_err(|err| B::Error::from(CommandError::from(err)))?;
    let output = backend.run_store_fsck(StoreFsckRequest {
        store_id,
        repair: args.repair,
    })?;

    if json {
        return print_json(&output).map_err(|err| B::Error::from(CommandError::from(err)));
    }

    write_stdout(&render_admin_fsck(&output))
        .map_err(|err| B::Error::from(CommandError::from(err)))?;
    Ok(())
}

pub fn render_admin_fsck(output: &AdminFsckOutput) -> String {
    let mut out = String::new();
    out.push_str("Store fsck:\n");
    out.push_str(&format!("  store_id: {}\n", output.store_id));
    out.push_str(&format!("  checked_at_ms: {}\n", output.checked_at_ms));
    out.push_str(&format!(
        "  stats: namespaces={} segments={} records={}\n",
        output.stats.namespaces, output.stats.segments, output.stats.records
    ));
    out.push_str(&format!(
        "  summary: risk={} safe_to_accept_writes={} safe_to_prune_wal={} safe_to_rebuild_index={}\n",
        output.summary.risk,
        output.summary.safe_to_accept_writes,
        output.summary.safe_to_prune_wal,
        output.summary.safe_to_rebuild_index
    ));

    if !output.repairs.is_empty() {
        out.push_str("  repairs:\n");
        for repair in &output.repairs {
            let path = repair
                .path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "none".to_string());
            out.push_str(&format!(
                "    - {}: {} ({path})\n",
                repair.kind, repair.detail
            ));
        }
    }

    out.push_str("  checks:\n");
    for check in &output.checks {
        out.push_str(&format!(
            "    - {}: {} (severity={}, issues={})\n",
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
                    "      * {}: {}{path}{namespace}{origin}{seq}{offset}\n",
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

fn handle_unlock<B>(
    json: bool,
    args: StoreUnlockArgs,
    backend: &B,
) -> std::result::Result<(), B::Error>
where
    B: CliHostBackend,
    B::Error: From<CommandError>,
{
    let store_id = StoreId::parse_str(&args.store_id)
        .map_err(|err| B::Error::from(CommandError::from(err)))?;
    let output = backend.run_store_unlock(StoreUnlockRequest {
        store_id,
        force: args.force,
    })?;

    if json {
        return print_json(&output).map_err(|err| B::Error::from(CommandError::from(err)));
    }
    write_stdout(&render_admin_store_unlock(&output))
        .map_err(|err| B::Error::from(CommandError::from(err)))?;
    Ok(())
}

pub fn render_admin_store_unlock(output: &AdminStoreUnlockOutput) -> String {
    let mut out = String::new();
    out.push_str("Store lock:\n");
    out.push_str(&format!("  store_id: {}\n", output.store_id));
    out.push_str(&format!("  lock_path: {}\n", output.lock_path.display()));
    render_lock_meta_output(&mut out, output.meta.as_ref());
    if let Some(daemon_pid) = output.daemon_pid {
        out.push_str(&format!("  daemon_pid: {daemon_pid}\n"));
    }
    out.push_str(&format!("  action: {}\n", output.action));
    out
}

pub fn render_admin_store_lock_info(output: &AdminStoreLockInfoOutput) -> String {
    let mut out = String::new();
    out.push_str("Store lock:\n");
    out.push_str(&format!("  store_id: {}\n", output.store_id));
    out.push_str(&format!("  lock_path: {}\n", output.lock_path.display()));
    render_lock_meta_output(&mut out, output.meta.as_ref());
    out
}

fn render_lock_meta_output(out: &mut String, meta: Option<&StoreLockMetaOutput>) {
    if let Some(meta) = meta {
        out.push_str(&format!("  replica_id: {}\n", meta.replica_id));
        out.push_str(&format!("  pid: {}\n", meta.pid));
        out.push_str(&format!("  started_at_ms: {}\n", meta.started_at_ms));
        out.push_str(&format!("  daemon_version: {}\n", meta.daemon_version));
        if let Some(heartbeat) = meta.last_heartbeat_ms {
            out.push_str(&format!("  last_heartbeat_ms: {heartbeat}\n"));
        } else {
            out.push_str("  last_heartbeat_ms: none\n");
        }
    } else {
        out.push_str("  meta: none\n");
    }
}

fn write_stdout(text: &str) -> crate::Result<()> {
    let mut stdout = std::io::stdout().lock();
    if let Err(err) = writeln!(stdout, "{text}")
        && err.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(beads_surface::ipc::IpcError::from(err));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use beads_api::{
        FsckCheck, FsckCheckId, FsckEvidence, FsckEvidenceCode, FsckRepair, FsckRepairKind,
        FsckRisk, FsckSeverity, FsckStats, FsckSummary,
    };
    use beads_core::{NamespaceId, ReplicaId};
    use std::path::PathBuf;
    use uuid::Uuid;

    fn sample_fsck_output() -> AdminFsckOutput {
        let store_id = StoreId::new(Uuid::from_bytes([9u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([7u8; 16]));
        AdminFsckOutput {
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
        let output = render_admin_fsck(&sample_fsck_output());
        let expected = concat!(
            "Store fsck:\n",
            "  store_id: 09090909-0909-0909-0909-090909090909\n",
            "  checked_at_ms: 12345\n",
            "  stats: namespaces=1 segments=2 records=3\n",
            "  summary: risk=high safe_to_accept_writes=false safe_to_prune_wal=false safe_to_rebuild_index=true\n",
            "  repairs:\n",
            "    - truncate_tail: truncated tail corruption (/tmp/wal/segment-1)\n",
            "  checks:\n",
            "    - segment_frames: fail (severity=high, issues=1)\n",
            "      * frame_crc_mismatch: crc mismatch path=/tmp/wal/segment-1 namespace=core origin=07070707-0707-0707-0707-070707070707 seq=5 offset=128\n",
            "      actions:\n",
            "        - run `bd store fsck --repair` to truncate tail corruption\n",
            "    - index_offsets: pass (severity=low, issues=0)\n",
        );
        assert_eq!(output, expected);
    }
}
