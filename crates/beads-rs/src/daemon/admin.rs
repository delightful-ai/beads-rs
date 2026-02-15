//! Admin / introspection handlers.

mod offline_store;
mod online;
mod policy_reload;
mod reporting;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use beads_core::StoreId;
use crossbeam::channel::Sender;

use crate::core::{
    Limits, NamespaceId, NamespacePolicies, NamespacePolicy, ReplicaId, WallClock, Watermarks,
};
use crate::daemon::fingerprint::{FingerprintError, FingerprintMode, fingerprint_namespaces};
use crate::daemon::metrics::{MetricHistogram, MetricLabel, MetricSample, MetricsSnapshot};
use crate::daemon::scrubber::{ScrubOptions, scrub_store};
use crate::daemon::store::runtime::{StoreRuntimeError, load_replica_roster};
use crate::daemon::wal::{ReplayStats, rebuild_index};
use crate::git::checkpoint::layout::SHARD_COUNT;
use crate::paths;
use beads_api::{
    AdminCheckpointGroup, AdminClockAnomaly, AdminClockAnomalyKind, AdminDoctorOutput,
    AdminFingerprintMode, AdminFingerprintOutput, AdminFingerprintSample, AdminFlushOutput,
    AdminFlushSegment, AdminMaintenanceModeOutput, AdminMetricHistogram, AdminMetricLabel,
    AdminMetricSample, AdminMetricsOutput, AdminPolicyChange, AdminPolicyDiff,
    AdminRebuildIndexOutput, AdminRebuildIndexStats, AdminRebuildIndexTruncation,
    AdminReloadLimitsOutput, AdminReloadPoliciesOutput, AdminReloadReplicationOutput,
    AdminReplicaLiveness, AdminReplicationNamespace, AdminReplicationPeer,
    AdminRotateReplicaIdOutput, AdminScrubOutput, AdminStatusOutput, AdminWalGrowth,
    AdminWalNamespace, AdminWalSegment, AdminWalWarning, AdminWalWarningKind,
};
use beads_api::{
    AdminFsckOutput, AdminStoreLockInfoOutput, AdminStoreUnlockOutput, FsckCheck, FsckCheckId,
    FsckEvidence, FsckEvidenceCode, FsckRepair, FsckRepairKind, FsckRisk, FsckSeverity, FsckStats,
    FsckStatus, FsckSummary, StoreLockMetaOutput, UnlockAction,
};
use beads_daemon::clock::{ClockAnomaly, ClockAnomalyKind};

use super::core::Daemon;
use super::ipc::{ReadConsistency, ResponseExt};
use super::{GitOp, OpError, QueryResult, Response, ResponsePayload};

pub(crate) use offline_store::{offline_store_fsck_output, offline_store_unlock_output};

pub(crate) fn fsck_report_to_output(
    report: crate::daemon::wal::fsck::FsckReport,
) -> AdminFsckOutput {
    AdminFsckOutput {
        store_id: report.store_id,
        checked_at_ms: report.checked_at_ms,
        stats: FsckStats {
            namespaces: report.stats.namespaces,
            segments: report.stats.segments,
            records: report.stats.records,
        },
        checks: report.checks.into_iter().map(fsck_check_to_api).collect(),
        summary: FsckSummary {
            risk: fsck_risk_to_api(report.summary.risk),
            safe_to_accept_writes: report.summary.safe_to_accept_writes,
            safe_to_prune_wal: report.summary.safe_to_prune_wal,
            safe_to_rebuild_index: report.summary.safe_to_rebuild_index,
        },
        repairs: report.repairs.into_iter().map(fsck_repair_to_api).collect(),
    }
}

fn fsck_check_to_api(check: crate::daemon::wal::fsck::FsckCheck) -> FsckCheck {
    FsckCheck {
        id: fsck_check_id_to_api(check.id),
        status: fsck_status_to_api(check.status),
        severity: fsck_severity_to_api(check.severity),
        evidence: check
            .evidence
            .into_iter()
            .map(fsck_evidence_to_api)
            .collect(),
        suggested_actions: check.suggested_actions,
    }
}

fn fsck_evidence_to_api(e: crate::daemon::wal::fsck::FsckEvidence) -> FsckEvidence {
    FsckEvidence {
        code: fsck_evidence_code_to_api(e.code),
        message: e.message,
        path: e.path,
        namespace: e.namespace,
        origin: e.origin,
        seq: e.seq,
        offset: e.offset,
    }
}

fn fsck_repair_to_api(r: crate::daemon::wal::fsck::FsckRepair) -> FsckRepair {
    FsckRepair {
        kind: match r.kind {
            crate::daemon::wal::fsck::FsckRepairKind::TruncateTail => FsckRepairKind::TruncateTail,
            crate::daemon::wal::fsck::FsckRepairKind::QuarantineSegment => {
                FsckRepairKind::QuarantineSegment
            }
            crate::daemon::wal::fsck::FsckRepairKind::RebuildIndex => FsckRepairKind::RebuildIndex,
        },
        path: r.path,
        detail: r.detail,
    }
}

fn fsck_status_to_api(s: crate::daemon::wal::fsck::FsckStatus) -> FsckStatus {
    match s {
        crate::daemon::wal::fsck::FsckStatus::Pass => FsckStatus::Pass,
        crate::daemon::wal::fsck::FsckStatus::Warn => FsckStatus::Warn,
        crate::daemon::wal::fsck::FsckStatus::Fail => FsckStatus::Fail,
    }
}

fn fsck_severity_to_api(s: crate::daemon::wal::fsck::FsckSeverity) -> FsckSeverity {
    match s {
        crate::daemon::wal::fsck::FsckSeverity::Low => FsckSeverity::Low,
        crate::daemon::wal::fsck::FsckSeverity::Medium => FsckSeverity::Medium,
        crate::daemon::wal::fsck::FsckSeverity::High => FsckSeverity::High,
        crate::daemon::wal::fsck::FsckSeverity::Critical => FsckSeverity::Critical,
    }
}

fn fsck_risk_to_api(r: crate::daemon::wal::fsck::FsckRisk) -> FsckRisk {
    match r {
        crate::daemon::wal::fsck::FsckRisk::Low => FsckRisk::Low,
        crate::daemon::wal::fsck::FsckRisk::Medium => FsckRisk::Medium,
        crate::daemon::wal::fsck::FsckRisk::High => FsckRisk::High,
        crate::daemon::wal::fsck::FsckRisk::Critical => FsckRisk::Critical,
    }
}

fn fsck_check_id_to_api(id: crate::daemon::wal::fsck::FsckCheckId) -> FsckCheckId {
    match id {
        crate::daemon::wal::fsck::FsckCheckId::SegmentHeaders => FsckCheckId::SegmentHeaders,
        crate::daemon::wal::fsck::FsckCheckId::SegmentFrames => FsckCheckId::SegmentFrames,
        crate::daemon::wal::fsck::FsckCheckId::RecordHashes => FsckCheckId::RecordHashes,
        crate::daemon::wal::fsck::FsckCheckId::OriginContiguity => FsckCheckId::OriginContiguity,
        crate::daemon::wal::fsck::FsckCheckId::IndexOffsets => FsckCheckId::IndexOffsets,
        crate::daemon::wal::fsck::FsckCheckId::CheckpointCache => FsckCheckId::CheckpointCache,
    }
}

fn fsck_evidence_code_to_api(c: crate::daemon::wal::fsck::FsckEvidenceCode) -> FsckEvidenceCode {
    match c {
        crate::daemon::wal::fsck::FsckEvidenceCode::SegmentHeaderInvalid => {
            FsckEvidenceCode::SegmentHeaderInvalid
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::SegmentHeaderMismatch => {
            FsckEvidenceCode::SegmentHeaderMismatch
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::SegmentHeaderSymlink => {
            FsckEvidenceCode::SegmentHeaderSymlink
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::FrameHeaderInvalid => {
            FsckEvidenceCode::FrameHeaderInvalid
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::FrameCrcMismatch => {
            FsckEvidenceCode::FrameCrcMismatch
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::FrameTruncated => {
            FsckEvidenceCode::FrameTruncated
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::RecordDecodeInvalid => {
            FsckEvidenceCode::RecordDecodeInvalid
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::RecordHeaderMismatch => {
            FsckEvidenceCode::RecordHeaderMismatch
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::RecordShaMismatch => {
            FsckEvidenceCode::RecordShaMismatch
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::PrevShaMismatch => {
            FsckEvidenceCode::PrevShaMismatch
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::NonContiguousSeq => {
            FsckEvidenceCode::NonContiguousSeq
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::SealedSegmentLenMismatch => {
            FsckEvidenceCode::SealedSegmentLenMismatch
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::IndexOffsetOutOfBounds => {
            FsckEvidenceCode::IndexOffsetOutOfBounds
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::IndexMissingSegment => {
            FsckEvidenceCode::IndexMissingSegment
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::IndexBehindWal => {
            FsckEvidenceCode::IndexBehindWal
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::IndexOpenFailed => {
            FsckEvidenceCode::IndexOpenFailed
        }
        crate::daemon::wal::fsck::FsckEvidenceCode::CheckpointCacheInvalid => {
            FsckEvidenceCode::CheckpointCacheInvalid
        }
    }
}

#[cfg(test)]
mod tests {
    use super::offline_store::{
        OfflinePidState, offline_store_lock_info_output, offline_store_unlock_with_pid_check,
    };
    use super::reporting::{WalSegmentStats, build_wal_growth, wal_guardrail_warnings};
    use crate::core::{Limits, NamespaceId, ReplicaId, StoreId};
    use crate::daemon::OpError;
    use crate::paths;
    use beads_api::{AdminWalWarningKind, UnlockAction};
    use std::path::Path;
    use tempfile::TempDir;
    use uuid::Uuid;

    #[test]
    fn wal_guardrails_warn_on_limits() {
        let namespace = NamespaceId::core();
        let now_ms = 10_000;
        let limits = Limits {
            wal_guardrail_max_bytes: 100,
            wal_guardrail_max_segments: 2,
            wal_guardrail_growth_window_ms: 1_000,
            wal_guardrail_growth_max_bytes: 50,
            ..Limits::default()
        };

        let segment_stats = vec![
            WalSegmentStats {
                created_at_ms: now_ms - 200,
                bytes: 60,
            },
            WalSegmentStats {
                created_at_ms: now_ms - 100,
                bytes: 40,
            },
            WalSegmentStats {
                created_at_ms: now_ms - 5_000,
                bytes: 50,
            },
        ];
        let growth = build_wal_growth(
            &segment_stats,
            limits.wal_guardrail_growth_window_ms,
            now_ms,
        );
        let warnings = wal_guardrail_warnings(
            &namespace,
            150,
            segment_stats.len() as u64,
            &growth,
            &limits,
        );

        assert!(
            warnings
                .iter()
                .any(|warning| warning.kind == AdminWalWarningKind::TotalBytesExceeded)
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.kind == AdminWalWarningKind::SegmentCountExceeded)
        );
        let growth_warning = warnings
            .iter()
            .find(|warning| warning.kind == AdminWalWarningKind::GrowthBytesExceeded)
            .expect("growth warning");
        assert_eq!(
            growth_warning.window_ms,
            Some(limits.wal_guardrail_growth_window_ms)
        );
    }

    #[test]
    fn offline_lock_info_returns_none_when_missing() {
        with_test_data_dir(|_| {
            let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
            let output = offline_store_lock_info_output(store_id).unwrap();
            assert_eq!(output.store_id, store_id);
            assert!(output.meta.is_none());
        });
    }

    #[test]
    fn offline_unlock_removes_stale_lock_file() {
        with_test_data_dir(|_| {
            let store_id = StoreId::new(Uuid::from_bytes([2u8; 16]));
            let lock_path = paths::store_lock_path(store_id);
            let meta = crate::daemon::store::lock::StoreLockMeta {
                store_id,
                replica_id: ReplicaId::new(Uuid::from_bytes([3u8; 16])),
                pid: 4242,
                started_at_ms: 1,
                daemon_version: "test".to_string(),
                last_heartbeat_ms: Some(2),
            };
            write_lock_meta(&lock_path, &meta);

            let output = offline_store_unlock_with_pid_check(store_id, false, None, |_| {
                OfflinePidState::Missing
            })
            .unwrap();
            assert_eq!(output.action, UnlockAction::RemovedStale);
            assert!(!lock_path.exists());
        });
    }

    #[test]
    fn offline_unlock_requires_force_for_live_daemon() {
        with_test_data_dir(|_| {
            let store_id = StoreId::new(Uuid::from_bytes([4u8; 16]));
            let lock_path = paths::store_lock_path(store_id);
            let meta = crate::daemon::store::lock::StoreLockMeta {
                store_id,
                replica_id: ReplicaId::new(Uuid::from_bytes([5u8; 16])),
                pid: 5151,
                started_at_ms: 1,
                daemon_version: "test".to_string(),
                last_heartbeat_ms: Some(2),
            };
            write_lock_meta(&lock_path, &meta);

            let err = offline_store_unlock_with_pid_check(store_id, false, Some(5151), |_| {
                OfflinePidState::Alive
            })
            .unwrap_err();
            match err {
                OpError::InvalidRequest { field, reason } => {
                    assert_eq!(field.as_deref(), Some("force"));
                    assert!(reason.contains("live_daemon"));
                }
                other => panic!("unexpected error: {other}"),
            }
            assert!(lock_path.exists());
        });
    }

    fn with_test_data_dir<F>(f: F)
    where
        F: FnOnce(&TempDir),
    {
        let temp = TempDir::new().unwrap();
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));
        f(&temp);
    }

    fn write_lock_meta(path: &Path, meta: &crate::daemon::store::lock::StoreLockMeta) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let data = serde_json::to_vec(meta).unwrap();
        std::fs::write(path, data).unwrap();
    }
}
