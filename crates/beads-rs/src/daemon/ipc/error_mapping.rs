use super::{IpcError, Response};
use crate::core::error::details as error_details;
use crate::core::{CliErrorCode, ErrorCode, ErrorPayload, InvalidId, ProtocolErrorCode, StoreId};
use crate::daemon::ops::OpError;
use crate::daemon::store_lock::{StoreLockError, StoreLockOperation};
use crate::daemon::store_runtime::StoreRuntimeError;
use crate::daemon::wal::{EventWalError, WalIndexError, WalReplayError};
use crate::git::error::{SyncError, WireError};

// =============================================================================
// Error mapping
// =============================================================================

pub trait IntoErrorPayload {
    fn into_error_payload(self) -> ErrorPayload;
}

impl IntoErrorPayload for ErrorPayload {
    fn into_error_payload(self) -> ErrorPayload {
        self
    }
}

impl IntoErrorPayload for OpError {
    fn into_error_payload(self) -> ErrorPayload {
        let message = self.to_string();
        let retryable = self.transience().is_retryable();
        match self {
            OpError::NotFound(id) => {
                ErrorPayload::new(CliErrorCode::NotFound.into(), message, retryable)
                    .with_details(error_details::NotFoundDetails { id })
            }
            OpError::AlreadyExists(id) => {
                ErrorPayload::new(CliErrorCode::AlreadyExists.into(), message, retryable)
                    .with_details(error_details::AlreadyExistsDetails { id })
            }
            OpError::AlreadyClaimed { by, expires } => {
                let expires_at_ms = expires.map(|value| value.0);
                ErrorPayload::new(CliErrorCode::AlreadyClaimed.into(), message, retryable)
                    .with_details(error_details::AlreadyClaimedDetails { by, expires_at_ms })
            }
            OpError::CasMismatch { expected, actual } => {
                ErrorPayload::new(CliErrorCode::CasMismatch.into(), message, retryable)
                    .with_details(error_details::CasMismatchDetails { expected, actual })
            }
            OpError::InvalidTransition { from, to } => {
                ErrorPayload::new(CliErrorCode::InvalidTransition.into(), message, retryable)
                    .with_details(error_details::InvalidTransitionDetails { from, to })
            }
            OpError::ValidationFailed { field, reason } => {
                ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                    .with_details(error_details::ValidationFailedDetails { field, reason })
            }
            OpError::InvalidRequest { field, reason } => {
                ErrorPayload::new(ProtocolErrorCode::InvalidRequest.into(), message, retryable)
                    .with_details(error_details::InvalidRequestDetails {
                        field,
                        reason: Some(reason),
                    })
            }
            OpError::InvalidId(err) => {
                ErrorPayload::new(CliErrorCode::InvalidId.into(), message, retryable)
                    .with_details(invalid_id_details(&err))
            }
            OpError::Overloaded {
                subsystem,
                retry_after_ms,
                queue_bytes,
                queue_events,
            } => ErrorPayload::new(ProtocolErrorCode::Overloaded.into(), message, retryable)
                .with_details(error_details::OverloadedDetails {
                    subsystem: Some(subsystem),
                    retry_after_ms,
                    queue_bytes,
                    queue_events,
                }),
            OpError::RateLimited {
                retry_after_ms,
                limit_bytes_per_sec,
            } => ErrorPayload::new(ProtocolErrorCode::RateLimited.into(), message, retryable)
                .with_details(error_details::RateLimitedDetails {
                    retry_after_ms,
                    limit_bytes_per_sec,
                }),
            OpError::MaintenanceMode { reason } => ErrorPayload::new(
                ProtocolErrorCode::MaintenanceMode.into(),
                message,
                retryable,
            )
            .with_details(error_details::MaintenanceModeDetails {
                reason,
                until_ms: None,
            }),
            OpError::ClientRequestIdReuseMismatch {
                namespace,
                client_request_id,
                expected_request_sha256,
                got_request_sha256,
            } => ErrorPayload::new(
                ProtocolErrorCode::ClientRequestIdReuseMismatch.into(),
                message,
                retryable,
            )
            .with_details(error_details::ClientRequestIdReuseMismatchDetails {
                namespace,
                client_request_id,
                expected_request_sha256: hex::encode(expected_request_sha256.as_ref()),
                got_request_sha256: hex::encode(got_request_sha256.as_ref()),
            }),
            OpError::NotAGitRepo(path) => {
                ErrorPayload::new(CliErrorCode::NotAGitRepo.into(), message, retryable)
                    .with_details(error_details::PathDetails {
                        path: path.display().to_string(),
                    })
            }
            OpError::NoRemote(path) => {
                ErrorPayload::new(CliErrorCode::NoRemote.into(), message, retryable).with_details(
                    error_details::PathDetails {
                        path: path.display().to_string(),
                    },
                )
            }
            OpError::RepoNotInitialized(path) => {
                ErrorPayload::new(CliErrorCode::RepoNotInitialized.into(), message, retryable)
                    .with_details(error_details::PathDetails {
                        path: path.display().to_string(),
                    })
            }
            OpError::Sync(err) => match err.as_ref() {
                SyncError::Wire(WireError::ChecksumMismatch {
                    blob,
                    expected,
                    actual,
                }) => ErrorPayload::new(ProtocolErrorCode::Corruption.into(), message, retryable)
                    .with_details(error_details::StoreChecksumMismatchDetails {
                        blob: (*blob).to_string(),
                        expected_sha256: expected.to_hex(),
                        got_sha256: actual.to_hex(),
                    }),
                _ => ErrorPayload::new(CliErrorCode::SyncFailed.into(), message, retryable),
            },
            OpError::BeadDeleted(id) => {
                ErrorPayload::new(CliErrorCode::BeadDeleted.into(), message, retryable)
                    .with_details(error_details::BeadDeletedDetails { id })
            }
            OpError::NoteTooLarge {
                max_bytes,
                got_bytes,
            } => ErrorPayload::new(ProtocolErrorCode::NoteTooLarge.into(), message, retryable)
                .with_details(error_details::NoteTooLargeDetails {
                    max_note_bytes: max_bytes as u64,
                    got_bytes: got_bytes as u64,
                }),
            OpError::OpsTooMany { max_ops, got_ops } => {
                ErrorPayload::new(ProtocolErrorCode::OpsTooMany.into(), message, retryable)
                    .with_details(error_details::OpsTooManyDetails {
                        max_ops_per_txn: max_ops as u64,
                        got_ops: got_ops as u64,
                    })
            }
            OpError::LabelsTooMany {
                max_labels,
                got_labels,
                bead_id,
            } => ErrorPayload::new(ProtocolErrorCode::LabelsTooMany.into(), message, retryable)
                .with_details(error_details::LabelsTooManyDetails {
                    max_labels_per_bead: max_labels as u64,
                    got_labels: got_labels as u64,
                    bead_id: bead_id.as_ref().map(|id| id.as_str().to_string()),
                }),
            OpError::WalRecordTooLarge {
                max_wal_record_bytes,
                estimated_bytes,
            } => ErrorPayload::new(
                ProtocolErrorCode::WalRecordTooLarge.into(),
                message,
                retryable,
            )
            .with_details(error_details::WalRecordTooLargeDetails {
                max_wal_record_bytes: max_wal_record_bytes as u64,
                estimated_bytes: estimated_bytes as u64,
            }),
            OpError::DurabilityUnavailable {
                requested,
                eligible_total,
                eligible_replica_ids,
            } => ErrorPayload::new(
                ProtocolErrorCode::DurabilityUnavailable.into(),
                message,
                retryable,
            )
            .with_details(error_details::DurabilityUnavailableDetails {
                requested,
                eligible_total,
                eligible_replica_ids,
            }),
            OpError::DurabilityTimeout {
                requested,
                waited_ms,
                pending_replica_ids,
                receipt,
            } => ErrorPayload::new(
                ProtocolErrorCode::DurabilityTimeout.into(),
                message,
                retryable,
            )
            .with_details(error_details::DurabilityTimeoutDetails {
                requested,
                waited_ms,
                pending_replica_ids,
            })
            .with_receipt(receipt),
            OpError::RequireMinSeenTimeout {
                waited_ms,
                required,
                current_applied,
            } => ErrorPayload::new(
                ProtocolErrorCode::RequireMinSeenTimeout.into(),
                message,
                retryable,
            )
            .with_details(error_details::RequireMinSeenTimeoutDetails {
                waited_ms,
                required: required.as_ref().clone(),
                current_applied: current_applied.as_ref().clone(),
            }),
            OpError::RequireMinSeenUnsatisfied {
                required,
                current_applied,
            } => ErrorPayload::new(
                ProtocolErrorCode::RequireMinSeenUnsatisfied.into(),
                message,
                retryable,
            )
            .with_details(error_details::RequireMinSeenUnsatisfiedDetails {
                required: required.as_ref().clone(),
                current_applied: current_applied.as_ref().clone(),
            }),
            OpError::NamespaceInvalid { namespace, .. } => ErrorPayload::new(
                ProtocolErrorCode::NamespaceInvalid.into(),
                message,
                retryable,
            )
            .with_details(error_details::NamespaceInvalidDetails {
                namespace,
                pattern: "[a-z][a-z0-9_]{0,31}".to_string(),
            }),
            OpError::NamespaceUnknown { namespace } => ErrorPayload::new(
                ProtocolErrorCode::NamespaceUnknown.into(),
                message,
                retryable,
            )
            .with_details(error_details::NamespaceUnknownDetails { namespace }),
            OpError::NamespacePolicyViolation {
                namespace,
                rule,
                reason,
            } => ErrorPayload::new(
                ProtocolErrorCode::NamespacePolicyViolation.into(),
                message,
                retryable,
            )
            .with_details(error_details::NamespacePolicyViolationDetails {
                namespace,
                rule,
                reason,
            }),
            OpError::CrossNamespaceDependency {
                from_namespace,
                to_namespace,
            } => ErrorPayload::new(
                ProtocolErrorCode::CrossNamespaceDependency.into(),
                message,
                retryable,
            )
            .with_details(error_details::CrossNamespaceDependencyDetails {
                from_namespace,
                to_namespace,
            }),
            OpError::EventWal(e) => match e.as_ref() {
                EventWalError::RecordTooLarge {
                    max_bytes,
                    got_bytes,
                } => ErrorPayload::new(
                    ProtocolErrorCode::WalRecordTooLarge.into(),
                    message,
                    retryable,
                )
                .with_details(error_details::WalRecordTooLargeDetails {
                    max_wal_record_bytes: *max_bytes as u64,
                    estimated_bytes: *got_bytes as u64,
                }),
                EventWalError::Symlink { path } => ErrorPayload::new(
                    ProtocolErrorCode::PathSymlinkRejected.into(),
                    message,
                    retryable,
                )
                .with_details(error_details::PathSymlinkRejectedDetails {
                    path: path.display().to_string(),
                }),
                _ => ErrorPayload::new(event_wal_error_code(e.as_ref()), message, retryable)
                    .with_details(error_details::WalErrorDetails {
                        message: e.to_string(),
                    }),
            },
            OpError::NotClaimedByYou => {
                ErrorPayload::new(CliErrorCode::NotClaimedByYou.into(), message, retryable)
            }
            OpError::DepNotFound => {
                ErrorPayload::new(CliErrorCode::DepNotFound.into(), message, retryable)
            }
            OpError::LoadTimeout {
                repo,
                timeout_secs,
                remote,
            } => ErrorPayload::new(CliErrorCode::LoadTimeout.into(), message, retryable)
                .with_details(error_details::LoadTimeoutDetails {
                    repo: repo.display().to_string(),
                    timeout_secs,
                    remote,
                }),
            OpError::StoreRuntime(err) => {
                store_runtime_error_payload(err.as_ref(), message, retryable)
            }
            OpError::Internal(_) => {
                ErrorPayload::new(CliErrorCode::Internal.into(), message, retryable)
            }
        }
    }
}

impl IntoErrorPayload for IpcError {
    fn into_error_payload(self) -> ErrorPayload {
        let message = self.to_string();
        let retryable = self.transience().is_retryable();
        match self {
            IpcError::Parse(err) => ErrorPayload::new(
                ProtocolErrorCode::MalformedPayload.into(),
                message,
                retryable,
            )
            .with_details(error_details::MalformedPayloadDetails {
                parser: error_details::ParserKind::Json,
                reason: Some(err.to_string()),
            }),
            IpcError::InvalidRequest { field, reason } => {
                ErrorPayload::new(ProtocolErrorCode::InvalidRequest.into(), message, retryable)
                    .with_details(error_details::InvalidRequestDetails {
                        field,
                        reason: Some(reason),
                    })
            }
            IpcError::Io(_) => ErrorPayload::new(CliErrorCode::IoError.into(), message, retryable),
            IpcError::InvalidId(err) => {
                ErrorPayload::new(CliErrorCode::InvalidId.into(), message, retryable)
                    .with_details(invalid_id_details(&err))
            }
            IpcError::Disconnected => {
                ErrorPayload::new(CliErrorCode::Disconnected.into(), message, retryable)
            }
            IpcError::DaemonUnavailable(_) => {
                ErrorPayload::new(CliErrorCode::DaemonUnavailable.into(), message, retryable)
            }
            IpcError::DaemonVersionMismatch { .. } => ErrorPayload::new(
                CliErrorCode::DaemonVersionMismatch.into(),
                message,
                retryable,
            ),
            IpcError::FrameTooLarge {
                max_bytes,
                got_bytes,
            } => ErrorPayload::new(ProtocolErrorCode::FrameTooLarge.into(), message, retryable)
                .with_details(error_details::FrameTooLargeDetails {
                    max_frame_bytes: max_bytes as u64,
                    got_bytes: got_bytes as u64,
                }),
            _ => ErrorPayload::new(CliErrorCode::Internal.into(), message, retryable),
        }
    }
}

pub trait ResponseExt {
    fn err_from<E: IntoErrorPayload>(e: E) -> Response;
}

impl ResponseExt for Response {
    fn err_from<E: IntoErrorPayload>(e: E) -> Response {
        Response::err(e.into_error_payload())
    }
}

fn store_runtime_error_payload(
    err: &StoreRuntimeError,
    message: String,
    retryable: bool,
) -> ErrorPayload {
    match err {
        StoreRuntimeError::Lock(lock_err) => store_lock_error_payload(lock_err, message, retryable),
        StoreRuntimeError::MetaSymlink { path } => ErrorPayload::new(
            ProtocolErrorCode::PathSymlinkRejected.into(),
            message,
            retryable,
        )
        .with_details(error_details::PathSymlinkRejectedDetails {
            path: path.display().to_string(),
        }),
        StoreRuntimeError::NamespacePoliciesSymlink { path }
        | StoreRuntimeError::ReplicaRosterSymlink { path } => ErrorPayload::new(
            ProtocolErrorCode::PathSymlinkRejected.into(),
            message,
            retryable,
        )
        .with_details(error_details::PathSymlinkRejectedDetails {
            path: path.display().to_string(),
        }),
        StoreRuntimeError::MetaRead { path, source } => match source.kind() {
            std::io::ErrorKind::PermissionDenied => ErrorPayload::new(
                ProtocolErrorCode::PermissionDenied.into(),
                message,
                retryable,
            )
            .with_details(error_details::PermissionDeniedDetails {
                path: path.display().to_string(),
                operation: error_details::PermissionOperation::Read,
            }),
            _ => ErrorPayload::new(ProtocolErrorCode::InternalError.into(), message, retryable),
        },
        StoreRuntimeError::MetaParse { source, .. } => {
            ErrorPayload::new(ProtocolErrorCode::Corruption.into(), message, retryable)
                .with_details(error_details::CorruptionDetails {
                    reason: source.to_string(),
                })
        }
        StoreRuntimeError::MetaMismatch { expected, got } => {
            ErrorPayload::new(ProtocolErrorCode::WrongStore.into(), message, retryable)
                .with_details(error_details::WrongStoreDetails {
                    expected_store_id: *expected,
                    got_store_id: *got,
                })
        }
        StoreRuntimeError::UnsupportedStoreMetaVersion { expected, got } => ErrorPayload::new(
            ProtocolErrorCode::VersionIncompatible.into(),
            message,
            retryable,
        )
        .with_details(error_details::StoreMetaVersionMismatchDetails {
            expected: *expected,
            got: *got,
        }),
        StoreRuntimeError::MetaWrite { path, source } => match source.kind() {
            std::io::ErrorKind::PermissionDenied => ErrorPayload::new(
                ProtocolErrorCode::PermissionDenied.into(),
                message,
                retryable,
            )
            .with_details(error_details::PermissionDeniedDetails {
                path: path.display().to_string(),
                operation: error_details::PermissionOperation::Write,
            }),
            _ => ErrorPayload::new(ProtocolErrorCode::InternalError.into(), message, retryable),
        },
        StoreRuntimeError::NamespacePoliciesRead { path, source } => match source.kind() {
            std::io::ErrorKind::PermissionDenied => ErrorPayload::new(
                ProtocolErrorCode::PermissionDenied.into(),
                message,
                retryable,
            )
            .with_details(error_details::PermissionDeniedDetails {
                path: path.display().to_string(),
                operation: error_details::PermissionOperation::Read,
            }),
            _ => ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                .with_details(error_details::ValidationFailedDetails {
                    field: "namespaces".to_string(),
                    reason: format!("failed to read {}: {source}", path.display()),
                }),
        },
        StoreRuntimeError::NamespacePoliciesParse { source, .. } => {
            ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                .with_details(error_details::ValidationFailedDetails {
                    field: "namespaces".to_string(),
                    reason: source.to_string(),
                })
        }
        StoreRuntimeError::ReplicaRosterRead { path, source } => match source.kind() {
            std::io::ErrorKind::PermissionDenied => ErrorPayload::new(
                ProtocolErrorCode::PermissionDenied.into(),
                message,
                retryable,
            )
            .with_details(error_details::PermissionDeniedDetails {
                path: path.display().to_string(),
                operation: error_details::PermissionOperation::Read,
            }),
            _ => ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                .with_details(error_details::ValidationFailedDetails {
                    field: "replicas".to_string(),
                    reason: format!("failed to read {}: {source}", path.display()),
                }),
        },
        StoreRuntimeError::ReplicaRosterParse { source, .. } => {
            ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                .with_details(error_details::ValidationFailedDetails {
                    field: "replicas".to_string(),
                    reason: source.to_string(),
                })
        }
        StoreRuntimeError::StoreConfigSymlink { path } => ErrorPayload::new(
            ProtocolErrorCode::PathSymlinkRejected.into(),
            message,
            retryable,
        )
        .with_details(error_details::PathSymlinkRejectedDetails {
            path: path.display().to_string(),
        }),
        StoreRuntimeError::StoreConfigRead { path, source } => match source.kind() {
            std::io::ErrorKind::PermissionDenied => ErrorPayload::new(
                ProtocolErrorCode::PermissionDenied.into(),
                message,
                retryable,
            )
            .with_details(error_details::PermissionDeniedDetails {
                path: path.display().to_string(),
                operation: error_details::PermissionOperation::Read,
            }),
            _ => ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                .with_details(error_details::ValidationFailedDetails {
                    field: "store_config".to_string(),
                    reason: format!("failed to read {}: {source}", path.display()),
                }),
        },
        StoreRuntimeError::StoreConfigParse { source, .. } => {
            ErrorPayload::new(CliErrorCode::ValidationFailed.into(), message, retryable)
                .with_details(error_details::ValidationFailedDetails {
                    field: "store_config".to_string(),
                    reason: source.to_string(),
                })
        }
        StoreRuntimeError::StoreConfigSerialize { .. } => {
            ErrorPayload::new(ProtocolErrorCode::InternalError.into(), message, retryable)
        }
        StoreRuntimeError::StoreConfigWrite { path, source } => match source.kind() {
            std::io::ErrorKind::PermissionDenied => ErrorPayload::new(
                ProtocolErrorCode::PermissionDenied.into(),
                message,
                retryable,
            )
            .with_details(error_details::PermissionDeniedDetails {
                path: path.display().to_string(),
                operation: error_details::PermissionOperation::Write,
            }),
            _ => ErrorPayload::new(ProtocolErrorCode::InternalError.into(), message, retryable),
        },
        StoreRuntimeError::WatermarkInvalid {
            kind,
            namespace,
            origin,
            source,
        } => ErrorPayload::new(ProtocolErrorCode::IndexCorrupt.into(), message, retryable)
            .with_details(error_details::IndexCorruptDetails {
                reason: format!("{kind} watermark for {namespace} {origin}: {source}"),
            }),
        StoreRuntimeError::WalIndex(err) => wal_index_error_payload(err, message, retryable),
        StoreRuntimeError::WalReplay(err) => wal_replay_error_payload(err, message, retryable),
    }
}

fn wal_replay_error_payload(
    err: &WalReplayError,
    message: String,
    retryable: bool,
) -> ErrorPayload {
    match err {
        WalReplayError::Symlink { path } => ErrorPayload::new(
            ProtocolErrorCode::PathSymlinkRejected.into(),
            message,
            retryable,
        )
        .with_details(error_details::PathSymlinkRejectedDetails {
            path: path.display().to_string(),
        }),
        WalReplayError::RecordShaMismatch(info) | WalReplayError::RecordPayloadMismatch(info) => {
            let info = info.as_ref();
            ErrorPayload::new(ProtocolErrorCode::HashMismatch.into(), message, retryable)
                .with_details(error_details::HashMismatchDetails {
                    eid: error_details::EventIdDetails {
                        namespace: info.namespace.clone(),
                        origin_replica_id: info.origin,
                        origin_seq: info.seq.get(),
                    },
                    expected_sha256: hex::encode(info.expected),
                    got_sha256: hex::encode(info.got),
                })
        }
        WalReplayError::PrevShaMismatch {
            namespace,
            origin,
            seq,
            expected_prev_sha256,
            got_prev_sha256,
            head_seq,
        } => ErrorPayload::new(
            ProtocolErrorCode::PrevShaMismatch.into(),
            message,
            retryable,
        )
        .with_details(error_details::PrevShaMismatchDetails {
            eid: error_details::EventIdDetails {
                namespace: namespace.clone(),
                origin_replica_id: *origin,
                origin_seq: seq.get(),
            },
            expected_prev_sha256: hex::encode(expected_prev_sha256),
            got_prev_sha256: hex::encode(got_prev_sha256),
            head_seq: head_seq.get(),
        }),
        WalReplayError::NonContiguousSeq {
            namespace,
            origin,
            expected,
            got,
        } => {
            let durable_seen = expected.prev_seq0().get();
            ErrorPayload::new(ProtocolErrorCode::GapDetected.into(), message, retryable)
                .with_details(error_details::GapDetectedDetails {
                    namespace: namespace.clone(),
                    origin_replica_id: *origin,
                    durable_seen,
                    got_seq: got.get(),
                })
        }
        WalReplayError::IndexOffsetInvalid { .. } | WalReplayError::OriginSeqOverflow { .. } => {
            ErrorPayload::new(ProtocolErrorCode::IndexCorrupt.into(), message, retryable)
                .with_details(error_details::IndexCorruptDetails {
                    reason: err.to_string(),
                })
        }
        WalReplayError::SegmentHeader {
            source: EventWalError::SegmentHeaderUnsupportedVersion { got, supported },
            ..
        } => ErrorPayload::new(
            ProtocolErrorCode::WalFormatUnsupported.into(),
            message,
            retryable,
        )
        .with_details(error_details::WalFormatUnsupportedDetails {
            wal_format_version: *got,
            supported: vec![*supported],
        }),
        WalReplayError::SegmentHeader { .. } => {
            ErrorPayload::new(wal_replay_error_code(err), message, retryable)
        }
        WalReplayError::Index(err) => wal_index_error_payload(err, message, retryable),
        _ => ErrorPayload::new(wal_replay_error_code(err), message, retryable),
    }
}

fn wal_index_error_payload(err: &WalIndexError, message: String, retryable: bool) -> ErrorPayload {
    match err {
        WalIndexError::Symlink { path } => ErrorPayload::new(
            ProtocolErrorCode::PathSymlinkRejected.into(),
            message,
            retryable,
        )
        .with_details(error_details::PathSymlinkRejectedDetails {
            path: path.display().to_string(),
        }),
        WalIndexError::SchemaVersionMismatch { expected, got } => ErrorPayload::new(
            ProtocolErrorCode::IndexRebuildRequired.into(),
            message,
            retryable,
        )
        .with_details(error_details::IndexRebuildRequiredDetails {
            namespace: None,
            reason: format!("index schema version mismatch: expected {expected}, got {got}"),
        }),
        WalIndexError::Equivocation {
            namespace,
            origin,
            seq,
            existing_sha256,
            new_sha256,
        } => ErrorPayload::new(ProtocolErrorCode::Equivocation.into(), message, retryable)
            .with_details(error_details::EquivocationDetails {
                eid: error_details::EventIdDetails {
                    namespace: namespace.clone(),
                    origin_replica_id: *origin,
                    origin_seq: *seq,
                },
                existing_sha256: hex::encode(existing_sha256),
                new_sha256: hex::encode(new_sha256),
            }),
        WalIndexError::ClientRequestIdReuseMismatch {
            namespace,
            client_request_id,
            expected_request_sha256,
            got_request_sha256,
            ..
        } => ErrorPayload::new(
            ProtocolErrorCode::ClientRequestIdReuseMismatch.into(),
            message,
            retryable,
        )
        .with_details(error_details::ClientRequestIdReuseMismatchDetails {
            namespace: namespace.clone(),
            client_request_id: *client_request_id,
            expected_request_sha256: hex::encode(expected_request_sha256),
            got_request_sha256: hex::encode(got_request_sha256),
        }),
        WalIndexError::MetaMismatch {
            key: "store_id",
            expected,
            got,
            ..
        } => {
            let expected = StoreId::parse_str(expected).ok();
            let got = StoreId::parse_str(got).ok();
            if let (Some(expected), Some(got)) = (expected, got) {
                ErrorPayload::new(ProtocolErrorCode::WrongStore.into(), message, retryable)
                    .with_details(error_details::WrongStoreDetails {
                        expected_store_id: expected,
                        got_store_id: got,
                    })
            } else {
                ErrorPayload::new(ProtocolErrorCode::IndexCorrupt.into(), message, retryable)
                    .with_details(error_details::IndexCorruptDetails {
                        reason: err.to_string(),
                    })
            }
        }
        WalIndexError::MetaMismatch {
            key: "store_epoch",
            expected,
            got,
            store_id,
        } => {
            let expected_epoch = expected.parse::<u64>().ok();
            let got_epoch = got.parse::<u64>().ok();
            if let (Some(expected_epoch), Some(got_epoch)) = (expected_epoch, got_epoch) {
                ErrorPayload::new(
                    ProtocolErrorCode::StoreEpochMismatch.into(),
                    message,
                    retryable,
                )
                .with_details(error_details::StoreEpochMismatchDetails {
                    store_id: *store_id,
                    expected_epoch,
                    got_epoch,
                })
            } else {
                ErrorPayload::new(ProtocolErrorCode::IndexCorrupt.into(), message, retryable)
                    .with_details(error_details::IndexCorruptDetails {
                        reason: err.to_string(),
                    })
            }
        }
        WalIndexError::MetaMismatch { .. }
        | WalIndexError::MetaMissing { .. }
        | WalIndexError::EventIdDecode(_)
        | WalIndexError::HlcRowDecode(_)
        | WalIndexError::SegmentRowDecode(_)
        | WalIndexError::WatermarkRowDecode(_)
        | WalIndexError::ReplicaLivenessRowDecode(_)
        | WalIndexError::CborDecode(_)
        | WalIndexError::CborEncode(_)
        | WalIndexError::ConcurrentWrite { .. }
        | WalIndexError::OriginSeqOverflow { .. }
        | WalIndexError::Sqlite(_) => {
            ErrorPayload::new(ProtocolErrorCode::IndexCorrupt.into(), message, retryable)
                .with_details(error_details::IndexCorruptDetails {
                    reason: err.to_string(),
                })
        }
        _ => ErrorPayload::new(wal_index_error_code(err), message, retryable),
    }
}

fn wal_index_error_code(err: &WalIndexError) -> ErrorCode {
    match err {
        WalIndexError::SchemaVersionMismatch { .. } => {
            ProtocolErrorCode::IndexRebuildRequired.into()
        }
        WalIndexError::Equivocation { .. } => ProtocolErrorCode::Equivocation.into(),
        WalIndexError::ClientRequestIdReuseMismatch { .. } => {
            ProtocolErrorCode::ClientRequestIdReuseMismatch.into()
        }
        WalIndexError::Symlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
        WalIndexError::MetaMismatch { key, .. } => match *key {
            "store_id" => ProtocolErrorCode::WrongStore.into(),
            "store_epoch" => ProtocolErrorCode::StoreEpochMismatch.into(),
            _ => ProtocolErrorCode::IndexCorrupt.into(),
        },
        WalIndexError::MetaMissing { .. }
        | WalIndexError::EventIdDecode(_)
        | WalIndexError::HlcRowDecode(_)
        | WalIndexError::SegmentRowDecode(_)
        | WalIndexError::WatermarkRowDecode(_)
        | WalIndexError::ReplicaLivenessRowDecode(_)
        | WalIndexError::CborDecode(_)
        | WalIndexError::CborEncode(_)
        | WalIndexError::ConcurrentWrite { .. }
        | WalIndexError::OriginSeqOverflow { .. } => ProtocolErrorCode::IndexCorrupt.into(),
        WalIndexError::Sqlite(_) => ProtocolErrorCode::IndexCorrupt.into(),
        WalIndexError::Io { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ProtocolErrorCode::PermissionDenied.into()
            } else {
                CliErrorCode::IoError.into()
            }
        }
    }
}

fn wal_replay_error_code(err: &WalReplayError) -> ErrorCode {
    match err {
        WalReplayError::Symlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
        WalReplayError::Io { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ProtocolErrorCode::PermissionDenied.into()
            } else {
                CliErrorCode::IoError.into()
            }
        }
        WalReplayError::SegmentHeader { source, .. } => wal_segment_header_error_code(source),
        WalReplayError::SegmentHeaderMismatch { .. } => {
            ProtocolErrorCode::SegmentHeaderMismatch.into()
        }
        WalReplayError::RecordShaMismatch(_) | WalReplayError::RecordPayloadMismatch(_) => {
            ProtocolErrorCode::HashMismatch.into()
        }
        WalReplayError::RecordDecode { .. }
        | WalReplayError::EventBodyDecode { .. }
        | WalReplayError::RecordHeaderMismatch { .. }
        | WalReplayError::RecordCanonicalEncode { .. }
        | WalReplayError::MissingHead { .. }
        | WalReplayError::UnexpectedHead { .. }
        | WalReplayError::SealedSegmentLenMismatch { .. }
        | WalReplayError::MidFileCorruption { .. } => ProtocolErrorCode::WalCorrupt.into(),
        WalReplayError::NonContiguousSeq { .. } => ProtocolErrorCode::GapDetected.into(),
        WalReplayError::PrevShaMismatch { .. } => ProtocolErrorCode::PrevShaMismatch.into(),
        WalReplayError::IndexOffsetInvalid { .. } | WalReplayError::OriginSeqOverflow { .. } => {
            ProtocolErrorCode::IndexCorrupt.into()
        }
        WalReplayError::Index(err) => wal_index_error_code(err),
    }
}

fn event_wal_error_code(err: &EventWalError) -> ErrorCode {
    match err {
        EventWalError::RecordTooLarge { .. } => ProtocolErrorCode::WalRecordTooLarge.into(),
        EventWalError::SegmentHeaderUnsupportedVersion { .. } => wal_segment_header_error_code(err),
        EventWalError::Symlink { .. } => ProtocolErrorCode::PathSymlinkRejected.into(),
        EventWalError::Io { source, .. } => {
            if source.kind() == std::io::ErrorKind::PermissionDenied {
                ProtocolErrorCode::PermissionDenied.into()
            } else {
                CliErrorCode::IoError.into()
            }
        }
        _ => ProtocolErrorCode::WalCorrupt.into(),
    }
}

fn wal_segment_header_error_code(source: &EventWalError) -> ErrorCode {
    match source {
        EventWalError::SegmentHeaderUnsupportedVersion { .. } => {
            ProtocolErrorCode::WalFormatUnsupported.into()
        }
        _ => ProtocolErrorCode::WalCorrupt.into(),
    }
}

fn store_lock_error_payload(
    err: &StoreLockError,
    message: String,
    retryable: bool,
) -> ErrorPayload {
    match err {
        StoreLockError::Held { store_id, meta, .. } => {
            let (holder_pid, holder_replica_id, started_at_ms, daemon_version) = meta
                .as_deref()
                .map(|meta| {
                    (
                        Some(meta.pid),
                        Some(meta.replica_id),
                        Some(meta.started_at_ms),
                        Some(meta.daemon_version.clone()),
                    )
                })
                .unwrap_or((None, None, None, None));
            ErrorPayload::new(ProtocolErrorCode::LockHeld.into(), message, retryable).with_details(
                error_details::LockHeldDetails {
                    store_id: *store_id,
                    holder_pid,
                    holder_replica_id,
                    started_at_ms,
                    daemon_version,
                },
            )
        }
        StoreLockError::Symlink { path } => ErrorPayload::new(
            ProtocolErrorCode::PathSymlinkRejected.into(),
            message,
            retryable,
        )
        .with_details(error_details::PathSymlinkRejectedDetails {
            path: path.display().to_string(),
        }),
        StoreLockError::MetadataCorrupt { source, .. } => {
            ErrorPayload::new(ProtocolErrorCode::Corruption.into(), message, retryable)
                .with_details(error_details::CorruptionDetails {
                    reason: source.to_string(),
                })
        }
        StoreLockError::Io {
            path,
            operation,
            source,
        } => match source.kind() {
            std::io::ErrorKind::PermissionDenied => ErrorPayload::new(
                ProtocolErrorCode::PermissionDenied.into(),
                message,
                retryable,
            )
            .with_details(error_details::PermissionDeniedDetails {
                path: path.display().to_string(),
                operation: lock_permission_operation(*operation),
            }),
            _ => ErrorPayload::new(ProtocolErrorCode::InternalError.into(), message, retryable),
        },
    }
}

fn lock_permission_operation(operation: StoreLockOperation) -> error_details::PermissionOperation {
    match operation {
        StoreLockOperation::Read => error_details::PermissionOperation::Read,
        StoreLockOperation::Write => error_details::PermissionOperation::Write,
        StoreLockOperation::Fsync => error_details::PermissionOperation::Fsync,
    }
}

fn invalid_id_details(err: &InvalidId) -> error_details::InvalidIdDetails {
    match err {
        InvalidId::Bead { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::Bead,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::Actor { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::Actor,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::Note { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::Note,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::Branch { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::Branch,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::ContentHash { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::ContentHash,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::Namespace { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::Namespace,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::StoreId { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::StoreId,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::ReplicaId { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::ReplicaId,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::TxnId { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::TxnId,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::ClientRequestId { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::ClientRequestId,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::TraceId { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::TraceId,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        InvalidId::SegmentId { raw, reason } => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::SegmentId,
            raw: raw.clone(),
            reason: reason.clone(),
        },
        // Handle any future variants added to the non-exhaustive enum
        _ => error_details::InvalidIdDetails {
            kind: error_details::InvalidIdKind::Bead, // fallback
            raw: format!("{:?}", err),
            reason: "unknown variant".to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        BeadId, DurabilityClass, DurabilityReceipt, Limits, NamespaceId, ReplicaId, Seq0, Seq1,
        StoreEpoch, StoreId, StoreIdentity, TxnId,
    };
    use crate::daemon::ipc::{Request, decode_request_with_limits};
    use crate::daemon::wal::{RecordShaMismatchInfo, WalIndexError, WalReplayError};
    use std::io;
    use uuid::Uuid;

    #[test]
    fn wal_replay_hash_mismatch_includes_details() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
        let err = WalReplayError::RecordShaMismatch(Box::new(RecordShaMismatchInfo {
            namespace: namespace.clone(),
            origin,
            seq: Seq1::from_u64(7).unwrap(),
            expected: [3u8; 32],
            got: [4u8; 32],
            path: std::path::PathBuf::from("/tmp/segment.wal"),
            offset: 12,
        }));

        let store_err = StoreRuntimeError::WalReplay(Box::new(err));
        let payload = store_runtime_error_payload(&store_err, "boom".to_string(), false);

        assert_eq!(payload.code, ProtocolErrorCode::HashMismatch.into());
        let details = payload
            .details_as::<error_details::HashMismatchDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.eid.namespace, namespace);
        assert_eq!(details.eid.origin_replica_id, origin);
        assert_eq!(details.eid.origin_seq, 7);
        assert_eq!(details.expected_sha256, hex::encode([3u8; 32]));
        assert_eq!(details.got_sha256, hex::encode([4u8; 32]));
    }

    #[test]
    fn wal_replay_format_unsupported_includes_details() {
        let err = WalReplayError::SegmentHeader {
            path: std::path::PathBuf::from("/tmp/segment.wal"),
            source: EventWalError::SegmentHeaderUnsupportedVersion {
                got: 3,
                supported: 2,
            },
        };

        let store_err = StoreRuntimeError::WalReplay(Box::new(err));
        let payload = store_runtime_error_payload(&store_err, "boom".to_string(), false);

        assert_eq!(payload.code, ProtocolErrorCode::WalFormatUnsupported.into());
        let details = payload
            .details_as::<error_details::WalFormatUnsupportedDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.wal_format_version, 3);
        assert_eq!(details.supported, vec![2]);
    }

    #[test]
    fn wal_replay_header_decode_is_corrupt() {
        let err = WalReplayError::SegmentHeader {
            path: std::path::PathBuf::from("/tmp/segment.wal"),
            source: EventWalError::SegmentHeaderMagicMismatch { got: [0u8; 5] },
        };

        let store_err = StoreRuntimeError::WalReplay(Box::new(err));
        let payload = store_runtime_error_payload(&store_err, "boom".to_string(), false);

        assert_eq!(payload.code, ProtocolErrorCode::WalCorrupt.into());
    }

    #[test]
    fn wal_index_equivocation_includes_details() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
        let err = WalIndexError::Equivocation {
            namespace: namespace.clone(),
            origin,
            seq: 7,
            existing_sha256: [1u8; 32],
            new_sha256: [2u8; 32],
        };

        let store_err = StoreRuntimeError::WalIndex(err);
        let payload = store_runtime_error_payload(&store_err, "boom".to_string(), false);

        assert_eq!(payload.code, ProtocolErrorCode::Equivocation.into());
        let details = payload
            .details_as::<error_details::EquivocationDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.eid.namespace, namespace);
        assert_eq!(details.eid.origin_replica_id, origin);
        assert_eq!(details.eid.origin_seq, 7);
        assert_eq!(details.existing_sha256, hex::encode([1u8; 32]));
        assert_eq!(details.new_sha256, hex::encode([2u8; 32]));
    }

    #[test]
    fn wal_index_store_epoch_mismatch_includes_details() {
        let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
        let err = WalIndexError::MetaMismatch {
            key: "store_epoch",
            expected: "1".to_string(),
            got: "2".to_string(),
            store_id,
        };

        let store_err = StoreRuntimeError::WalIndex(err);
        let payload = store_runtime_error_payload(&store_err, "boom".to_string(), false);

        assert_eq!(payload.code, ProtocolErrorCode::StoreEpochMismatch.into());
        let details = payload
            .details_as::<error_details::StoreEpochMismatchDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.store_id, store_id);
        assert_eq!(details.expected_epoch, 1);
        assert_eq!(details.got_epoch, 2);
    }

    #[test]
    fn wal_replay_prev_sha_mismatch_includes_details() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([8u8; 16]));
        let err = WalReplayError::PrevShaMismatch {
            namespace: namespace.clone(),
            origin,
            seq: Seq1::from_u64(2).unwrap(),
            expected_prev_sha256: [3u8; 32],
            got_prev_sha256: [4u8; 32],
            head_seq: Seq0::new(1),
        };

        let store_err = StoreRuntimeError::WalReplay(Box::new(err));
        let payload = store_runtime_error_payload(&store_err, "boom".to_string(), false);

        assert_eq!(payload.code, ProtocolErrorCode::PrevShaMismatch.into());
        let details = payload
            .details_as::<error_details::PrevShaMismatchDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.eid.namespace, namespace);
        assert_eq!(details.eid.origin_replica_id, origin);
        assert_eq!(details.eid.origin_seq, 2);
        assert_eq!(details.expected_prev_sha256, hex::encode([3u8; 32]));
        assert_eq!(details.got_prev_sha256, hex::encode([4u8; 32]));
        assert_eq!(details.head_seq, 1);
    }

    #[test]
    fn lock_permission_denied_includes_details() {
        let err = StoreLockError::Io {
            path: std::path::PathBuf::from("/tmp/beads.lock"),
            operation: StoreLockOperation::Write,
            source: io::Error::from(io::ErrorKind::PermissionDenied),
        };

        let payload = store_lock_error_payload(&err, "boom".to_string(), false);

        assert_eq!(payload.code, ProtocolErrorCode::PermissionDenied.into());
        let details = payload
            .details_as::<error_details::PermissionDeniedDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.path, "/tmp/beads.lock");
        assert_eq!(details.operation, error_details::PermissionOperation::Write);
    }

    #[test]
    fn version_mismatch_error_includes_parse_error() {
        let err = IpcError::DaemonVersionMismatch {
            daemon: None,
            client_version: "0.1.0".into(),
            protocol_version: 1,
            parse_error: Some("data did not match any variant".into()),
        };
        // Error should indicate version mismatch
        assert_eq!(err.code(), CliErrorCode::DaemonVersionMismatch.into());
        // The error message should be actionable
        assert!(err.to_string().contains("restart the daemon"));
    }

    #[test]
    fn version_mismatch_is_retryable() {
        let err = IpcError::DaemonVersionMismatch {
            daemon: None,
            client_version: "0.1.0".into(),
            protocol_version: 1,
            parse_error: None,
        };
        assert!(err.transience().is_retryable());
    }

    #[test]
    fn invalid_json_would_cause_parse_error() {
        // Simulate what an old/incompatible daemon might send
        let bad_json = r#"{"unexpected": "format"}"#;
        let result: Result<Response, _> = serde_json::from_str(bad_json);
        assert!(result.is_err());
        // This is the error type that would be converted to DaemonVersionMismatch
        let err = result.unwrap_err();
        assert!(err.to_string().contains("did not match"));
    }

    #[test]
    fn decode_request_invalid_json_is_malformed_payload() {
        let err = decode_request_with_limits("{", &Limits::default()).unwrap_err();
        let payload = err.into_error_payload();
        assert_eq!(payload.code, ProtocolErrorCode::MalformedPayload.into());
        let details = payload
            .details_as::<error_details::MalformedPayloadDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.parser, error_details::ParserKind::Json);
        assert!(details.reason.is_some());
    }

    #[test]
    fn decode_request_semantic_error_is_invalid_request() {
        let err = decode_request_with_limits(r#"{"op":"create"}"#, &Limits::default()).unwrap_err();
        let payload = err.into_error_payload();
        assert_eq!(payload.code, ProtocolErrorCode::InvalidRequest.into());
        let details = payload
            .details_as::<error_details::InvalidRequestDetails>()
            .unwrap()
            .expect("details");
        assert!(details.reason.is_some());
        assert!(details.field.is_none());
    }

    #[test]
    fn decode_request_rejects_invalid_status_value() {
        let err = decode_request_with_limits(
            r#"{"op":"update","repo":".","id":"bd-123","patch":{"status":"bad"}}"#,
            &Limits::default(),
        )
        .unwrap_err();
        let payload = err.into_error_payload();
        assert_eq!(payload.code, ProtocolErrorCode::InvalidRequest.into());
        let details = payload
            .details_as::<error_details::InvalidRequestDetails>()
            .unwrap()
            .expect("details");
        assert!(details.reason.is_some());
    }

    #[test]
    fn decode_request_rejects_invalid_bead_id() {
        let err = decode_request_with_limits(
            r#"{"op":"show","repo":".","id":"BAD"}"#,
            &Limits::default(),
        )
        .unwrap_err();
        let payload = err.into_error_payload();
        assert_eq!(payload.code, ProtocolErrorCode::InvalidRequest.into());
    }

    #[test]
    fn decode_request_rejects_invalid_namespace() {
        let err = decode_request_with_limits(
            r#"{"op":"status","repo":".","namespace":"BAD"}"#,
            &Limits::default(),
        )
        .unwrap_err();
        let payload = err.into_error_payload();
        assert_eq!(payload.code, ProtocolErrorCode::InvalidRequest.into());
    }

    #[test]
    fn decode_request_rejects_invalid_durability() {
        let err = decode_request_with_limits(
            r#"{"op":"create","repo":".","durability":"nope","title":"bad","type":"task","priority":2}"#,
            &Limits::default(),
        )
        .unwrap_err();
        let payload = err.into_error_payload();
        assert_eq!(payload.code, ProtocolErrorCode::InvalidRequest.into());
    }

    #[test]
    fn decode_request_accepts_valid_bead_id() {
        let request = decode_request_with_limits(
            r#"{"op":"show","repo":".","id":"bd-123"}"#,
            &Limits::default(),
        )
        .expect("decode");
        match request {
            Request::Show { payload, .. } => {
                assert_eq!(payload.id, BeadId::parse("bd-123").expect("bead id"));
            }
            other => panic!("unexpected request: {other:?}"),
        }
    }

    #[test]
    fn decode_request_rejects_oversize_frames() {
        let limits = Limits {
            max_frame_bytes: 1,
            ..Limits::default()
        };
        let err = decode_request_with_limits(r#"{"op":"ping"}"#, &limits).unwrap_err();
        assert!(matches!(err, IpcError::FrameTooLarge { .. }));
    }

    #[test]
    fn durability_timeout_includes_receipt() {
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([4u8; 16])), StoreEpoch::ZERO);
        let txn_id = TxnId::new(Uuid::from_bytes([5u8; 16]));
        let receipt = DurabilityReceipt::local_fsync_defaults(store, txn_id, Vec::new(), 123);

        let err = OpError::DurabilityTimeout {
            requested: DurabilityClass::LocalFsync,
            waited_ms: 500,
            pending_replica_ids: None,
            receipt: Box::new(receipt.clone()),
        };

        let payload = err.into_error_payload();
        assert_eq!(payload.code, ProtocolErrorCode::DurabilityTimeout.into());
        let parsed: DurabilityReceipt = payload
            .receipt_as()
            .expect("receipt decode")
            .expect("receipt");
        assert_eq!(parsed, receipt);
    }

    #[test]
    fn namespace_policy_violation_includes_details() {
        let err = OpError::NamespacePolicyViolation {
            namespace: NamespaceId::core(),
            rule: "replicate_mode".to_string(),
            reason: Some("replicate_mode=none".to_string()),
        };
        let payload = err.into_error_payload();
        assert_eq!(
            payload.code,
            ProtocolErrorCode::NamespacePolicyViolation.into()
        );
        let details = payload
            .details_as::<error_details::NamespacePolicyViolationDetails>()
            .expect("details decode")
            .expect("details");
        assert_eq!(details.namespace, NamespaceId::core());
        assert_eq!(details.rule, "replicate_mode");
        assert_eq!(details.reason.as_deref(), Some("replicate_mode=none"));
    }
}
