use super::Response;
use crate::core::IntoErrorPayload;

// =============================================================================
// Error mapping
// =============================================================================

pub trait ResponseExt {
    fn err_from<E: IntoErrorPayload>(e: E) -> Response;
}

impl ResponseExt for Response {
    fn err_from<E: IntoErrorPayload>(e: E) -> Response {
        Response::err(e.into_error_payload())
    }
}

#[cfg(test)]
mod tests {
    use crate::core::error::details as error_details;
    use crate::core::{
        BeadId, CliErrorCode, DurabilityClass, DurabilityReceipt, IntoErrorPayload, Limits,
        NamespaceId, ProtocolErrorCode, ReplicaId, Seq0, Seq1, StoreEpoch, StoreId, StoreIdentity,
        TxnId,
    };
    use crate::daemon::ipc::{IpcError, Request, Response, decode_request_with_limits};
    use crate::daemon::ops::OpError;
    use crate::daemon::store_lock::{StoreLockError, StoreLockOperation};
    use crate::daemon::store_runtime::StoreRuntimeError;
    use crate::daemon::wal::{EventWalError, RecordShaMismatchInfo, WalIndexError, WalReplayError};
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
        let payload = store_err.into_error_payload();

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
        let payload = store_err.into_error_payload();

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
        let payload = store_err.into_error_payload();

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
        let payload = store_err.into_error_payload();

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
        let payload = store_err.into_error_payload();

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
        let payload = store_err.into_error_payload();

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

        let payload = err.into_error_payload();

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
