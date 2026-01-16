//! Replication internal error types.

use crate::core::error::details::{
    ClientRequestIdReuseMismatchDetails, CorruptionDetails, EquivocationDetails,
    FrameTooLargeDetails, GapDetectedDetails, HashMismatchDetails, IndexCorruptDetails,
    InternalErrorDetails, InvalidRequestDetails, MaintenanceModeDetails,
    NamespacePolicyViolationDetails, NamespaceUnknownDetails, NonCanonicalDetails,
    OverloadedDetails, PrevShaMismatchDetails, ReplicaIdCollisionDetails,
    StoreEpochMismatchDetails, SubscriberLaggedDetails, VersionIncompatibleDetails,
    WalCorruptDetails, WrongStoreDetails,
};
use crate::core::{ErrorCode, ErrorPayload};

#[derive(Clone, Debug, PartialEq)]
pub struct ReplError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    pub details: Option<Box<ReplErrorDetails>>,
}

impl ReplError {
    pub fn new(code: ErrorCode, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code,
            message: message.into(),
            retryable,
            details: None,
        }
    }

    pub fn with_details(mut self, details: ReplErrorDetails) -> Self {
        self.details = Some(Box::new(details));
        self
    }

    pub fn to_payload(&self) -> ErrorPayload {
        let payload = ErrorPayload::new(self.code.clone(), self.message.clone(), self.retryable);
        match self.details.as_deref() {
            None => payload,
            Some(details) => match details {
                ReplErrorDetails::WrongStore(details) => payload.with_details(details.clone()),
                ReplErrorDetails::StoreEpochMismatch(details) => payload.with_details(details.clone()),
                ReplErrorDetails::ReplicaIdCollision(details) => payload.with_details(details.clone()),
                ReplErrorDetails::VersionIncompatible(details) => payload.with_details(details.clone()),
                ReplErrorDetails::InvalidRequest(details) => payload.with_details(details.clone()),
                ReplErrorDetails::NamespacePolicyViolation(details) => {
                    payload.with_details(details.clone())
                }
                ReplErrorDetails::NamespaceUnknown(details) => payload.with_details(details.clone()),
                ReplErrorDetails::InternalError(details) => payload.with_details(details.clone()),
                ReplErrorDetails::SubscriberLagged(details) => payload.with_details(details.clone()),
                ReplErrorDetails::HashMismatch(details) => payload.with_details(details.clone()),
                ReplErrorDetails::PrevShaMismatch(details) => payload.with_details(details.clone()),
                ReplErrorDetails::FrameTooLarge(details) => payload.with_details(details.clone()),
                ReplErrorDetails::NonCanonical(details) => payload.with_details(details.clone()),
                ReplErrorDetails::Overloaded(details) => payload.with_details(details.clone()),
                ReplErrorDetails::WalCorrupt(details) => payload.with_details(details.clone()),
                ReplErrorDetails::IndexCorrupt(details) => payload.with_details(details.clone()),
                ReplErrorDetails::GapDetected(details) => payload.with_details(details.clone()),
                ReplErrorDetails::Equivocation(details) => payload.with_details(details.clone()),
                ReplErrorDetails::ClientRequestIdReuseMismatch(details) => {
                    payload.with_details(details.clone())
                }
                ReplErrorDetails::MaintenanceMode(details) => payload.with_details(details.clone()),
                ReplErrorDetails::Corruption(details) => payload.with_details(details.clone()),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ReplErrorDetails {
    WrongStore(WrongStoreDetails),
    StoreEpochMismatch(StoreEpochMismatchDetails),
    ReplicaIdCollision(ReplicaIdCollisionDetails),
    VersionIncompatible(VersionIncompatibleDetails),
    InvalidRequest(InvalidRequestDetails),
    NamespacePolicyViolation(NamespacePolicyViolationDetails),
    NamespaceUnknown(NamespaceUnknownDetails),
    InternalError(InternalErrorDetails),
    SubscriberLagged(SubscriberLaggedDetails),
    HashMismatch(HashMismatchDetails),
    PrevShaMismatch(PrevShaMismatchDetails),
    FrameTooLarge(FrameTooLargeDetails),
    NonCanonical(NonCanonicalDetails),
    Overloaded(OverloadedDetails),
    WalCorrupt(WalCorruptDetails),
    IndexCorrupt(IndexCorruptDetails),
    GapDetected(GapDetectedDetails),
    Equivocation(EquivocationDetails),
    ClientRequestIdReuseMismatch(ClientRequestIdReuseMismatchDetails),
    MaintenanceMode(MaintenanceModeDetails),
    Corruption(CorruptionDetails),
}

#[cfg(test)]
mod tests {
    use super::{ReplError, ReplErrorDetails};
    use crate::core::error::details::WrongStoreDetails;
    use crate::core::{CliErrorCode, ProtocolErrorCode, StoreId};
    use uuid::Uuid;

    #[test]
    fn to_payload_preserves_basic_fields() {
        let error = ReplError::new(CliErrorCode::Internal.into(), "boom", false);
        let payload = error.to_payload();
        assert_eq!(payload.code, CliErrorCode::Internal.into());
        assert_eq!(payload.message, "boom");
        assert!(!payload.retryable);
        assert!(payload.details.is_none());
    }

    #[test]
    fn to_payload_carries_details() {
        let expected = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let got = StoreId::new(Uuid::from_bytes([2u8; 16]));
        let error = ReplError::new(ProtocolErrorCode::WrongStore.into(), "wrong store", false).with_details(
            ReplErrorDetails::WrongStore(WrongStoreDetails {
                expected_store_id: expected,
                got_store_id: got,
            }),
        );
        let payload = error.to_payload();
        assert_eq!(payload.code, ProtocolErrorCode::WrongStore.into());
        let details = payload
            .details_as::<WrongStoreDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.expected_store_id, expected);
        assert_eq!(details.got_store_id, got);
    }
}
