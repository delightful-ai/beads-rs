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
    pub details: Option<ReplErrorDetails>,
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
        self.details = Some(details);
        self
    }

    pub fn to_payload(&self) -> ErrorPayload {
        let payload = ErrorPayload::new(self.code, self.message.clone(), self.retryable);
        match &self.details {
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
