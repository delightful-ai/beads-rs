//! Realtime subscription schemas.

use serde::{Deserialize, Serialize};

use crate::core::{
    ActorId, Applied, ClientRequestId, EventBody as CoreEventBody, EventId, EventKindV1, HlcMax,
    NamespaceId, ReplicaId, Seq1, StoreIdentity, TxnDeltaV1, TxnId, Watermarks,
};

// =============================================================================
// Realtime subscriptions
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventHlcMax {
    pub actor_id: ActorId,
    pub physical_ms: u64,
    pub logical: u32,
}

impl From<&HlcMax> for EventHlcMax {
    fn from(max: &HlcMax) -> Self {
        Self {
            actor_id: max.actor_id.clone(),
            physical_ms: max.physical_ms,
            logical: max.logical,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBody {
    pub envelope_v: u32,
    pub store: StoreIdentity,
    pub namespace: NamespaceId,
    pub origin_replica_id: ReplicaId,
    pub origin_seq: Seq1,
    pub event_time_ms: u64,
    pub txn_id: TxnId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_request_id: Option<ClientRequestId>,
    pub kind: String,
    pub delta: TxnDeltaV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hlc_max: Option<EventHlcMax>,
}

impl From<&CoreEventBody> for EventBody {
    fn from(body: &CoreEventBody) -> Self {
        let (delta, hlc_max) = match &body.kind {
            EventKindV1::TxnV1(txn) => (txn.delta.clone(), Some(EventHlcMax::from(&txn.hlc_max))),
        };
        Self {
            envelope_v: body.envelope_v,
            store: body.store,
            namespace: body.namespace.clone(),
            origin_replica_id: body.origin_replica_id,
            origin_seq: body.origin_seq,
            event_time_ms: body.event_time_ms,
            txn_id: body.txn_id,
            client_request_id: body.client_request_id,
            kind: body.kind.as_str().to_string(),
            delta,
            hlc_max,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvent {
    pub event_id: EventId,
    pub sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_sha256: Option<String>,
    pub body: EventBody,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_bytes_hex: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeInfo {
    pub namespace: NamespaceId,
    pub watermarks_applied: Watermarks<Applied>,
}
