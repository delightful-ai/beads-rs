//! Helpers to construct production EventBody/EventFrameV1 instances for models.

use crate::core::{
    ActorId, ClientRequestId, EventBody, EventBytes, EventFrameError, EventFrameV1, EventId,
    EventKindV1, HlcMax, NamespaceId, Opaque, ReplicaId, Seq1, Sha256, StoreIdentity, TraceId,
    TxnDeltaV1, TxnId, TxnV1, encode_event_body_canonical, hash_event_body,
};

pub struct EventFactory {
    pub store: StoreIdentity,
    pub namespace: NamespaceId,
    pub origin: ReplicaId,
    pub actor: ActorId,
}

impl EventFactory {
    pub fn new(
        store: StoreIdentity,
        namespace: NamespaceId,
        origin: ReplicaId,
        actor: ActorId,
    ) -> Self {
        Self {
            store,
            namespace,
            origin,
            actor,
        }
    }

    pub fn txn_body(
        &self,
        origin_seq: Seq1,
        txn_id: TxnId,
        event_time_ms: u64,
        hlc_logical: u32,
        delta: TxnDeltaV1,
        client_request_id: Option<ClientRequestId>,
    ) -> EventBody {
        EventBody {
            envelope_v: 1,
            store: self.store,
            namespace: self.namespace.clone(),
            origin_replica_id: self.origin,
            origin_seq,
            event_time_ms,
            txn_id,
            client_request_id,
            trace_id: client_request_id.map(TraceId::from),
            kind: EventKindV1::TxnV1(TxnV1 {
                hlc_max: HlcMax {
                    actor_id: self.actor.clone(),
                    physical_ms: event_time_ms,
                    logical: hlc_logical,
                },
                delta,
            }),
        }
    }
}

pub fn encode_frame(
    body: &EventBody,
    prev_sha256: Option<Sha256>,
) -> Result<EventFrameV1, EventFrameError> {
    let bytes = encode_event_body_canonical(body)?;
    let sha256 = hash_event_body(&bytes);
    let eid = EventId::new(
        body.origin_replica_id,
        body.namespace.clone(),
        body.origin_seq,
    );
    EventFrameV1::try_from_parts(eid, sha256, prev_sha256, EventBytes::<Opaque>::from(bytes))
}
