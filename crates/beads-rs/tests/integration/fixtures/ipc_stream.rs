#![allow(dead_code)]

use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

use beads_rs::NamespaceId;
use beads_rs::api::{StreamEvent, SubscribeInfo};
use beads_rs::daemon::ipc::{
    EmptyPayload, IpcClient, IpcError, ReadConsistency, ReadCtx, Request, Response, ResponsePayload,
    SubscriptionStream,
};

#[derive(Debug)]
pub enum StreamMessage {
    Subscribed(SubscribeInfo),
    Event(Box<StreamEvent>),
}

#[derive(Debug, Error)]
pub enum StreamClientError {
    #[error(transparent)]
    Ipc(#[from] IpcError),
    #[error("stream error: {0:?}")]
    Remote(Box<beads_rs::ErrorPayload>),
    #[error("unexpected response payload: {0:?}")]
    Unexpected(Box<ResponsePayload>),
}

pub struct StreamingClient {
    stream: SubscriptionStream,
    subscribed: Option<SubscribeInfo>,
}

impl StreamingClient {
    pub fn subscribe(repo: PathBuf, namespace: NamespaceId) -> Result<Self, StreamClientError> {
        let read = ReadConsistency {
            namespace: Some(namespace.as_str().to_string()),
            ..ReadConsistency::default()
        };
        Self::subscribe_with_read(repo, read)
    }

    pub fn subscribe_with_read(
        repo: PathBuf,
        read: ReadConsistency,
    ) -> Result<Self, StreamClientError> {
        Self::subscribe_with_client(repo, read, IpcClient::new())
    }

    pub fn subscribe_with_client(
        repo: PathBuf,
        read: ReadConsistency,
        client: IpcClient,
    ) -> Result<Self, StreamClientError> {
        let request = Request::Subscribe {
            ctx: ReadCtx::new(repo, read),
            payload: EmptyPayload {},
        };
        let mut stream = client.subscribe_stream(&request)?;
        let subscribed = loop {
            match Self::read_message_from_stream(&mut stream)? {
                Some(StreamMessage::Subscribed(info)) => break info,
                Some(StreamMessage::Event(_)) => continue,
                None => {
                    return Err(StreamClientError::Ipc(IpcError::Disconnected));
                }
            }
        };

        Ok(Self {
            stream,
            subscribed: Some(subscribed),
        })
    }

    pub fn subscribed(&self) -> Option<&SubscribeInfo> {
        self.subscribed.as_ref()
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> Result<(), StreamClientError> {
        self.stream.set_read_timeout(timeout)?;
        Ok(())
    }

    pub fn next_message(&mut self) -> Result<Option<StreamMessage>, StreamClientError> {
        Self::read_message_from_stream(&mut self.stream)
    }

    pub fn next_event(&mut self) -> Result<Option<StreamEvent>, StreamClientError> {
        loop {
            match self.next_message()? {
                Some(StreamMessage::Event(event)) => return Ok(Some(*event)),
                Some(StreamMessage::Subscribed(info)) => {
                    self.subscribed = Some(info);
                }
                None => {
                    tracing::warn!("subscribe stream closed before delivering next event");
                    return Err(StreamClientError::Ipc(IpcError::Disconnected));
                }
            }
        }
    }

    fn read_message_from_stream(
        stream: &mut SubscriptionStream,
    ) -> Result<Option<StreamMessage>, StreamClientError> {
        let Some(response) = stream.read_response()? else {
            return Ok(None);
        };
        Self::message_from_response(response).map(Some)
    }

    fn message_from_response(response: Response) -> Result<StreamMessage, StreamClientError> {
        match response {
            Response::Ok { ok } => match ok {
                ResponsePayload::Subscribed(payload) => {
                    Ok(StreamMessage::Subscribed(payload.subscribed))
                }
                ResponsePayload::Event(payload) => {
                    Ok(StreamMessage::Event(Box::new(payload.event)))
                }
                other => Err(StreamClientError::Unexpected(Box::new(other))),
            },
            Response::Err { err } => Err(StreamClientError::Remote(Box::new(err))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use beads_rs::api::EventBody;
    use beads_rs::daemon::ipc::ResponsePayload;
    use beads_rs::{
        EventId, NamespaceId, ReplicaId, Seq1, StoreEpoch, StoreId, StoreIdentity, TxnDeltaV1,
        TxnId,
    };
    use uuid::Uuid;

    #[test]
    fn fixtures_ipc_stream_parses_subscribed() {
        let info = SubscribeInfo {
            namespace: NamespaceId::core(),
            watermarks_applied: Default::default(),
        };
        let response = Response::ok(ResponsePayload::subscribed(info.clone()));
        let message = StreamingClient::message_from_response(response).expect("message");
        match message {
            StreamMessage::Subscribed(actual) => assert_eq!(actual.namespace, info.namespace),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn fixtures_ipc_stream_parses_event() {
        let origin = ReplicaId::new(Uuid::from_bytes([1u8; 16]));
        let event_id = EventId::new(
            origin,
            NamespaceId::core(),
            Seq1::from_u64(1).expect("seq1"),
        );
        let body = EventBody {
            envelope_v: 1,
            store: StoreIdentity::new(
                StoreId::new(Uuid::from_bytes([2u8; 16])),
                StoreEpoch::new(0),
            ),
            namespace: NamespaceId::core(),
            origin_replica_id: origin,
            origin_seq: Seq1::from_u64(1).expect("seq1"),
            event_time_ms: 1_700_000_000_000,
            txn_id: TxnId::new(Uuid::from_bytes([3u8; 16])),
            client_request_id: None,
            trace_id: None,
            kind: "txn_v1".to_string(),
            delta: TxnDeltaV1::new(),
            hlc_max: None,
        };
        let event = StreamEvent {
            event_id,
            sha256: "00".repeat(32),
            prev_sha256: None,
            body,
            body_bytes_hex: None,
        };
        let response = Response::ok(ResponsePayload::event(event.clone()));
        let message = StreamingClient::message_from_response(response).expect("message");
        match message {
            StreamMessage::Event(actual) => assert_eq!(actual.event_id, event.event_id),
            other => panic!("unexpected message: {other:?}"),
        }
    }
}
