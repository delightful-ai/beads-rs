use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::os::unix::net::UnixStream;
use std::sync::Arc;

use crossbeam::channel::Sender;

use super::super::ipc::{
    Request, Response, ResponseExt, ResponsePayload, decode_request_with_limits, send_response,
};
use super::super::subscription::{SubscribeReply, subscriber_limits};
use super::{RequestMessage, ServerReply};
use crate::core::error::details as error_details;
use crate::core::{
    CliErrorCode, ErrorPayload, EventFrameV1, EventId, Limits, ProtocolErrorCode, ReplicaId,
    Sha256, decode_event_body,
};
use beads_daemon::broadcast::{BroadcastEvent, DropReason};

/// Handle a single client connection.
///
/// Reads requests, sends to state thread, waits for response, writes back.
pub(in crate::daemon) fn handle_client(
    stream: UnixStream,
    req_tx: Sender<RequestMessage>,
    limits: Arc<Limits>,
) {
    let reader = match stream.try_clone() {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("failed to clone stream: {}", e);
            return;
        }
    };
    let reader = BufReader::new(reader);
    let mut writer = stream;

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break, // Client disconnected
        };

        // Skip empty lines
        if line.trim().is_empty() {
            continue;
        }

        // Parse request
        let request = match decode_request_with_limits(&line, &limits) {
            Ok(r) => r,
            Err(e) => {
                if send_response(&mut writer, &Response::err_from(e)).is_err() {
                    break;
                }
                continue;
            }
        };

        // Check if this is a shutdown request
        let is_shutdown = matches!(request, Request::Shutdown);

        // Send to state thread, wait for response
        let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
        if req_tx
            .send(RequestMessage::new(request, respond_tx))
            .is_err()
        {
            break; // State thread died
        }

        let reply = match respond_rx.recv() {
            Ok(r) => r,
            Err(_) => break, // State thread died
        };

        match reply {
            ServerReply::Response(response) => {
                if send_response(&mut writer, &response).is_err() {
                    break; // Client disconnected
                }

                // If shutdown, close connection
                if is_shutdown {
                    break;
                }
            }
            ServerReply::Subscribe(reply) => {
                if send_response(&mut writer, &reply.ack).is_err() {
                    break;
                }
                stream_subscription(&mut writer, reply, &limits);
                break;
            }
        }
    }
}

fn stream_subscription(writer: &mut UnixStream, reply: SubscribeReply, limits: &Limits) {
    let namespace = reply.namespace;
    let subscriber_limits = subscriber_limits(limits);
    let backfill_end = reply.backfill_end;

    for frame in reply.backfill {
        if !send_stream_frame(writer, frame, limits) {
            return;
        }
    }

    for event in reply.hot_cache {
        if event.namespace != namespace {
            continue;
        }
        if should_skip_backfill(&backfill_end, &event) {
            continue;
        }
        if !send_stream_event(writer, event, limits) {
            return;
        }
    }

    let subscription = reply.subscription;
    loop {
        match subscription.recv() {
            Ok(event) => {
                if event.namespace != namespace {
                    continue;
                }
                if should_skip_backfill(&backfill_end, &event) {
                    continue;
                }
                if !send_stream_event(writer, event, limits) {
                    return;
                }
            }
            Err(_) => {
                if matches!(
                    subscription.drop_reason(),
                    Some(DropReason::SubscriberLagged)
                ) {
                    let payload = ErrorPayload::new(
                        ProtocolErrorCode::SubscriberLagged.into(),
                        "subscriber lagged",
                        true,
                    )
                    .with_details(error_details::SubscriberLaggedDetails {
                        reason: None,
                        max_queue_bytes: Some(subscriber_limits.max_bytes as u64),
                        max_queue_events: Some(subscriber_limits.max_events as u64),
                    });
                    let _ = send_response(writer, &Response::err_from(payload));
                } else {
                    let payload = ErrorPayload::new(
                        CliErrorCode::Disconnected.into(),
                        "subscription closed",
                        true,
                    );
                    let _ = send_response(writer, &Response::err_from(payload));
                }
                return;
            }
        }
    }
}

fn send_stream_event(writer: &mut UnixStream, event: BroadcastEvent, limits: &Limits) -> bool {
    let response = stream_event_response(event, limits);
    let should_continue = !matches!(response, Response::Err { .. });
    if send_response(writer, &response).is_err() {
        return false;
    }
    should_continue
}

pub(super) fn stream_event_response(event: BroadcastEvent, limits: &Limits) -> Response {
    stream_event_response_from_parts(
        event.event_id,
        event.sha256,
        event.prev_sha256,
        event.bytes.as_ref(),
        limits,
    )
}

fn send_stream_frame(writer: &mut UnixStream, frame: EventFrameV1, limits: &Limits) -> bool {
    let response = stream_frame_response(&frame, limits);
    let should_continue = !matches!(response, Response::Err { .. });
    if send_response(writer, &response).is_err() {
        return false;
    }
    should_continue
}

fn stream_frame_response(frame: &EventFrameV1, limits: &Limits) -> Response {
    stream_event_response_from_parts(
        frame.eid().clone(),
        frame.sha256(),
        frame.prev_sha256(),
        frame.bytes().as_ref(),
        limits,
    )
}

fn stream_event_response_from_parts(
    event_id: EventId,
    sha256: Sha256,
    prev_sha256: Option<Sha256>,
    bytes: &[u8],
    limits: &Limits,
) -> Response {
    let (_, body) = match decode_event_body(bytes, limits) {
        Ok(body) => body,
        Err(err) => {
            return Response::err_from(
                ErrorPayload::new(
                    ProtocolErrorCode::WalCorrupt.into(),
                    "event body decode failed",
                    false,
                )
                .with_details(error_details::WalCorruptDetails {
                    namespace: event_id.namespace.clone(),
                    segment_id: None,
                    offset: None,
                    reason: err.to_string(),
                }),
            );
        }
    };

    let stream_event = crate::api::StreamEvent {
        event_id,
        sha256: hex::encode(sha256.as_bytes()),
        prev_sha256: prev_sha256.map(|prev| hex::encode(prev.as_bytes())),
        body: crate::api::EventBody::from(body.as_ref()),
        body_bytes_hex: Some(hex::encode(bytes)),
    };

    Response::ok(ResponsePayload::event(stream_event))
}

fn should_skip_backfill(backfill_end: &HashMap<ReplicaId, u64>, event: &BroadcastEvent) -> bool {
    let origin = event.event_id.origin_replica_id;
    let seq = event.event_id.origin_seq.get();
    backfill_end
        .get(&origin)
        .is_some_and(|backfill_seq| seq <= *backfill_seq)
}
