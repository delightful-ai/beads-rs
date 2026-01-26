//! IPC protocol types and codec.
//!
//! Protocol: newline-delimited JSON (ndjson) over Unix socket.
//! Request format: `{"op": "create", ...}\n`
//! Response format: `{"ok": ...}\n` or `{"err": {"code": "...", "message": "..."}}\n`

mod client;
mod error_mapping;

pub use crate::core::{ErrorCode, ErrorPayload};
pub use beads_surface::ipc::types::{
    IPC_PROTOCOL_VERSION, InitializedPayload, MutationMeta, OpResponse, ReadConsistency,
    RefreshedPayload, Request, Response, ResponsePayload, ShuttingDownPayload, StreamEventPayload,
    SubscribedPayload, SyncedPayload,
};
pub use beads_surface::ipc::{
    IpcError, decode_request, decode_request_with_limits, encode_response, read_requests,
    send_response,
};
pub use client::{
    IpcClient, IpcConnection, SubscriptionStream, ensure_socket_dir, send_request, send_request_at,
    send_request_no_autostart, send_request_no_autostart_at, socket_dir, socket_path,
    socket_path_for_runtime_dir, subscribe_stream, subscribe_stream_at,
    subscribe_stream_no_autostart_at, wait_for_daemon_ready, wait_for_daemon_ready_at,
};
pub use error_mapping::{IntoErrorPayload, ResponseExt};
