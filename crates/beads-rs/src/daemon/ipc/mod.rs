//! IPC protocol types and codec.
//!
//! Protocol: newline-delimited JSON (ndjson) over Unix socket.
//! Request format: `{"op": "create", ...}\n`
//! Response format: `{"ok": ...}\n` or `{"err": {"code": "...", "message": "..."}}\n`

mod error_mapping;

pub use crate::core::{ErrorCode, ErrorPayload};
pub use beads_surface::ipc::types::{
    IPC_PROTOCOL_VERSION, InitializedPayload, MutationMeta, OpResponse, ReadConsistency,
    RefreshedPayload, Request, Response, ResponsePayload, ShuttingDownPayload, StreamEventPayload,
    SubscribedPayload, SyncedPayload,
};
pub use beads_surface::ipc::{
    IpcClient, IpcConnection, IpcError, SubscriptionStream, decode_request,
    decode_request_with_limits, encode_response, ensure_socket_dir, read_requests, send_request,
    send_request_at, send_request_no_autostart, send_request_no_autostart_at, send_response,
    socket_dir, socket_path, socket_path_for_runtime_dir, subscribe_stream, subscribe_stream_at,
    subscribe_stream_no_autostart_at, wait_for_daemon_ready, wait_for_daemon_ready_at,
};
pub use error_mapping::{IntoErrorPayload, ResponseExt};
