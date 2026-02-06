//! IPC protocol types and codec.
//!
//! Protocol: newline-delimited JSON (ndjson) over Unix socket.
//! Request format: `{"op": "create", ...}\n`
//! Response format: `{"ok": ...}\n` or `{"err": {"code": "...", "message": "..."}}\n`

// Re-export everything from surface.
pub use beads_surface::ipc::*;

// Keep error types accessible.
pub use beads_core::{ErrorCode, ErrorPayload, IntoErrorPayload};

// Daemon-only additions.
mod error_mapping;
pub use error_mapping::ResponseExt;
