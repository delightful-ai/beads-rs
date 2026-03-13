//! Assembly-owned daemon/product integration coverage.
//!
//! These tests intentionally stay in `beads-rs` because they exercise the
//! shipped `bd` product surface: daemon process startup, runtime/log/data-dir
//! wiring, IPC behavior, crash recovery, and multi-daemon replication as an
//! operator would observe it. Lower-level session, WAL, and harness ownership
//! lives in owner crates and has already moved out of this assembly test crate.

mod admin;
mod admin_status;
mod crash_recovery;
mod lifecycle;
mod logging;
mod realtime_smoke;
mod repl_e2e;
mod store_lock;
mod subscribe;
