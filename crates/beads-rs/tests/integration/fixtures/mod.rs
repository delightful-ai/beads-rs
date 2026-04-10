//! Assembly-owned integration fixtures.
//!
//! The helpers exported here are limited to product-level concerns that still
//! belong to the `bd` assembly crate: spawning the shipped binary, wiring test
//! repos/runtime dirs, driving IPC requests, and orchestrating multi-daemon
//! end-to-end scenarios. Lower-level repl/WAL fixture surfaces now live in
//! their owner crates and must not grow back here.

pub mod admin_status;
pub mod bd_runtime;
pub mod daemon_boundary;
pub mod daemon_runtime;
pub mod event_body;
pub mod git;
pub mod identity;
pub mod ipc_client;
pub mod ipc_stream;
pub mod legacy_store;
pub mod load_gen;
pub mod mutation;
pub mod realtime;
pub mod receipt;
pub mod repl_rig;
pub mod store_dir;
pub mod store_lock;
pub mod tailnet_proxy;
pub mod temp;
pub mod timing;
pub mod wait;
pub mod wal;
pub mod wal_corrupt;
