#[path = "integration/fixtures/mod.rs"]
mod fixtures;

#[path = "integration/core/mod.rs"]
mod core;
#[path = "integration/wal/mod.rs"]
mod wal;
#[path = "integration/repl/mod.rs"]
mod repl;
#[path = "integration/daemon/mod.rs"]
mod daemon;
#[path = "integration/cli/mod.rs"]
mod cli;

#[path = "integration/checkpoint.rs"]
mod checkpoint;
#[path = "integration/realtime_errors.rs"]
mod realtime_errors;
