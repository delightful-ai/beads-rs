#[path = "integration/fixtures/mod.rs"]
mod fixtures;

#[path = "integration/cli/mod.rs"]
mod cli;
#[path = "integration/core/mod.rs"]
mod core;
#[path = "integration/daemon/mod.rs"]
mod daemon;
#[path = "integration/repl/mod.rs"]
mod repl;
#[path = "integration/wal/mod.rs"]
mod wal;

#[path = "integration/checkpoint.rs"]
mod checkpoint;
#[path = "integration/realtime_errors.rs"]
mod realtime_errors;
