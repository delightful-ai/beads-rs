## Boundary
`mod.rs` is the live ownership map. `core/`, `coord/`, and `store/` own generation-scoped store lifecycle; `executor.rs` and `query_executor.rs` own mutation/read wrappers; `repl/**`, `git_worker.rs`, and `server/**` are daemon-specific consumers around those cores.

## Canonical Extension Paths
- Thread IPC-visible behavior through `ipc/mod.rs`, `server/dispatch.rs`, and `coord/request.rs`, then land the actual mutation/read work in `executor.rs` or `query_executor.rs`.
- Thread per-store lifetime changes through `core/store_session.rs`, `core/loaded_store.rs`, and `core/repo_load.rs`. Pass `LoadedStore` or `StoreSessionToken` proofs instead of caching raw `StoreRuntime` or background handles.
- Thread replication changes through `core/replication.rs`, `repl/manager.rs`, `repl/server.rs`, and `durability_coordinator.rs`.
- Thread checkpoint orchestration through `core/checkpoint_import.rs`, `core/checkpoint_scheduling.rs`, and `git_worker.rs`; leave checkpoint format/cache/publish rules in `crates/beads-git`.

## Invariants / Bait
- `core/helpers.rs::PendingReplayApply` must be acknowledged with `acknowledge_checkpoint_dirty(...)` before replayed state becomes live. The compile-fail tests in `crates/beads-daemon/tests/ui/*_private.rs` exist to stop field-peeking.
- `mutation_engine.rs::PlannedMutation` is another acknowledgement boundary. Keep its fields private and extend it through methods, not direct struct access.
- `wal_atomic_commit.rs` and `core/repl_ingest.rs` preserve the append -> index/frontier commit -> in-memory state/broadcast order. Do not split that sequence across ad-hoc helpers.
- `admin/online.rs` reload flows must restore config and preserve existing handles when strict load, runtime reload, or peer reload fails. `core/mod.rs` carries the regression coverage for that path.
- `repl/server.rs` is roster-driven when a roster exists. Do not assume open-peer admission in rostered deployments.
- `durability_coordinator.rs` computes replicated durability from eligible durability roles plus peer ACK watermarks, not from “connected peers”.
- `repl/mod.rs` only re-exports daemon-core frame/proto types. Shared protocol changes belong in `crates/beads-daemon-core/src/repl/proto.rs`.
- `wal/mod.rs` is only a compatibility shim. Shared WAL laws belong in `crates/beads-daemon-core/src/wal/**`.
- `git_backend.rs` is capability-shaped on purpose, and `git_worker.rs` owns `git2::Repository` handles. Do not open repository handles from request paths or session code.
- `core/checkpoint_import.rs` treats hash mismatch and `CheckpointImportError::IncompatibleDepsFormat` as skip-and-rebuild signals. Do not turn that into a lossy merge path.

## Tests
- Keep local subsystem tests beside the owning runtime files, such as `server/tests.rs` and the large regression matrix in `core/mod.rs`.
- Keep crate-level cross-runtime seams in `crates/beads-daemon/tests`.

## Proof Loops
- Run `cargo test -p beads-daemon admin_reload_replication` for replication reload, rebind, or config-restore changes.
- Run `cargo test -p beads-daemon --features test-harness --test repl -- --list` for daemon REPL seam coverage in `crates/beads-daemon/tests/repl.rs`.
- Run `cargo test -p beads-daemon --features test-harness --test wal -- --list` for daemon WAL seam coverage in `crates/beads-daemon/tests/wal.rs`.
- Run `cargo test -p beads-rs --test public_boundary` if a change makes you want to expose `runtime::*`, but treat that test as a guard against forbidden external imports and assembly-owned boundary drift, not as proof of the daemon export list.
- Run `cargo test -p beads-daemon --features test-harness` before calling the runtime subtree done.
