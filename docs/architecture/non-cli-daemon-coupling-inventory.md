# Non-CLI Daemon Coupling Inventory (`bd-6ea1.1`)

Audit date: 2026-02-24  
Scope audited:
- `crates/beads-rs/src/model/**`
- `crates/beads_stateright_models/**`
- `crates/fuzz/**`
- `crates/beads-rs/tests/**`
- `crates/beads-rs/src/bin/**`

Method: searched Rust sources for direct `crate::daemon::...` / `beads_rs::daemon::...` references and reviewed each callsite:

```bash
rg -n --glob '*.rs' "crate::daemon::|beads_rs::daemon::" \
  crates/beads-rs/src/model \
  crates/beads_stateright_models \
  crates/fuzz \
  crates/beads-rs/tests \
  crates/beads-rs/src/bin
```

## Coupling Entries

| File path | Daemon symbol/module used | Classification | Migration note |
| --- | --- | --- | --- |
| `crates/beads_stateright_models/examples/durability_idempotency_machine.rs` | `beads_rs::daemon::repl::WatermarkState` | `shared-core candidate` | Move watermark state type to a non-daemon protocol/state crate so model checks do not depend on daemon module exports. |
| `crates/fuzz/fuzz_targets/repl_message_decode.rs` | `beads_rs::daemon::repl::decode_envelope` | `shared-core candidate` | Fuzz target should consume repl wire decode API from extracted protocol surface, not daemon path. |
| `crates/fuzz/fuzz_targets/wal_decode.rs` | `beads_rs::daemon::wal::FrameReader` | `shared-core candidate` | Frame decode should come from shared WAL/repl wire layer once extracted. |
| `crates/beads-rs/tests/integration/daemon/store_lock.rs` | `beads_rs::daemon::{StoreLock, StoreLockError, read_lock_meta}` | `daemon-internal` | Store lock semantics are daemon internals; keep scoped to daemon-owned tests/testkit. |
| `crates/beads-rs/tests/integration/fixtures/daemon_boundary.rs` | `beads_rs::daemon::repl::*`, `beads_rs::daemon::wal::*` | `daemon-internal` | Boundary fixture is intentionally deep daemon test plumbing; migrate only via explicit daemon testkit API. |
| `crates/beads-rs/tests/integration/repl/ack.rs` | `beads_rs::daemon::repl::session::*`, `beads_rs::daemon::repl::*`, `beads_rs::daemon::wal::*` | `daemon-internal` | Ack/session behavior validates daemon runtime internals. |
| `crates/beads-rs/tests/integration/repl/backpressure.rs` | `beads_rs::daemon::repl::session::*`, `beads_rs::daemon::repl::*` | `daemon-internal` | Backpressure flow control is daemon runtime policy. |
| `crates/beads-rs/tests/integration/repl/range.rs` | `beads_rs::daemon::repl::*`, `beads_rs::daemon::wal::*` | `daemon-internal` | Range behavior remains coupled to daemon WAL/repl internals. |
| `crates/beads-rs/tests/integration/wal/fsck.rs` | `beads_rs::daemon::wal::fsck::*`, `beads_rs::daemon::wal::*` | `daemon-internal` | FSCK and WAL row/index details are daemon storage internals. |
| `crates/beads-rs/tests/integration/wal/idempotency.rs` | `beads_rs::daemon::wal::*` | `daemon-internal` | WAL idempotency currently validates internal index behavior. |
| `crates/beads-rs/tests/integration/wal/index.rs` | `beads_rs::daemon::wal::*` | `daemon-internal` | WAL index rebuild APIs are daemon implementation details. |
| `crates/beads-rs/tests/integration/wal/receipts.rs` | `beads_rs::daemon::wal::*` | `daemon-internal` | Receipt reconstruction is currently daemon-internal WAL behavior. |
| `crates/beads-rs/tests/integration/wal/seq.rs` | `beads_rs::daemon::wal::*` | `daemon-internal` | WAL sequence checks depend on daemon WAL internals. |
| `crates/beads-rs/tests/integration/wal/wal_tests.rs` | `beads_rs::daemon::wal::*` | `daemon-internal` | Core WAL replay/frame tests are implementation tests for daemon internals. |

## Audited Paths With No Direct Coupling Entries

| Path | Result |
| --- | --- |
| `crates/beads-rs/src/model/**` | No direct `crate::daemon::...` or `beads_rs::daemon::...` references detected. |
| `crates/beads-rs/src/bin/**` | No direct `crate::daemon::...` or `beads_rs::daemon::...` references detected. |

## Count By Classification

| Classification | Count |
| --- | ---: |
| `shared-core candidate` | 3 |
| `daemon-internal` | 11 |
| **Total coupling entries** | **14** |

## Current Blockers For Daemon Split

1. Non-production targets (`crates/fuzz/**`, `crates/beads_stateright_models/**`) still import `beads_rs::daemon::*` directly.
2. Repl/WAL integration tests rely on daemon-internal types rather than a dedicated daemon testkit boundary.
3. Fixture plumbing (`integration/fixtures/daemon_boundary.rs`) centralizes deep daemon internals, making public path reduction risky without replacement APIs.
4. Store lock tests still consume daemon-owned lock primitives directly; no lower-layer stable contract exists yet.
