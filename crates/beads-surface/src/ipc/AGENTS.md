## Boundary
This subtree owns the IPC wire contract: request/response types, payload flattening, codec, client behavior, and IPC-specific error mapping.
NEVER: put new unsafe spawn or fd-sanitization logic back into `client.rs`; the autostart process boundary was split into `spawn_sanitizer.rs` on purpose.

## Routing
- `ctx.rs` owns flattened repo, mutation, and read context pieces reused across requests. Do not inline `repo` or consistency fields directly into one-off variants.
- `payload.rs` owns per-op payload structs and defaults like claim leases.
- `types.rs` owns `Request`, `AdminOp`, protocol versioning, and response roundtrip guarantees.
- `codec.rs` owns frame-size enforcement and serde framing.
- `client.rs` owns retries, autostart, version handshake, and runtime-dir overrides, but it should stay transport-level rather than grow command semantics.
- When adding a new transport operation, add the reusable ctx/payload pieces first, then wire the variant in `types.rs`, then add roundtrip tests there instead of shaping a daemon-local request/response pair.

## Verification
- `cargo test -p beads-surface ipc::` is the local proof loop for request roundtrips, payload defaults, client behavior, and spawn sanitization in this subtree.
