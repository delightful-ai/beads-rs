## Boundary
This crate owns request/response contracts, client helpers, and shared patch/query types.
NEVER: invent parallel request payloads, patch semantics, or daemon-optional store-admin fallbacks outside this crate just because a caller is nearby.

## Routing
- If a change is about request/response transport compatibility or client-helper behavior, it belongs here.
- `ops.rs` owns `Patch<T>` and `BeadPatch`; extend that typed update contract here instead of open-coding `Option<Option<_>>` behavior in callers.
- `query.rs` owns shared filter semantics for list/count-like reads.
- If a change is about emitted JSON summary/result shapes, it belongs in `beads-api`, not here.
- Compatibility-sensitive IPC rules live under `src/ipc/`; use that child file for the local contract details there.
- For daemon-optional admin/store flows, copy the `StoreAdminCall<T>` plus `call_*_no_autostart` pattern in `src/store_admin.rs` instead of open-coding ping/fallback behavior in commands or backends.

## Verification
- `cargo test -p beads-surface` exercises patch serde, IPC roundtrips, client/autostart behavior, and store-admin fallback logic.
