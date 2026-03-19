## Boundary
This crate holds thin byte-in fuzz entrypoints over low-level decode seams.
Depends on: decoder surfaces in `beads-core` and `beads-daemon-core`.
NEVER: turn fuzz targets into general integration coverage.

## Routing
- Existing targets are `wal_decode`, `event_body_decode`, and `repl_message_decode`; add new fuzz entrypoints under `fuzz_targets/` only when a new low-level decoder boundary appears.
- Keep each target focused on one low-level input boundary or decode surface.

## Local rules
- Keep the target body thin and route into existing crate code.
- Copy the current target shape in `fuzz_targets/*.rs`: raw bytes in, existing decoder called, no extra harness logic.
- Do not pull `beads-rs` package seams or daemon runtime orchestration into fuzz targets when `beads-core` or `beads-daemon-core` already owns the parser/decoder boundary.
- Avoid filesystem or network side effects in target logic.
- Use `cargo fuzz list` to confirm the target is still declared.
- Use `cargo fuzz build <target>` for the touched target before you call the change done.
