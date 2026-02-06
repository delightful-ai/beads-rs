# IPC Protocol

## Overview
The daemon speaks newline-delimited JSON (NDJSON) over a Unix socket. Each request is a single
JSON object terminated by `\n`. Each response is either:

- `{"ok": ...}\n`
- `{"err": {"code": "...", "message": "..."}}\n`

The protocol version is tracked in `crates/beads-surface/src/ipc/types.rs` as
`IPC_PROTOCOL_VERSION`.

Current version: **3**.

## Requests
Requests are tagged by `op`:

```json
{"op": "create", ...}
```

Context fields are flattened into the request (e.g. `repo`, `namespace`, `require_min_seen`).

## Admin requests
Admin requests now use a single `op` and an `admin_op` tag:

```json
{"op": "admin", "admin_op": "status", "repo": "/path/to/repo"}
```

The `admin_op` value selects the operation, and the remaining fields are the same as before.
Examples:

- `status`, `metrics`, `doctor`, `scrub`, `fingerprint`
- `flush`, `checkpoint_wait`
- `reload_policies`, `reload_limits`, `reload_replication`
- `rotate_replica_id`, `maintenance_mode`, `rebuild_index`

Each admin operation includes the same flattened context and payload fields it had prior to the
nesting (e.g. `repo`, optional read consistency fields, and any payload fields like
`verify_checkpoint_cache` or `namespace`).

### Wire format change
The admin wire format changed from:

```json
{"op": "admin_status", "repo": "/path/to/repo"}
```

to:

```json
{"op": "admin", "admin_op": "status", "repo": "/path/to/repo"}
```

This is a breaking change and is the reason for the protocol bump to version 3.
