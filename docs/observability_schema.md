# Observability Field Schema

This document defines canonical field keys for spans/logs so search and correlation
stay consistent across the daemon.

## Canonical keys

- `request_type`
- `repo`
- `remote`
- `store_id`
- `store_epoch`
- `namespace`
- `actor_id`
- `replica_id`
- `origin_replica_id`
- `peer_replica_id`
- `peer_addr`
- `direction`
- `txn_id`
- `client_request_id`
- `trace_id`
- `origin_seq`
- `checkpoint_group`
- `read_consistency`

## Required by context

### IPC request span (`ipc_request`)
- `request_type`
- `repo`
- `namespace` (when provided)
- `actor_id` (mutations)
- `client_request_id` (mutations)
- `read_consistency` (reads)

### Mutation span (`mutation`)
- `store_id`
- `store_epoch`
- `replica_id`
- `actor_id`
- `namespace`
- `txn_id`
- `client_request_id`
- `trace_id`
- `origin_replica_id`
- `origin_seq`

### Replication spans (`repl_peer_loop`, `repl_accept_loop`, `repl_session`)
- `store_id`
- `store_epoch`
- `replica_id`
- `peer_replica_id`
- `peer_addr`
- `direction`

### Git sync span (`sync`)
- `store_id`
- `repo`
- `remote`
- `actor_id`

### Checkpoint span (`checkpoint`)
- `store_id`
- `repo`
- `checkpoint_group`
