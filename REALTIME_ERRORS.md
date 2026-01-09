# Realtime Error Codes

This document defines the canonical error codes for the beads realtime protocol.
It is referenced by REALTIME_PLAN.md and serves as the single source of truth for
error semantics.

## Stability Rules

- Error codes are **stable once added**: they are never removed or renamed.
- New codes may be added at any time.
- Clients MUST handle unknown codes gracefully (treat as non-retryable unless `retryable=true`).

## ErrorPayload Shape

All errors use this payload structure:

```
{
  code: string,           // one of the codes below
  message: string,        // human-readable description
  retryable: bool,        // true if client can retry without human intervention
  retry_after_ms?: u64,   // suggested wait before retry (avoid thundering herds)
  details?: object,       // error-specific structured data
  receipt?: DurabilityReceipt  // present on partial success (e.g., durability timeout)
}
```

## Error Codes

### Protocol and Identity

| Code | Retryable | Description |
|------|-----------|-------------|
| `wrong_store` | no | store_id mismatch; connection to wrong store |
| `store_epoch_mismatch` | no | store_epoch differs; requires snapshot/bootstrap |
| `replica_id_collision` | no | peer has same replica_id as local (duplicate store dir) |
| `version_incompatible` | no | protocol version negotiation failed |
| `diverged` | no | same seq but different head sha; chain has forked |

### Operational

| Code | Retryable | Description |
|------|-----------|-------------|
| `overloaded` | yes | server is at capacity; try again later |
| `maintenance_mode` | yes | server in maintenance mode; mutations rejected |
| `durability_timeout` | yes | durability wait timed out; receipt included |
| `durability_unavailable` | no | requested durability class cannot be satisfied (e.g., k > eligible replicas) |

### Replication

| Code | Retryable | Description |
|------|-----------|-------------|
| `snapshot_required` | yes | WAL range pruned or missing; need snapshot |
| `snapshot_expired` | yes | snapshot temp file expired; restart from chunk 0 |
| `unknown_replica` | no | replica_id not in roster (if replicas.toml exists) |
| `subscriber_lagged` | yes | subscribe stream fell too far behind; reconnect |

### Client Request

| Code | Retryable | Description |
|------|-----------|-------------|
| `client_request_id_reuse_mismatch` | no | same client_request_id with different request_sha256 |
| `request_too_large` | no | request exceeds MAX_WAL_RECORD_BYTES; split into multiple requests |
| `invalid_request` | no | malformed or invalid request |

### Data Integrity

| Code | Retryable | Description |
|------|-----------|-------------|
| `corruption` | no | data corruption detected (CRC, hash, or structural) |
| `non_canonical` | no | received bytes are not canonical CBOR |
| `equivocation` | no | same EventId with different sha256 (protocol violation) |

---

## Adding New Codes

When adding a new error code:

1. Add it to the appropriate section in this document
2. Set `retryable` based on whether automated retry is safe
3. Document any `details` fields the error provides
4. Ensure code name is descriptive and uses snake_case
