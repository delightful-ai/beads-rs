//! Realtime safety limits (normative defaults).

use serde::{Deserialize, Serialize};

/// Limits are normative defaults from REALTIME_PLAN.md §0.19.
///
/// Values are intentionally explicit about their units to avoid confusion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_event_batch_events: usize,
    pub max_event_batch_bytes: usize,
    pub max_wal_record_bytes: usize,

    pub max_repl_gap_events: usize,
    pub max_repl_gap_bytes: usize,
    pub repl_gap_timeout_ms: u64,

    pub keepalive_ms: u64,
    pub dead_ms: u64,

    pub wal_segment_max_bytes: usize,
    pub wal_segment_max_age_ms: u64,
    pub wal_guardrail_max_bytes: u64,
    pub wal_guardrail_max_segments: u64,
    pub wal_guardrail_growth_window_ms: u64,
    pub wal_guardrail_growth_max_bytes: u64,

    pub wal_group_commit_max_latency_ms: u64,
    pub wal_group_commit_max_events: usize,
    pub wal_group_commit_max_bytes: usize,
    pub wal_sqlite_checkpoint_interval_ms: u64,

    pub max_repl_ingest_queue_bytes: usize,
    pub max_repl_ingest_queue_events: usize,

    pub max_ipc_inflight_mutations: usize,
    pub max_checkpoint_job_queue: usize,
    pub max_broadcast_subscribers: usize,

    pub event_hot_cache_max_bytes: usize,
    pub event_hot_cache_max_events: usize,

    pub max_events_per_origin_per_batch: usize,
    pub max_bytes_per_origin_per_batch: usize,

    pub max_snapshot_bytes: usize,
    pub max_snapshot_entries: usize,
    pub max_snapshot_entry_bytes: usize,
    pub max_concurrent_snapshots: usize,
    pub max_jsonl_line_bytes: usize,
    pub max_jsonl_shard_bytes: usize,

    pub max_cbor_depth: usize,
    pub max_cbor_map_entries: usize,
    pub max_cbor_array_entries: usize,
    pub max_cbor_bytes_string_len: usize,
    pub max_cbor_text_string_len: usize,

    pub max_repl_ingest_bytes_per_sec: usize,
    pub max_background_io_bytes_per_sec: usize,

    pub hlc_max_forward_drift_ms: u64,

    pub max_note_bytes: usize,
    pub max_ops_per_txn: usize,
    pub max_note_appends_per_txn: usize,
    pub max_labels_per_bead: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 16 * 1024 * 1024,
            max_event_batch_events: 10_000,
            max_event_batch_bytes: 10 * 1024 * 1024,
            max_wal_record_bytes: 16 * 1024 * 1024,

            max_repl_gap_events: 50_000,
            max_repl_gap_bytes: 32 * 1024 * 1024,
            repl_gap_timeout_ms: 30_000,

            keepalive_ms: 5_000,
            dead_ms: 30_000,

            wal_segment_max_bytes: 32 * 1024 * 1024,
            wal_segment_max_age_ms: 60_000,
            wal_guardrail_max_bytes: 8 * 1024 * 1024 * 1024,
            wal_guardrail_max_segments: 10_000,
            wal_guardrail_growth_window_ms: 600_000,
            wal_guardrail_growth_max_bytes: 512 * 1024 * 1024,

            wal_group_commit_max_latency_ms: 2,
            wal_group_commit_max_events: 64,
            wal_group_commit_max_bytes: 1024 * 1024,
            wal_sqlite_checkpoint_interval_ms: 3_600_000,

            max_repl_ingest_queue_bytes: 32 * 1024 * 1024,
            max_repl_ingest_queue_events: 50_000,

            max_ipc_inflight_mutations: 1024,
            max_checkpoint_job_queue: 8,
            max_broadcast_subscribers: 256,

            event_hot_cache_max_bytes: 64 * 1024 * 1024,
            event_hot_cache_max_events: 200_000,

            max_events_per_origin_per_batch: 1024,
            max_bytes_per_origin_per_batch: 1024 * 1024,

            max_snapshot_bytes: 512 * 1024 * 1024,
            max_snapshot_entries: 200_000,
            max_snapshot_entry_bytes: 64 * 1024 * 1024,
            max_concurrent_snapshots: 1,
            max_jsonl_line_bytes: 4 * 1024 * 1024,
            max_jsonl_shard_bytes: 256 * 1024 * 1024,

            max_cbor_depth: 32,
            max_cbor_map_entries: 10_000,
            max_cbor_array_entries: 10_000,
            max_cbor_bytes_string_len: 16 * 1024 * 1024,
            max_cbor_text_string_len: 16 * 1024 * 1024,

            max_repl_ingest_bytes_per_sec: 64 * 1024 * 1024,
            max_background_io_bytes_per_sec: 128 * 1024 * 1024,

            hlc_max_forward_drift_ms: 600_000,

            max_note_bytes: 64 * 1024,
            max_ops_per_txn: 10_000,
            max_note_appends_per_txn: 1_000,
            max_labels_per_bead: 256,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Limits;

    #[test]
    fn limits_defaults_match_plan() {
        let limits = Limits::default();
        assert_eq!(limits.max_frame_bytes, 16 * 1024 * 1024);
        assert_eq!(limits.max_event_batch_events, 10_000);
        assert_eq!(limits.max_event_batch_bytes, 10 * 1024 * 1024);
        assert_eq!(limits.max_wal_record_bytes, 16 * 1024 * 1024);
        assert_eq!(limits.max_repl_gap_events, 50_000);
        assert_eq!(limits.max_repl_gap_bytes, 32 * 1024 * 1024);
        assert_eq!(limits.repl_gap_timeout_ms, 30_000);
        assert_eq!(limits.keepalive_ms, 5_000);
        assert_eq!(limits.dead_ms, 30_000);
        assert_eq!(limits.wal_segment_max_bytes, 32 * 1024 * 1024);
        assert_eq!(limits.wal_segment_max_age_ms, 60_000);
        assert_eq!(limits.wal_guardrail_max_bytes, 8 * 1024 * 1024 * 1024);
        assert_eq!(limits.wal_guardrail_max_segments, 10_000);
        assert_eq!(limits.wal_guardrail_growth_window_ms, 600_000);
        assert_eq!(limits.wal_guardrail_growth_max_bytes, 512 * 1024 * 1024);
        assert_eq!(limits.wal_group_commit_max_latency_ms, 2);
        assert_eq!(limits.wal_group_commit_max_events, 64);
        assert_eq!(limits.wal_group_commit_max_bytes, 1024 * 1024);
        assert_eq!(limits.wal_sqlite_checkpoint_interval_ms, 3_600_000);
        assert_eq!(limits.max_repl_ingest_queue_bytes, 32 * 1024 * 1024);
        assert_eq!(limits.max_repl_ingest_queue_events, 50_000);
        assert_eq!(limits.max_ipc_inflight_mutations, 1024);
        assert_eq!(limits.max_checkpoint_job_queue, 8);
        assert_eq!(limits.max_broadcast_subscribers, 256);
        assert_eq!(limits.event_hot_cache_max_bytes, 64 * 1024 * 1024);
        assert_eq!(limits.event_hot_cache_max_events, 200_000);
        assert_eq!(limits.max_events_per_origin_per_batch, 1024);
        assert_eq!(limits.max_bytes_per_origin_per_batch, 1024 * 1024);
        assert_eq!(limits.max_snapshot_bytes, 512 * 1024 * 1024);
        assert_eq!(limits.max_snapshot_entries, 200_000);
        assert_eq!(limits.max_snapshot_entry_bytes, 64 * 1024 * 1024);
        assert_eq!(limits.max_concurrent_snapshots, 1);
        assert_eq!(limits.max_jsonl_line_bytes, 4 * 1024 * 1024);
        assert_eq!(limits.max_jsonl_shard_bytes, 256 * 1024 * 1024);
        assert_eq!(limits.max_cbor_depth, 32);
        assert_eq!(limits.max_cbor_map_entries, 10_000);
        assert_eq!(limits.max_cbor_array_entries, 10_000);
        assert_eq!(limits.max_cbor_bytes_string_len, 16 * 1024 * 1024);
        assert_eq!(limits.max_cbor_text_string_len, 16 * 1024 * 1024);
        assert_eq!(limits.max_repl_ingest_bytes_per_sec, 64 * 1024 * 1024);
        assert_eq!(limits.max_background_io_bytes_per_sec, 128 * 1024 * 1024);
        assert_eq!(limits.hlc_max_forward_drift_ms, 600_000);
        assert_eq!(limits.max_note_bytes, 64 * 1024);
        assert_eq!(limits.max_ops_per_txn, 10_000);
        assert_eq!(limits.max_note_appends_per_txn, 1_000);
        assert_eq!(limits.max_labels_per_bead, 256);
    }
}
