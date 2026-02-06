//! Realtime safety limits (normative defaults).

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{Labels, TxnDeltaV1, TxnOpV1};

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

#[derive(Clone, Copy, Debug)]
pub struct LimitsPolicy<'a> {
    limits: &'a Limits,
}

impl Limits {
    pub fn policy(&self) -> LimitsPolicy<'_> {
        LimitsPolicy { limits: self }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LimitViolation {
    #[error("txn ops {got_ops} exceeds max {max_ops}")]
    OpsTooMany { max_ops: usize, got_ops: usize },
    #[error("note content bytes {got_bytes} exceeds max {max_bytes}")]
    NoteTooLarge { max_bytes: usize, got_bytes: usize },
    #[error("note_appends count {got} exceeds max {max}")]
    NoteAppendsTooMany { max: usize, got: usize },
    #[error("labels count {got} exceeds max {max}")]
    LabelsTooMany { max: usize, got: usize },
    #[error("wal record payload bytes {got_bytes} exceeds max {max_bytes}")]
    WalRecordPayloadTooLarge { max_bytes: usize, got_bytes: usize },
    #[error("wal record bytes {got_bytes} exceeds max {max_bytes}")]
    WalRecordTooLarge { max_bytes: usize, got_bytes: usize },
    #[error("jsonl shard bytes {got_bytes} exceeds max {max_bytes}")]
    JsonlShardTooLarge { max_bytes: u64, got_bytes: u64 },
    #[error("jsonl line bytes {got_bytes} exceeds max {max_bytes}")]
    JsonlLineTooLarge { max_bytes: u64, got_bytes: u64 },
    #[error("snapshot entries {got_entries} exceeds max {max_entries}")]
    SnapshotEntriesTooMany {
        max_entries: usize,
        got_entries: usize,
    },
    #[error("json depth {got_depth} exceeds max {max_depth}")]
    JsonDepthExceeded { max_depth: usize, got_depth: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatedNoteContent<'a> {
    content: &'a str,
}

impl<'a> ValidatedNoteContent<'a> {
    pub fn as_str(&self) -> &'a str {
        self.content
    }
}

impl AsRef<str> for ValidatedNoteContent<'_> {
    fn as_ref(&self) -> &str {
        self.content
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatedLabels<'a> {
    labels: &'a Labels,
}

impl<'a> ValidatedLabels<'a> {
    pub fn as_labels(&self) -> &'a Labels {
        self.labels
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatedTxnDeltaLimits<'a> {
    delta: &'a TxnDeltaV1,
    total_ops: usize,
    note_appends: usize,
}

impl<'a> ValidatedTxnDeltaLimits<'a> {
    pub fn delta(&self) -> &'a TxnDeltaV1 {
        self.delta
    }

    pub fn total_ops(&self) -> usize {
        self.total_ops
    }

    pub fn note_appends(&self) -> usize {
        self.note_appends
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatedWalRecordPayload {
    payload_bytes: usize,
}

impl ValidatedWalRecordPayload {
    pub fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatedWalRecordBytes {
    record_bytes: usize,
}

impl ValidatedWalRecordBytes {
    pub fn record_bytes(&self) -> usize {
        self.record_bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatedJsonlShardBytes {
    bytes: u64,
}

impl ValidatedJsonlShardBytes {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatedJsonlLineBytes {
    bytes: u64,
}

impl ValidatedJsonlLineBytes {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatedSnapshotEntries {
    entries: usize,
}

impl ValidatedSnapshotEntries {
    pub fn entries(&self) -> usize {
        self.entries
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatedJsonDepth {
    depth: usize,
}

impl ValidatedJsonDepth {
    pub fn depth(&self) -> usize {
        self.depth
    }
}

impl<'a> LimitsPolicy<'a> {
    pub fn note_content(
        &self,
        content: &'a str,
    ) -> Result<ValidatedNoteContent<'a>, LimitViolation> {
        let got_bytes = content.len();
        if got_bytes > self.limits.max_note_bytes {
            return Err(LimitViolation::NoteTooLarge {
                max_bytes: self.limits.max_note_bytes,
                got_bytes,
            });
        }
        Ok(ValidatedNoteContent { content })
    }

    pub fn labels(&self, labels: &'a Labels) -> Result<ValidatedLabels<'a>, LimitViolation> {
        let got = labels.len();
        if got > self.limits.max_labels_per_bead {
            return Err(LimitViolation::LabelsTooMany {
                max: self.limits.max_labels_per_bead,
                got,
            });
        }
        Ok(ValidatedLabels { labels })
    }

    pub fn txn_delta(
        &self,
        delta: &'a TxnDeltaV1,
    ) -> Result<ValidatedTxnDeltaLimits<'a>, LimitViolation> {
        let total_ops = delta.total_ops();
        if total_ops > self.limits.max_ops_per_txn {
            return Err(LimitViolation::OpsTooMany {
                max_ops: self.limits.max_ops_per_txn,
                got_ops: total_ops,
            });
        }

        let mut note_appends = 0usize;
        for op in delta.iter() {
            if let TxnOpV1::NoteAppend(append) = op {
                note_appends += 1;
                let _ = self.note_content(&append.note.content)?;
            }
        }

        if note_appends > self.limits.max_note_appends_per_txn {
            return Err(LimitViolation::NoteAppendsTooMany {
                max: self.limits.max_note_appends_per_txn,
                got: note_appends,
            });
        }

        Ok(ValidatedTxnDeltaLimits {
            delta,
            total_ops,
            note_appends,
        })
    }

    pub fn max_wal_record_payload_bytes(&self) -> usize {
        self.limits
            .max_wal_record_bytes
            .min(self.limits.max_frame_bytes)
    }

    pub fn max_wal_record_bytes(&self) -> usize {
        self.limits.max_wal_record_bytes
    }

    pub fn wal_record_payload(
        &self,
        payload_len: usize,
    ) -> Result<ValidatedWalRecordPayload, LimitViolation> {
        let max_bytes = self.max_wal_record_payload_bytes();
        if payload_len > max_bytes {
            return Err(LimitViolation::WalRecordPayloadTooLarge {
                max_bytes,
                got_bytes: payload_len,
            });
        }
        Ok(ValidatedWalRecordPayload {
            payload_bytes: payload_len,
        })
    }

    pub fn wal_record_len(
        &self,
        record_len: usize,
    ) -> Result<ValidatedWalRecordBytes, LimitViolation> {
        if record_len > self.limits.max_wal_record_bytes {
            return Err(LimitViolation::WalRecordTooLarge {
                max_bytes: self.limits.max_wal_record_bytes,
                got_bytes: record_len,
            });
        }
        Ok(ValidatedWalRecordBytes {
            record_bytes: record_len,
        })
    }

    pub fn jsonl_shard_bytes(
        &self,
        shard_bytes: u64,
    ) -> Result<ValidatedJsonlShardBytes, LimitViolation> {
        let max_bytes = self.limits.max_jsonl_shard_bytes as u64;
        if shard_bytes > max_bytes {
            return Err(LimitViolation::JsonlShardTooLarge {
                max_bytes,
                got_bytes: shard_bytes,
            });
        }
        Ok(ValidatedJsonlShardBytes { bytes: shard_bytes })
    }

    pub fn jsonl_line_bytes(
        &self,
        line_bytes: usize,
    ) -> Result<ValidatedJsonlLineBytes, LimitViolation> {
        let max_bytes = self.limits.max_jsonl_line_bytes as u64;
        let got_bytes = line_bytes as u64;
        if got_bytes > max_bytes {
            return Err(LimitViolation::JsonlLineTooLarge {
                max_bytes,
                got_bytes,
            });
        }
        Ok(ValidatedJsonlLineBytes { bytes: got_bytes })
    }

    pub fn snapshot_entries(
        &self,
        entries: usize,
    ) -> Result<ValidatedSnapshotEntries, LimitViolation> {
        if entries > self.limits.max_snapshot_entries {
            return Err(LimitViolation::SnapshotEntriesTooMany {
                max_entries: self.limits.max_snapshot_entries,
                got_entries: entries,
            });
        }
        Ok(ValidatedSnapshotEntries { entries })
    }

    pub fn json_depth(&self, depth: usize) -> Result<ValidatedJsonDepth, LimitViolation> {
        if depth > self.limits.max_cbor_depth {
            return Err(LimitViolation::JsonDepthExceeded {
                max_depth: self.limits.max_cbor_depth,
                got_depth: depth,
            });
        }
        Ok(ValidatedJsonDepth { depth })
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
