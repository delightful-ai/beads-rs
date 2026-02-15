use super::*;

pub(super) fn collect_namespaces(
    store: &crate::daemon::store::runtime::StoreRuntime,
) -> BTreeSet<NamespaceId> {
    let mut namespaces = BTreeSet::new();
    namespaces.extend(store.policies.keys().cloned());
    namespaces.extend(store.watermarks_applied.namespaces().cloned());
    namespaces.extend(store.watermarks_durable.namespaces().cloned());
    namespaces
}

pub(super) struct WalStatusReport {
    pub(super) namespaces: Vec<AdminWalNamespace>,
    pub(super) warnings: Vec<AdminWalWarning>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct WalSegmentStats {
    pub(super) created_at_ms: u64,
    pub(super) bytes: u64,
}

pub(super) fn build_wal_status(
    store: &crate::daemon::store::runtime::StoreRuntime,
    namespaces: &BTreeSet<NamespaceId>,
    limits: &Limits,
    now_ms: u64,
) -> Result<WalStatusReport, OpError> {
    let reader = store.wal_index.reader();
    let mut out = Vec::new();
    let mut warnings = Vec::new();
    let store_dir = paths::store_dir(store.meta.store_id());
    for namespace in namespaces {
        let mut segments = reader
            .list_segments(namespace)
            .map_err(|err| OpError::StoreRuntime(Box::new(StoreRuntimeError::WalIndex(err))))?;
        segments.sort_by_key(|segment| (segment.created_at_ms(), segment.segment_id()));
        let mut segment_infos = Vec::new();
        let mut segment_stats = Vec::new();
        let mut total_bytes = 0u64;
        for segment in segments {
            let resolved_path = resolve_segment_path(&store_dir, segment.segment_path());
            let bytes = segment_bytes(&resolved_path, segment.final_len());
            if let Some(value) = bytes {
                total_bytes = total_bytes.saturating_add(value);
            }
            segment_stats.push(WalSegmentStats {
                created_at_ms: segment.created_at_ms(),
                bytes: bytes.unwrap_or(0),
            });
            segment_infos.push(AdminWalSegment {
                segment_id: segment.segment_id(),
                created_at_ms: segment.created_at_ms(),
                last_indexed_offset: segment.last_indexed_offset(),
                sealed: segment.is_sealed(),
                final_len: segment.final_len(),
                bytes,
                path: resolved_path.to_string_lossy().to_string(),
            });
        }
        let segment_count = segment_infos.len() as u64;
        let growth = build_wal_growth(
            &segment_stats,
            limits.wal_guardrail_growth_window_ms,
            now_ms,
        );
        record_wal_metrics(namespace, total_bytes, segment_count, &growth);
        warnings.extend(wal_guardrail_warnings(
            namespace,
            total_bytes,
            segment_count,
            &growth,
            limits,
        ));
        out.push(AdminWalNamespace {
            namespace: namespace.clone(),
            segment_count: segment_infos.len(),
            total_bytes,
            growth,
            segments: segment_infos,
        });
    }
    Ok(WalStatusReport {
        namespaces: out,
        warnings,
    })
}

pub(super) fn build_wal_growth(
    segment_stats: &[WalSegmentStats],
    window_ms: u64,
    now_ms: u64,
) -> AdminWalGrowth {
    if window_ms == 0 {
        return AdminWalGrowth {
            window_ms,
            segments: 0,
            bytes: 0,
            segments_per_sec: 0,
            bytes_per_sec: 0,
        };
    }
    let cutoff = now_ms.saturating_sub(window_ms);
    let mut segments = 0u64;
    let mut bytes = 0u64;
    for segment in segment_stats {
        if segment.created_at_ms >= cutoff {
            segments = segments.saturating_add(1);
            bytes = bytes.saturating_add(segment.bytes);
        }
    }
    let denom_ms = window_ms.max(1);
    let segments_per_sec = segments.saturating_mul(1000) / denom_ms;
    let bytes_per_sec = bytes.saturating_mul(1000) / denom_ms;
    AdminWalGrowth {
        window_ms,
        segments,
        bytes,
        segments_per_sec,
        bytes_per_sec,
    }
}

pub(super) fn wal_guardrail_warnings(
    namespace: &NamespaceId,
    total_bytes: u64,
    segment_count: u64,
    growth: &AdminWalGrowth,
    limits: &Limits,
) -> Vec<AdminWalWarning> {
    let mut warnings = Vec::new();
    if limits.wal_guardrail_max_bytes > 0 && total_bytes > limits.wal_guardrail_max_bytes {
        warnings.push(AdminWalWarning {
            namespace: namespace.clone(),
            kind: AdminWalWarningKind::TotalBytesExceeded,
            observed: total_bytes,
            limit: limits.wal_guardrail_max_bytes,
            window_ms: None,
        });
    }
    if limits.wal_guardrail_max_segments > 0 && segment_count > limits.wal_guardrail_max_segments {
        warnings.push(AdminWalWarning {
            namespace: namespace.clone(),
            kind: AdminWalWarningKind::SegmentCountExceeded,
            observed: segment_count,
            limit: limits.wal_guardrail_max_segments,
            window_ms: None,
        });
    }
    if limits.wal_guardrail_growth_max_bytes > 0
        && growth.window_ms > 0
        && growth.bytes > limits.wal_guardrail_growth_max_bytes
    {
        warnings.push(AdminWalWarning {
            namespace: namespace.clone(),
            kind: AdminWalWarningKind::GrowthBytesExceeded,
            observed: growth.bytes,
            limit: limits.wal_guardrail_growth_max_bytes,
            window_ms: Some(growth.window_ms),
        });
    }
    warnings
}

fn record_wal_metrics(
    namespace: &NamespaceId,
    total_bytes: u64,
    segment_count: u64,
    growth: &AdminWalGrowth,
) {
    crate::daemon::metrics::set_wal_bytes_total(namespace, total_bytes);
    crate::daemon::metrics::set_wal_segments_total(namespace, segment_count);
    crate::daemon::metrics::set_wal_growth_bytes_per_sec(
        namespace,
        growth.window_ms,
        growth.bytes_per_sec,
    );
    crate::daemon::metrics::set_wal_growth_segments_per_sec(
        namespace,
        growth.window_ms,
        growth.segments_per_sec,
    );
}

fn segment_bytes(path: &Path, final_len: Option<u64>) -> Option<u64> {
    final_len.or_else(|| fs::metadata(path).ok().map(|m| m.len()))
}

fn resolve_segment_path(store_dir: &Path, segment_path: &Path) -> PathBuf {
    if segment_path.is_absolute() {
        segment_path.to_path_buf()
    } else {
        store_dir.join(segment_path)
    }
}

pub(super) fn build_replication_status(
    store: &crate::daemon::store::runtime::StoreRuntime,
    namespaces: &BTreeSet<NamespaceId>,
) -> Vec<AdminReplicationPeer> {
    let local_replica = store.meta.replica_id;
    let snapshots = store
        .peer_acks
        .lock()
        .expect("peer ack lock poisoned")
        .snapshot();

    snapshots
        .into_iter()
        .map(|snapshot| {
            let lag_by_namespace = namespaces
                .iter()
                .map(|namespace| {
                    let local_durable =
                        seq_for(&store.watermarks_durable, namespace, &local_replica);
                    let peer_durable = seq_for(&snapshot.durable, namespace, &local_replica);
                    let local_applied =
                        seq_for(&store.watermarks_applied, namespace, &local_replica);
                    let peer_applied = seq_for(&snapshot.applied, namespace, &local_replica);
                    AdminReplicationNamespace {
                        namespace: namespace.clone(),
                        local_durable_seq: local_durable,
                        peer_durable_seq: peer_durable,
                        durable_lag: local_durable.saturating_sub(peer_durable),
                        local_applied_seq: local_applied,
                        peer_applied_seq: peer_applied,
                        applied_lag: local_applied.saturating_sub(peer_applied),
                    }
                })
                .collect();

            AdminReplicationPeer {
                peer: snapshot.peer,
                last_ack_at_ms: snapshot.last_ack_at_ms,
                diverged: snapshot.diverged,
                lag_by_namespace,
                watermarks_durable: snapshot.durable,
                watermarks_applied: snapshot.applied,
            }
        })
        .collect()
}

pub(super) fn build_replica_liveness(
    store: &crate::daemon::store::runtime::StoreRuntime,
) -> Vec<AdminReplicaLiveness> {
    let mut rows = match store.wal_index.reader().load_replica_liveness() {
        Ok(rows) => rows,
        Err(err) => {
            tracing::warn!(
                "replica liveness load failed for {}: {err}",
                store.meta.store_id()
            );
            return Vec::new();
        }
    };

    rows.sort_by_key(|row| row.replica_id);
    rows.into_iter()
        .map(|row| AdminReplicaLiveness {
            replica_id: row.replica_id,
            last_seen_ms: row.last_seen_ms,
            last_handshake_ms: row.last_handshake_ms,
            role: row.role.role(),
            durability_eligible: row.role.durability_eligible(),
        })
        .collect()
}

pub(super) fn build_checkpoint_status(
    snapshots: Vec<crate::daemon::checkpoint_scheduler::CheckpointGroupSnapshot>,
) -> Vec<AdminCheckpointGroup> {
    snapshots
        .into_iter()
        .map(|snapshot| AdminCheckpointGroup {
            group: snapshot.group,
            namespaces: snapshot.namespaces,
            git_ref: snapshot.git_ref,
            dirty: snapshot.dirty,
            in_flight: snapshot.in_flight,
            last_checkpoint_wall_ms: snapshot.last_checkpoint_wall_ms,
        })
        .collect()
}

pub(super) fn rebuild_stats(stats: ReplayStats) -> AdminRebuildIndexStats {
    AdminRebuildIndexStats {
        segments_scanned: stats.segments_scanned,
        records_indexed: stats.records_indexed,
        segments_truncated: stats.segments_truncated,
        tail_truncations: stats
            .tail_truncations
            .into_iter()
            .map(|truncation| AdminRebuildIndexTruncation {
                namespace: truncation.namespace,
                segment_id: truncation.segment_id,
                truncated_from_offset: truncation.truncated_from_offset,
            })
            .collect(),
    }
}

fn seq_for<K>(watermarks: &Watermarks<K>, namespace: &NamespaceId, origin: &ReplicaId) -> u64 {
    watermarks
        .get(namespace, origin)
        .map(|watermark| watermark.seq().get())
        .unwrap_or(0)
}

pub(super) fn build_metrics_output(snapshot: MetricsSnapshot) -> AdminMetricsOutput {
    AdminMetricsOutput {
        counters: snapshot
            .counters
            .into_iter()
            .map(to_metric_sample)
            .collect(),
        gauges: snapshot.gauges.into_iter().map(to_metric_sample).collect(),
        histograms: snapshot
            .histograms
            .into_iter()
            .map(to_metric_histogram)
            .collect(),
    }
}

fn to_metric_sample(sample: MetricSample) -> AdminMetricSample {
    AdminMetricSample {
        name: sample.name.to_string(),
        value: sample.value,
        labels: to_metric_labels(sample.labels),
    }
}

fn to_metric_histogram(sample: MetricHistogram) -> AdminMetricHistogram {
    AdminMetricHistogram {
        name: sample.name.to_string(),
        count: sample.count,
        min: sample.min,
        max: sample.max,
        p50: sample.p50,
        p95: sample.p95,
        labels: to_metric_labels(sample.labels),
    }
}

fn to_metric_labels(labels: Vec<MetricLabel>) -> Vec<AdminMetricLabel> {
    labels
        .into_iter()
        .map(|label| AdminMetricLabel {
            key: label.key.to_string(),
            value: label.value,
        })
        .collect()
}

pub(super) fn clock_anomaly_output(anomaly: Option<ClockAnomaly>) -> Option<AdminClockAnomaly> {
    anomaly.map(|anomaly| AdminClockAnomaly {
        at_wall_ms: anomaly.wall_ms,
        kind: match anomaly.kind {
            ClockAnomalyKind::ForwardJumpClamped => AdminClockAnomalyKind::ForwardJumpClamped,
        },
        delta_ms: anomaly.delta_ms,
    })
}
