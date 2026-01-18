//! Minimal metrics emission helpers.
//!
//! These helpers emit structured metrics via tracing by default. A test sink can
//! be installed to capture emissions in unit tests.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

const HISTOGRAM_MAX_SAMPLES: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetricValue {
    Counter(u64),
    Gauge(u64),
    Histogram(u64),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MetricLabel {
    pub key: &'static str,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricEvent {
    pub name: &'static str,
    pub value: MetricValue,
    pub labels: Vec<MetricLabel>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MetricKey {
    name: &'static str,
    labels: Vec<MetricLabel>,
}

impl MetricKey {
    fn new(name: &'static str, labels: Vec<MetricLabel>) -> Self {
        let mut labels = labels;
        labels.sort();
        Self { name, labels }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricSample {
    pub name: &'static str,
    pub value: u64,
    pub labels: Vec<MetricLabel>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricHistogram {
    pub name: &'static str,
    pub count: u64,
    pub min: Option<u64>,
    pub max: Option<u64>,
    pub p50: Option<u64>,
    pub p95: Option<u64>,
    pub labels: Vec<MetricLabel>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub counters: Vec<MetricSample>,
    pub gauges: Vec<MetricSample>,
    pub histograms: Vec<MetricHistogram>,
}

pub trait MetricSink: Send + Sync {
    fn record(&self, event: MetricEvent);
}

struct TracingSink;

impl MetricSink for TracingSink {
    fn record(&self, event: MetricEvent) {
        match event.value {
            MetricValue::Counter(value) => {
                tracing::info!(
                    target: "metrics",
                    metric = event.name,
                    value,
                    labels = ?event.labels
                );
            }
            MetricValue::Gauge(value) => {
                tracing::info!(
                    target: "metrics",
                    metric = event.name,
                    value,
                    labels = ?event.labels
                );
            }
            MetricValue::Histogram(value) => {
                tracing::info!(
                    target: "metrics",
                    metric = event.name,
                    value,
                    labels = ?event.labels
                );
            }
        }
    }
}

static METRIC_SINK: std::sync::OnceLock<RwLock<Arc<dyn MetricSink>>> = std::sync::OnceLock::new();
static METRIC_STATE: std::sync::OnceLock<Mutex<MetricsState>> = std::sync::OnceLock::new();

fn sink() -> Arc<dyn MetricSink> {
    METRIC_SINK
        .get_or_init(|| RwLock::new(Arc::new(TracingSink)))
        .read()
        .expect("metrics sink lock poisoned")
        .clone()
}

pub fn set_sink(sink: Arc<dyn MetricSink>) {
    let lock = METRIC_SINK.get_or_init(|| RwLock::new(Arc::new(TracingSink)));
    *lock.write().expect("metrics sink lock poisoned") = sink;
}

fn state() -> &'static Mutex<MetricsState> {
    METRIC_STATE.get_or_init(|| Mutex::new(MetricsState::default()))
}

fn emit(name: &'static str, value: MetricValue, labels: Vec<MetricLabel>) {
    record_state(name, &value, &labels);
    sink().record(MetricEvent {
        name,
        value,
        labels,
    });
}

fn duration_ms(duration: Duration) -> u64 {
    let ms = duration.as_millis();
    u64::try_from(ms).unwrap_or(u64::MAX)
}

pub fn wal_append_ok(duration: Duration) {
    emit("wal_append_ok", MetricValue::Counter(1), Vec::new());
    emit(
        "wal_append_duration",
        MetricValue::Histogram(duration_ms(duration)),
        Vec::new(),
    );
}

pub fn wal_append_err(duration: Duration) {
    emit("wal_append_err", MetricValue::Counter(1), Vec::new());
    emit(
        "wal_append_duration",
        MetricValue::Histogram(duration_ms(duration)),
        Vec::new(),
    );
}

pub fn wal_fsync_ok(duration: Duration) {
    emit("wal_fsync_ok", MetricValue::Counter(1), Vec::new());
    emit(
        "wal_fsync_duration",
        MetricValue::Histogram(duration_ms(duration)),
        Vec::new(),
    );
}

pub fn wal_fsync_err(duration: Duration) {
    emit("wal_fsync_err", MetricValue::Counter(1), Vec::new());
    emit(
        "wal_fsync_duration",
        MetricValue::Histogram(duration_ms(duration)),
        Vec::new(),
    );
}

pub fn wal_index_checkpoint_ok(duration: Duration) {
    emit(
        "wal_index_checkpoint_ok",
        MetricValue::Counter(1),
        Vec::new(),
    );
    emit(
        "wal_index_checkpoint_duration",
        MetricValue::Histogram(duration_ms(duration)),
        Vec::new(),
    );
}

pub fn wal_index_checkpoint_err(duration: Duration) {
    emit(
        "wal_index_checkpoint_err",
        MetricValue::Counter(1),
        Vec::new(),
    );
    emit(
        "wal_index_checkpoint_duration",
        MetricValue::Histogram(duration_ms(duration)),
        Vec::new(),
    );
}

pub fn apply_ok(duration: Duration) {
    emit("apply_ok", MetricValue::Counter(1), Vec::new());
    emit(
        "apply_duration",
        MetricValue::Histogram(duration_ms(duration)),
        Vec::new(),
    );
}

pub fn apply_err(duration: Duration) {
    emit("apply_err", MetricValue::Counter(1), Vec::new());
    emit(
        "apply_duration",
        MetricValue::Histogram(duration_ms(duration)),
        Vec::new(),
    );
}

pub fn repl_events_in(count: usize) {
    emit(
        "repl_events_in",
        MetricValue::Counter(count as u64),
        Vec::new(),
    );
}

pub fn repl_events_out(count: usize) {
    emit(
        "repl_events_out",
        MetricValue::Counter(count as u64),
        Vec::new(),
    );
}

pub fn repl_pending_dropped_events(direction: &'static str, count: usize) {
    emit(
        "repl_pending_dropped_events",
        MetricValue::Counter(count as u64),
        vec![MetricLabel {
            key: "direction",
            value: direction.to_string(),
        }],
    );
}

pub fn repl_pending_dropped_bytes(direction: &'static str, count: usize) {
    emit(
        "repl_pending_dropped_bytes",
        MetricValue::Counter(count as u64),
        vec![MetricLabel {
            key: "direction",
            value: direction.to_string(),
        }],
    );
}

pub fn checkpoint_export_ok(duration: Duration) {
    emit("checkpoint_export_ok", MetricValue::Counter(1), Vec::new());
    emit(
        "checkpoint_duration",
        MetricValue::Histogram(duration_ms(duration)),
        Vec::new(),
    );
}

pub fn checkpoint_export_err(duration: Duration) {
    emit("checkpoint_export_err", MetricValue::Counter(1), Vec::new());
    emit(
        "checkpoint_duration",
        MetricValue::Histogram(duration_ms(duration)),
        Vec::new(),
    );
}

pub fn repl_ingest_throttle(wait: Duration, bytes: u64) {
    emit(
        "repl_ingest_throttle_ms",
        MetricValue::Histogram(duration_ms(wait)),
        Vec::new(),
    );
    emit(
        "repl_ingest_throttle_bytes",
        MetricValue::Counter(bytes),
        Vec::new(),
    );
}

pub fn background_io_throttle(wait: Duration, bytes: u64) {
    emit(
        "background_io_throttle_ms",
        MetricValue::Histogram(duration_ms(wait)),
        Vec::new(),
    );
    emit(
        "background_io_throttle_bytes",
        MetricValue::Counter(bytes),
        Vec::new(),
    );
}

pub fn set_ipc_inflight(value: usize) {
    emit("ipc_inflight", MetricValue::Gauge(value as u64), Vec::new());
}

pub fn set_repl_queue_bytes(value: u64) {
    emit("repl_queue_bytes", MetricValue::Gauge(value), Vec::new());
}

pub fn set_repl_queue_events(value: u64) {
    emit("repl_queue_events", MetricValue::Gauge(value), Vec::new());
}

pub fn set_repl_peer_lag(
    peer: crate::core::ReplicaId,
    namespace: &crate::core::NamespaceId,
    lag: u64,
) {
    emit(
        "repl_peer_lag",
        MetricValue::Gauge(lag),
        vec![
            MetricLabel {
                key: "peer",
                value: peer.to_string(),
            },
            MetricLabel {
                key: "namespace",
                value: namespace.to_string(),
            },
        ],
    );
}

pub fn set_checkpoint_queue_depth(value: usize) {
    emit(
        "checkpoint_queue_depth",
        MetricValue::Gauge(value as u64),
        Vec::new(),
    );
}

pub fn set_wal_bytes_total(namespace: &crate::core::NamespaceId, value: u64) {
    emit(
        "wal_bytes_total",
        MetricValue::Gauge(value),
        vec![MetricLabel {
            key: "namespace",
            value: namespace.to_string(),
        }],
    );
}

pub fn set_wal_segments_total(namespace: &crate::core::NamespaceId, value: u64) {
    emit(
        "wal_segments_total",
        MetricValue::Gauge(value),
        vec![MetricLabel {
            key: "namespace",
            value: namespace.to_string(),
        }],
    );
}

pub fn set_wal_growth_bytes_per_sec(
    namespace: &crate::core::NamespaceId,
    window_ms: u64,
    value: u64,
) {
    emit(
        "wal_growth_bytes_per_sec",
        MetricValue::Gauge(value),
        vec![
            MetricLabel {
                key: "namespace",
                value: namespace.to_string(),
            },
            MetricLabel {
                key: "window_ms",
                value: window_ms.to_string(),
            },
        ],
    );
}

pub fn set_wal_growth_segments_per_sec(
    namespace: &crate::core::NamespaceId,
    window_ms: u64,
    value: u64,
) {
    emit(
        "wal_growth_segments_per_sec",
        MetricValue::Gauge(value),
        vec![
            MetricLabel {
                key: "namespace",
                value: namespace.to_string(),
            },
            MetricLabel {
                key: "window_ms",
                value: window_ms.to_string(),
            },
        ],
    );
}

pub fn scrub_ok() {
    emit("scrub_ok", MetricValue::Counter(1), Vec::new());
}

pub fn scrub_err() {
    emit("scrub_err", MetricValue::Counter(1), Vec::new());
}

pub fn scrub_records_checked(count: u64) {
    emit(
        "scrub_records_checked",
        MetricValue::Counter(count),
        Vec::new(),
    );
}

pub fn snapshot() -> MetricsSnapshot {
    let guard = state().lock().expect("metrics state lock poisoned");
    guard.snapshot()
}

fn record_state(name: &'static str, value: &MetricValue, labels: &[MetricLabel]) {
    let mut guard = state().lock().expect("metrics state lock poisoned");
    let key = MetricKey::new(name, labels.to_vec());
    match value {
        MetricValue::Counter(delta) => {
            let entry = guard.counters.entry(key).or_insert(0);
            *entry = entry.saturating_add(*delta);
        }
        MetricValue::Gauge(value) => {
            guard.gauges.insert(key, *value);
        }
        MetricValue::Histogram(value) => {
            guard.histograms.entry(key).or_default().record(*value);
        }
    }
}

#[derive(Default)]
struct MetricsState {
    counters: BTreeMap<MetricKey, u64>,
    gauges: BTreeMap<MetricKey, u64>,
    histograms: BTreeMap<MetricKey, HistogramState>,
}

impl MetricsState {
    fn snapshot(&self) -> MetricsSnapshot {
        let counters = self
            .counters
            .iter()
            .map(|(key, value)| MetricSample {
                name: key.name,
                value: *value,
                labels: key.labels.clone(),
            })
            .collect();

        let gauges = self
            .gauges
            .iter()
            .map(|(key, value)| MetricSample {
                name: key.name,
                value: *value,
                labels: key.labels.clone(),
            })
            .collect();

        let histograms = self
            .histograms
            .iter()
            .map(|(key, histogram)| {
                let mut values: Vec<u64> = histogram.samples.iter().copied().collect();
                values.sort_unstable();
                let min = values.first().copied();
                let max = values.last().copied();
                let p50 = percentile(&values, 0.50);
                let p95 = percentile(&values, 0.95);
                MetricHistogram {
                    name: key.name,
                    count: histogram.count,
                    min,
                    max,
                    p50,
                    p95,
                    labels: key.labels.clone(),
                }
            })
            .collect();

        MetricsSnapshot {
            counters,
            gauges,
            histograms,
        }
    }
}

#[derive(Default)]
struct HistogramState {
    samples: VecDeque<u64>,
    count: u64,
}

impl HistogramState {
    fn record(&mut self, value: u64) {
        self.count = self.count.saturating_add(1);
        if self.samples.len() >= HISTOGRAM_MAX_SAMPLES {
            self.samples.pop_front();
        }
        self.samples.push_back(value);
    }
}

fn percentile(sorted: &[u64], percentile: f64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let p = percentile.clamp(0.0, 1.0);
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted.get(idx).copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct TestSink {
        events: Mutex<Vec<MetricEvent>>,
    }

    impl MetricSink for TestSink {
        fn record(&self, event: MetricEvent) {
            self.events.lock().expect("metrics lock").push(event);
        }
    }

    #[test]
    fn emits_counters_and_histograms() {
        let sink = Arc::new(TestSink::default());
        set_sink(sink.clone());

        wal_append_ok(Duration::from_millis(12));
        wal_fsync_err(Duration::from_millis(7));
        wal_index_checkpoint_ok(Duration::from_millis(5));
        apply_ok(Duration::from_millis(3));
        set_checkpoint_queue_depth(4);
        repl_ingest_throttle(Duration::from_millis(5), 1024);
        background_io_throttle(Duration::from_millis(7), 2048);

        let events = sink.events.lock().expect("metrics lock");
        assert!(events.iter().any(|e| e.name == "wal_append_ok"));
        assert!(events.iter().any(|e| e.name == "wal_append_duration"));
        assert!(events.iter().any(|e| e.name == "wal_fsync_err"));
        assert!(events.iter().any(|e| e.name == "wal_index_checkpoint_ok"));
        assert!(
            events
                .iter()
                .any(|e| e.name == "wal_index_checkpoint_duration")
        );
        assert!(events.iter().any(|e| e.name == "apply_ok"));
        assert!(events.iter().any(|e| e.name == "apply_duration"));
        assert!(events.iter().any(|e| e.name == "checkpoint_queue_depth"));
        assert!(events.iter().any(|e| e.name == "repl_ingest_throttle_ms"));
        assert!(
            events
                .iter()
                .any(|e| e.name == "repl_ingest_throttle_bytes")
        );
        assert!(events.iter().any(|e| e.name == "background_io_throttle_ms"));
        assert!(
            events
                .iter()
                .any(|e| e.name == "background_io_throttle_bytes")
        );
    }

    #[test]
    fn snapshot_collects_metrics() {
        wal_append_ok(Duration::from_millis(4));
        wal_fsync_ok(Duration::from_millis(2));
        set_ipc_inflight(3);

        let snapshot = snapshot();
        assert!(snapshot.counters.iter().any(|m| m.name == "wal_append_ok"));
        assert!(
            snapshot
                .histograms
                .iter()
                .any(|m| m.name == "wal_append_duration")
        );
        assert!(snapshot.gauges.iter().any(|m| m.name == "ipc_inflight"));
    }
}
