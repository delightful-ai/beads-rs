//! Minimal metrics emission helpers.
//!
//! These helpers emit structured metrics via tracing by default. A test sink can
//! be installed to capture emissions in unit tests.

use std::sync::{Arc, RwLock};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetricValue {
    Counter(u64),
    Gauge(u64),
    Histogram(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

static METRIC_SINK: std::sync::OnceLock<RwLock<Arc<dyn MetricSink>>> =
    std::sync::OnceLock::new();

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

fn emit(name: &'static str, value: MetricValue, labels: Vec<MetricLabel>) {
    sink().record(MetricEvent { name, value, labels });
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
    emit("repl_events_in", MetricValue::Counter(count as u64), Vec::new());
}

pub fn repl_events_out(count: usize) {
    emit("repl_events_out", MetricValue::Counter(count as u64), Vec::new());
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

pub fn set_ipc_inflight(value: usize) {
    emit("ipc_inflight", MetricValue::Gauge(value as u64), Vec::new());
}

pub fn set_repl_queue_bytes(value: u64) {
    emit("repl_queue_bytes", MetricValue::Gauge(value), Vec::new());
}

pub fn set_repl_queue_events(value: u64) {
    emit("repl_queue_events", MetricValue::Gauge(value), Vec::new());
}

pub fn set_repl_peer_lag(peer: crate::core::ReplicaId, lag_ms: u64) {
    emit(
        "repl_peer_lag_ms",
        MetricValue::Gauge(lag_ms),
        vec![MetricLabel {
            key: "peer",
            value: peer.to_string(),
        }],
    );
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
        apply_ok(Duration::from_millis(3));

        let events = sink.events.lock().expect("metrics lock");
        assert!(events.iter().any(|e| e.name == "wal_append_ok"));
        assert!(events.iter().any(|e| e.name == "wal_append_duration"));
        assert!(events.iter().any(|e| e.name == "wal_fsync_err"));
        assert!(events.iter().any(|e| e.name == "apply_ok"));
        assert!(events.iter().any(|e| e.name == "apply_duration"));
    }
}
