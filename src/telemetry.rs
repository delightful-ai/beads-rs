use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

use crate::config::{FileLoggingConfig, LogFormat, LogRotation, LoggingConfig};
use crate::paths;

const LOG_FILE_PREFIX: &str = "beads.log";

#[derive(Clone, Debug)]
pub struct SpanContext {
    pub name: &'static str,
    pub fields: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct LogRecord {
    pub timestamp: SystemTime,
    pub level: Level,
    pub target: &'static str,
    pub name: &'static str,
    pub message: Option<String>,
    pub fields: BTreeMap<String, String>,
    pub spans: Vec<SpanContext>,
}

#[derive(Clone, Debug)]
pub struct SpanRecord {
    pub id: u64,
    pub parent_id: Option<u64>,
    pub name: &'static str,
    pub fields: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug)]
pub enum SpanPhase {
    New,
    Enter,
    Exit,
    Close,
}

pub trait Logger: Send + Sync {
    fn log(&self, record: LogRecord);
}

pub trait Tracer: Send + Sync {
    fn on_span(&self, record: SpanRecord, phase: SpanPhase);
    fn on_event(&self, record: LogRecord);
}

#[derive(Clone)]
pub struct TelemetryConfig {
    pub verbosity: u8,
    pub logging: LoggingConfig,
    pub logger: Option<Arc<dyn Logger>>,
    pub tracer: Option<Arc<dyn Tracer>>,
}

impl TelemetryConfig {
    pub fn new(verbosity: u8, logging: LoggingConfig) -> Self {
        Self {
            verbosity,
            logging,
            logger: None,
            tracer: None,
        }
    }
}

pub fn is_test_env() -> bool {
    std::env::var_os("BD_TESTING").is_some() || std::env::var_os("RUST_TEST_THREADS").is_some()
}

pub fn apply_daemon_logging_defaults(logging: &mut LoggingConfig) {
    apply_daemon_logging_defaults_inner(
        logging,
        is_test_env(),
        std::env::var_os("BD_LOG_FILE").is_some(),
    );
}

fn apply_daemon_logging_defaults_inner(
    logging: &mut LoggingConfig,
    is_test_env: bool,
    has_log_file_env: bool,
) {
    if is_test_env {
        return;
    }
    if has_log_file_env {
        return;
    }
    if !logging.file.enabled {
        logging.file.enabled = true;
    }
}

pub struct TelemetryGuard {
    _guards: Vec<tracing_appender::non_blocking::WorkerGuard>,
}

pub fn init(config: TelemetryConfig) -> TelemetryGuard {
    let filter = EnvFilter::builder()
        .with_default_directive(level_from_verbosity(config.verbosity).into())
        .with_env_var("LOG")
        .from_env_lossy();

    let mut guards = Vec::new();
    let mut layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = Vec::new();

    if config.logging.stdout {
        layers.push(build_stdout_layer(config.logging.stdout_format));
    }

    let mut file_prune_report = None;
    let mut file_setup_error = None;
    if config.logging.file.enabled {
        let dir = resolve_log_dir(&config.logging.file);
        match fs::create_dir_all(&dir) {
            Ok(()) => {
                let retention = RetentionLimits::from_file_config(&config.logging.file);
                if retention.is_enabled() {
                    match prune_logs(&dir, LOG_FILE_PREFIX, retention, SystemTime::now()) {
                        Ok(report) => file_prune_report = Some(report),
                        Err(err) => {
                            file_setup_error = Some(format!("log retention failed: {err}"));
                        }
                    }
                }

                let (layer, guard) = build_file_layer(&config.logging.file, &dir);
                layers.push(layer);
                guards.push(guard);
            }
            Err(err) => {
                file_setup_error =
                    Some(format!("log dir init failed for {}: {err}", dir.display()));
            }
        }
    }

    if let Some(layer) = TelemetryLayer::new(config.logger, config.tracer) {
        layers.push(Box::new(layer));
    }

    layers.push(Box::new(filter));

    Registry::default().with(layers).init();

    if let Some(report) = file_prune_report {
        tracing::info!(
            pruned = report.removed,
            failed = report.failed,
            candidates = report.candidates,
            "log retention applied"
        );
    }
    if let Some(error) = file_setup_error {
        tracing::warn!("{error}");
    }

    TelemetryGuard { _guards: guards }
}

#[derive(Clone, Debug, Default)]
struct SpanFields {
    fields: BTreeMap<String, String>,
}

#[derive(Default)]
struct FieldVisitor {
    message: Option<String>,
    fields: BTreeMap<String, String>,
}

impl FieldVisitor {
    fn record(&mut self, field: &Field, value: String) {
        if field.name() == "message" {
            self.message = Some(value);
        } else {
            self.fields.insert(field.name().to_string(), value);
        }
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.record(field, format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record(field, value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record(field, value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record(field, value.to_string());
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.record(field, value.to_string());
    }
}

#[derive(Clone)]
struct TelemetryLayer {
    logger: Option<Arc<dyn Logger>>,
    tracer: Option<Arc<dyn Tracer>>,
}

impl TelemetryLayer {
    fn new(logger: Option<Arc<dyn Logger>>, tracer: Option<Arc<dyn Tracer>>) -> Option<Self> {
        if logger.is_none() && tracer.is_none() {
            None
        } else {
            Some(Self { logger, tracer })
        }
    }
}

impl<S> Layer<S> for TelemetryLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        ctx: Context<'_, S>,
    ) {
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanFields {
                fields: visitor.fields.clone(),
            });
            if let Some(tracer) = &self.tracer {
                tracer.on_span(
                    SpanRecord {
                        id: id.into_u64(),
                        parent_id: span.parent().map(|p| p.id().into_u64()),
                        name: span.metadata().name(),
                        fields: visitor.fields,
                    },
                    SpanPhase::New,
                );
            }
        }
    }

    fn on_record(&self, id: &tracing::Id, values: &tracing::span::Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut visitor = FieldVisitor::default();
            values.record(&mut visitor);
            let mut extensions = span.extensions_mut();
            if extensions.get_mut::<SpanFields>().is_none() {
                extensions.insert(SpanFields::default());
            }
            let fields = extensions.get_mut::<SpanFields>().expect("span fields");
            fields.fields.extend(visitor.fields);
        }
    }

    fn on_enter(&self, id: &tracing::Id, ctx: Context<'_, S>) {
        self.emit_span_event(id, ctx, SpanPhase::Enter);
    }

    fn on_exit(&self, id: &tracing::Id, ctx: Context<'_, S>) {
        self.emit_span_event(id, ctx, SpanPhase::Exit);
    }

    fn on_close(&self, id: tracing::Id, ctx: Context<'_, S>) {
        self.emit_span_event(&id, ctx, SpanPhase::Close);
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let record = build_log_record(event, ctx);
        if let Some(logger) = &self.logger {
            logger.log(record.clone());
        }
        if let Some(tracer) = &self.tracer {
            tracer.on_event(record);
        }
    }
}

impl TelemetryLayer {
    fn emit_span_event<S>(&self, id: &tracing::Id, ctx: Context<'_, S>, phase: SpanPhase)
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let Some(tracer) = &self.tracer else {
            return;
        };
        let Some(span) = ctx.span(id) else {
            return;
        };
        let fields = span
            .extensions()
            .get::<SpanFields>()
            .map(|fields| fields.fields.clone())
            .unwrap_or_default();
        tracer.on_span(
            SpanRecord {
                id: id.into_u64(),
                parent_id: span.parent().map(|parent| parent.id().into_u64()),
                name: span.metadata().name(),
                fields,
            },
            phase,
        );
    }
}

fn build_log_record<S>(event: &Event<'_>, ctx: Context<'_, S>) -> LogRecord
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let mut visitor = FieldVisitor::default();
    event.record(&mut visitor);
    let spans = ctx
        .event_scope(event)
        .map(|scope| {
            scope
                .from_root()
                .map(|span| SpanContext {
                    name: span.metadata().name(),
                    fields: span
                        .extensions()
                        .get::<SpanFields>()
                        .map(|fields| fields.fields.clone())
                        .unwrap_or_default(),
                })
                .collect()
        })
        .unwrap_or_default();

    LogRecord {
        timestamp: SystemTime::now(),
        level: *event.metadata().level(),
        target: event.metadata().target(),
        name: event.metadata().name(),
        message: visitor.message,
        fields: visitor.fields,
        spans,
    }
}

fn build_stdout_layer(format: LogFormat) -> Box<dyn Layer<Registry> + Send + Sync> {
    match format {
        LogFormat::Tree => Box::new(tracing_tree::HierarchicalLayer::new(2)),
        LogFormat::Pretty => Box::new(
            tracing_subscriber::fmt::layer()
                .pretty()
                .with_writer(std::io::stderr)
                .with_target(true)
                .with_thread_names(true)
                .with_thread_ids(true),
        ),
        LogFormat::Compact => Box::new(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_writer(std::io::stderr)
                .with_target(true)
                .with_thread_names(true)
                .with_thread_ids(true),
        ),
        LogFormat::Json => Box::new(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(std::io::stderr)
                .with_target(true)
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_current_span(true)
                .with_span_list(true),
        ),
    }
}

fn build_file_layer(
    config: &FileLoggingConfig,
    dir: &Path,
) -> (
    Box<dyn Layer<Registry> + Send + Sync>,
    tracing_appender::non_blocking::WorkerGuard,
) {
    let rotation = match config.rotation {
        LogRotation::Daily => tracing_appender::rolling::Rotation::DAILY,
        LogRotation::Hourly => tracing_appender::rolling::Rotation::HOURLY,
        LogRotation::Minutely => tracing_appender::rolling::Rotation::MINUTELY,
        LogRotation::Never => tracing_appender::rolling::Rotation::NEVER,
    };
    let appender =
        tracing_appender::rolling::RollingFileAppender::new(rotation, dir, LOG_FILE_PREFIX);
    let (writer, guard) = tracing_appender::non_blocking(appender);
    let layer: Box<dyn Layer<Registry> + Send + Sync> = match config.format {
        LogFormat::Tree => Box::new(
            tracing_tree::HierarchicalLayer::new(2)
                .with_ansi(false)
                .with_writer(writer),
        ),
        LogFormat::Pretty => Box::new(
            tracing_subscriber::fmt::layer()
                .pretty()
                .with_writer(writer)
                .with_ansi(false)
                .with_target(true)
                .with_thread_names(true)
                .with_thread_ids(true),
        ),
        LogFormat::Compact => Box::new(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_writer(writer)
                .with_ansi(false)
                .with_target(true)
                .with_thread_names(true)
                .with_thread_ids(true),
        ),
        LogFormat::Json => Box::new(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(writer)
                .with_target(true)
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_current_span(true)
                .with_span_list(true),
        ),
    };
    (layer, guard)
}

fn level_from_verbosity(verbosity: u8) -> tracing::metadata::LevelFilter {
    match verbosity {
        0 => tracing::metadata::LevelFilter::ERROR,
        1 => tracing::metadata::LevelFilter::INFO,
        _ => tracing::metadata::LevelFilter::DEBUG,
    }
}

fn resolve_log_dir(config: &FileLoggingConfig) -> PathBuf {
    config.dir.clone().unwrap_or_else(paths::log_dir)
}

#[derive(Clone, Copy, Debug, Default)]
struct RetentionLimits {
    max_age: Option<Duration>,
    max_files: Option<usize>,
}

impl RetentionLimits {
    fn from_file_config(config: &FileLoggingConfig) -> Self {
        let max_age = config
            .retention_max_age_days
            .map(|days| Duration::from_secs(days.saturating_mul(24 * 60 * 60)));
        Self {
            max_age,
            max_files: config.retention_max_files,
        }
    }

    fn is_enabled(&self) -> bool {
        self.max_age.is_some() || self.max_files.is_some()
    }
}

#[derive(Clone, Debug)]
struct LogEntry {
    path: PathBuf,
    modified: SystemTime,
}

#[derive(Clone, Debug, Default)]
struct PruneReport {
    candidates: usize,
    removed: usize,
    failed: usize,
}

fn prune_logs(
    dir: &Path,
    prefix: &str,
    retention: RetentionLimits,
    now: SystemTime,
) -> std::io::Result<PruneReport> {
    let mut entries = collect_log_entries(dir, prefix, now)?;
    let candidates = entries.len();
    let to_remove = prune_log_entries(&mut entries, retention, now);
    let mut removed = 0usize;
    let mut failed = 0usize;
    for path in to_remove {
        match fs::remove_file(&path) {
            Ok(()) => removed += 1,
            Err(_) => failed += 1,
        }
    }
    Ok(PruneReport {
        candidates,
        removed,
        failed,
    })
}

fn collect_log_entries(
    dir: &Path,
    prefix: &str,
    now: SystemTime,
) -> std::io::Result<Vec<LogEntry>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.starts_with(prefix) {
            continue;
        }
        let meta = entry.metadata()?;
        if !meta.is_file() {
            continue;
        }
        let modified = meta.modified().unwrap_or(now);
        entries.push(LogEntry { path, modified });
    }
    Ok(entries)
}

fn prune_log_entries(
    entries: &mut Vec<LogEntry>,
    retention: RetentionLimits,
    now: SystemTime,
) -> Vec<PathBuf> {
    let mut removed = Vec::new();

    if let Some(max_age) = retention.max_age {
        let mut keep = Vec::new();
        for entry in entries.drain(..) {
            let age = now.duration_since(entry.modified).unwrap_or(Duration::ZERO);
            if age > max_age {
                removed.push(entry.path);
            } else {
                keep.push(entry);
            }
        }
        *entries = keep;
    }

    if let Some(max_files) = retention.max_files {
        entries.sort_by_key(|entry| entry.modified);
        if entries.len() > max_files {
            let excess = entries.len() - max_files;
            for entry in entries.drain(..excess) {
                removed.push(entry.path);
            }
        }
    }

    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prune_log_entries_respects_age_and_count() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
        let mut entries = vec![
            LogEntry {
                path: PathBuf::from("old.log"),
                modified: now - Duration::from_secs(9_000),
            },
            LogEntry {
                path: PathBuf::from("mid.log"),
                modified: now - Duration::from_secs(500),
            },
            LogEntry {
                path: PathBuf::from("new.log"),
                modified: now - Duration::from_secs(40),
            },
            LogEntry {
                path: PathBuf::from("newest.log"),
                modified: now - Duration::from_secs(5),
            },
        ];
        let retention = RetentionLimits {
            max_age: Some(Duration::from_secs(1_000)),
            max_files: Some(2),
        };

        let removed = prune_log_entries(&mut entries, retention, now);

        assert!(removed.contains(&PathBuf::from("old.log")));
        assert!(removed.contains(&PathBuf::from("mid.log")));
        assert_eq!(entries.len(), 2);
        assert!(
            entries
                .iter()
                .any(|entry| entry.path == PathBuf::from("new.log"))
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.path == PathBuf::from("newest.log"))
        );
    }

    #[test]
    fn daemon_logging_defaults_skip_in_tests() {
        let mut logging = LoggingConfig::default();
        logging.file.enabled = false;
        apply_daemon_logging_defaults_inner(&mut logging, true, false);
        assert!(
            !logging.file.enabled,
            "daemon logging defaults should skip in tests"
        );
    }
}
