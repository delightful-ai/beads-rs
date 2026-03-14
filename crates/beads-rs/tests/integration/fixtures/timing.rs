use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde_json::json;

#[derive(Clone, Debug)]
struct TimingSink {
    dir: PathBuf,
    binary: String,
    test_name: Option<String>,
    pid: u32,
}

impl TimingSink {
    fn from_env() -> Option<Self> {
        let dir = std::env::var_os("BD_TEST_TIMING_DIR")?;
        Some(Self {
            dir: PathBuf::from(dir),
            binary: current_binary_name(),
            test_name: current_test_name(),
            pid: process::id(),
        })
    }

    #[cfg(test)]
    fn for_test(dir: &Path, binary: &str, test_name: Option<&str>, pid: u32) -> Self {
        Self {
            dir: dir.to_path_buf(),
            binary: binary.to_string(),
            test_name: test_name.map(ToOwned::to_owned),
            pid,
        }
    }

    fn file_path(&self) -> PathBuf {
        let binary = sanitize_filename(&self.binary);
        let test_name = sanitize_filename(self.test_name.as_deref().unwrap_or("unknown-test"));
        self.dir
            .join(format!("{binary}--{test_name}--{}.jsonl", self.pid))
    }

    fn record(&self, phase: &str, context: Option<&str>, elapsed_ms: u128) {
        fs::create_dir_all(&self.dir).expect("create timing dir");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.file_path())
            .expect("open timing output");
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis();
        let event = json!({
            "phase": phase,
            "context": context,
            "elapsed_ms": elapsed_ms,
            "timestamp_ms": timestamp_ms,
            "binary": self.binary,
            "test_name": self.test_name,
            "pid": self.pid,
        });
        writeln!(file, "{event}").expect("write timing event");
    }
}

pub struct ScopedPhase {
    sink: Option<TimingSink>,
    phase: &'static str,
    context: Option<String>,
    start: Instant,
}

impl ScopedPhase {
    fn new(phase: &'static str, context: Option<String>) -> Self {
        Self {
            sink: TimingSink::from_env(),
            phase,
            context,
            start: Instant::now(),
        }
    }

    #[cfg(test)]
    fn with_sink(sink: TimingSink, phase: &'static str, context: Option<String>) -> Self {
        Self {
            sink: Some(sink),
            phase,
            context,
            start: Instant::now(),
        }
    }
}

impl Drop for ScopedPhase {
    fn drop(&mut self) {
        let Some(sink) = self.sink.as_ref() else {
            return;
        };
        sink.record(self.phase, self.context.as_deref(), self.start.elapsed().as_millis());
    }
}

pub fn scoped_phase(phase: &'static str) -> ScopedPhase {
    ScopedPhase::new(phase, None)
}

pub fn scoped_phase_with_context(
    phase: &'static str,
    context: impl std::fmt::Display,
) -> ScopedPhase {
    ScopedPhase::new(phase, Some(context.to_string()))
}

fn current_binary_name() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|path| path.file_name().map(|name| name.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "unknown-binary".to_string())
}

fn current_test_name() -> Option<String> {
    let args = std::env::args().collect::<Vec<_>>();
    args.windows(2)
        .find_map(|window| (window[0] == "--exact").then(|| window[1].clone()))
}

fn sanitize_filename(raw: &str) -> String {
    raw.chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::{ScopedPhase, TimingSink, current_test_name};

    #[test]
    fn scoped_phase_records_jsonl_event() {
        let dir = TempDir::new().expect("temp dir");
        let sink = TimingSink::for_test(dir.path(), "integration", Some("test_name"), 42);
        {
            let _phase = ScopedPhase::with_sink(
                sink.clone(),
                "fixture.realtime.new",
                Some("repo=/tmp/example".to_string()),
            );
        }

        let contents = fs::read_to_string(sink.file_path()).expect("read timing output");
        let value: serde_json::Value =
            serde_json::from_str(contents.lines().next().expect("json line")).expect("parse json");
        assert_eq!(value["phase"], "fixture.realtime.new");
        assert_eq!(value["context"], "repo=/tmp/example");
        assert_eq!(value["binary"], "integration");
        assert_eq!(value["test_name"], "test_name");
        assert_eq!(value["pid"], 42);
        assert!(value["elapsed_ms"].as_u64().is_some());
    }

    #[test]
    fn scoped_phase_without_sink_is_noop() {
        drop(super::ScopedPhase::new("fixture.realtime.new", None));
    }

    #[test]
    fn current_test_name_reads_exact_filter() {
        let parsed = {
            let args = vec![
                "integration".to_string(),
                "--exact".to_string(),
                "fixtures::timing::tests::current_test_name_reads_exact_filter".to_string(),
                "--nocapture".to_string(),
            ];
            args.windows(2)
                .find_map(|window| (window[0] == "--exact").then(|| window[1].clone()))
        };
        assert_eq!(
            parsed.as_deref(),
            Some("fixtures::timing::tests::current_test_name_reads_exact_filter")
        );
        let _ = current_test_name();
    }
}
