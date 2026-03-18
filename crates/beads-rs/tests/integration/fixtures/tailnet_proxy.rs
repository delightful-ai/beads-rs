#![allow(dead_code)]

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

use super::temp;
use super::timing;
use super::wait;

#[derive(Clone, Debug)]
pub struct TailnetProfile {
    pub profile: String,
    pub base_latency_ms: Option<u64>,
    pub jitter_ms: Option<u64>,
    pub loss_rate: Option<f64>,
    pub duplicate_rate: Option<f64>,
    pub reorder_rate: Option<f64>,
    pub blackhole_after_frames: Option<u64>,
    pub blackhole_after_bytes: Option<u64>,
    pub blackhole_for_ms: Option<u64>,
    pub reset_after_frames: Option<u64>,
    pub reset_after_bytes: Option<u64>,
    pub one_way_loss: Option<String>,
    pub max_frame_bytes: Option<usize>,
}

impl TailnetProfile {
    pub fn tailnet() -> Self {
        Self {
            profile: "tailnet".to_string(),
            base_latency_ms: None,
            jitter_ms: None,
            loss_rate: None,
            duplicate_rate: None,
            reorder_rate: None,
            blackhole_after_frames: None,
            blackhole_after_bytes: None,
            blackhole_for_ms: None,
            reset_after_frames: None,
            reset_after_bytes: None,
            one_way_loss: None,
            max_frame_bytes: None,
        }
    }

    pub fn none() -> Self {
        Self {
            profile: "none".to_string(),
            base_latency_ms: None,
            jitter_ms: None,
            loss_rate: None,
            duplicate_rate: None,
            reorder_rate: None,
            blackhole_after_frames: None,
            blackhole_after_bytes: None,
            blackhole_for_ms: None,
            reset_after_frames: None,
            reset_after_bytes: None,
            one_way_loss: None,
            max_frame_bytes: None,
        }
    }

    pub fn pathological() -> Self {
        Self {
            profile: "pathological".to_string(),
            base_latency_ms: None,
            jitter_ms: None,
            loss_rate: None,
            duplicate_rate: None,
            reorder_rate: None,
            blackhole_after_frames: None,
            blackhole_after_bytes: None,
            blackhole_for_ms: None,
            reset_after_frames: None,
            reset_after_bytes: None,
            one_way_loss: None,
            max_frame_bytes: None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum TailnetTraceMode {
    Record,
    Replay,
}

impl TailnetTraceMode {
    pub fn as_str(self) -> &'static str {
        match self {
            TailnetTraceMode::Record => "record",
            TailnetTraceMode::Replay => "replay",
        }
    }
}

#[derive(Clone, Debug)]
pub struct TailnetTrace {
    pub mode: TailnetTraceMode,
    pub path: PathBuf,
    pub timeout_ms: Option<u64>,
}

pub struct TailnetProxy {
    child: Child,
    listen_addr: String,
    _ready_dir: tempfile::TempDir,
}

impl TailnetProxy {
    pub fn spawn(listen_addr: String, upstream_addr: String, seed: u64) -> Self {
        Self::spawn_with_profile(listen_addr, upstream_addr, seed, TailnetProfile::tailnet())
    }

    pub fn spawn_with_profile(
        listen_addr: String,
        upstream_addr: String,
        seed: u64,
        profile: TailnetProfile,
    ) -> Self {
        Self::spawn_with_profile_and_trace(listen_addr, upstream_addr, seed, profile, None)
    }

    pub fn spawn_with_profile_and_trace(
        listen_addr: String,
        upstream_addr: String,
        seed: u64,
        profile: TailnetProfile,
        trace: Option<TailnetTrace>,
    ) -> Self {
        let _phase = timing::scoped_phase_with_context("fixture.tailnet_proxy.spawn", &listen_addr);
        let bin = assert_cmd::cargo::cargo_bin!("tailnet_proxy");
        let ready_dir = temp::fixture_tempdir("tailnet-proxy");
        let ready_path = ready_dir.path().join("ready");
        let mut cmd = Command::new(bin);
        cmd.args([
            "--listen",
            listen_addr.as_str(),
            "--upstream",
            upstream_addr.as_str(),
            "--profile",
            profile.profile.as_str(),
            "--seed",
            &seed.to_string(),
        ]);
        cmd.arg("--ready-file").arg(&ready_path);
        push_opt_arg(&mut cmd, "--base-latency-ms", profile.base_latency_ms);
        push_opt_arg(&mut cmd, "--jitter-ms", profile.jitter_ms);
        push_opt_arg(&mut cmd, "--loss-rate", profile.loss_rate);
        push_opt_arg(&mut cmd, "--duplicate-rate", profile.duplicate_rate);
        push_opt_arg(&mut cmd, "--reorder-rate", profile.reorder_rate);
        push_opt_arg(
            &mut cmd,
            "--blackhole-after-frames",
            profile.blackhole_after_frames,
        );
        push_opt_arg(
            &mut cmd,
            "--blackhole-after-bytes",
            profile.blackhole_after_bytes,
        );
        push_opt_arg(&mut cmd, "--blackhole-for-ms", profile.blackhole_for_ms);
        push_opt_arg(&mut cmd, "--reset-after-frames", profile.reset_after_frames);
        push_opt_arg(&mut cmd, "--reset-after-bytes", profile.reset_after_bytes);
        push_opt_arg(&mut cmd, "--one-way-loss", profile.one_way_loss.clone());
        push_opt_arg(&mut cmd, "--max-frame-bytes", profile.max_frame_bytes);
        if let Some(trace) = trace {
            cmd.arg("--trace-mode").arg(trace.mode.as_str());
            cmd.arg("--trace-path").arg(&trace.path);
            push_opt_arg(&mut cmd, "--trace-timeout-ms", trace.timeout_ms);
        }
        let mut child = {
            let _phase = timing::scoped_phase("fixture.tailnet_proxy.process_spawn");
            cmd.spawn().expect("spawn tailnet proxy")
        };
        {
            let _phase = timing::scoped_phase("fixture.tailnet_proxy.ready_wait");
            assert!(
                wait::poll_until(Duration::from_secs(2), || read_ready_listen_addr(
                    &ready_path
                )
                .is_some()),
                "tailnet proxy did not publish readiness at {}",
                ready_path.display()
            );
        }
        if let Some(status) = child.try_wait().expect("check proxy status") {
            panic!("tailnet proxy exited early: {status}");
        }
        let listen_addr = read_ready_listen_addr(&ready_path).expect("tailnet proxy listen addr");
        Self {
            child,
            listen_addr,
            _ready_dir: ready_dir,
        }
    }

    pub fn listen_addr(&self) -> &str {
        &self.listen_addr
    }
}

impl Drop for TailnetProxy {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn push_opt_arg<T: ToString>(cmd: &mut Command, flag: &str, value: Option<T>) {
    if let Some(value) = value {
        cmd.arg(flag).arg(value.to_string());
    }
}

fn read_ready_listen_addr(path: &Path) -> Option<String> {
    let contents = fs::read_to_string(path).ok()?;
    contents
        .lines()
        .find_map(|line| line.strip_prefix("listen_addr=").map(ToOwned::to_owned))
}
