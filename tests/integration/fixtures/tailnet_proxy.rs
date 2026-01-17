#![allow(dead_code)]

use std::net::TcpListener;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

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

pub struct TailnetProxy {
    child: Child,
    listen_addr: String,
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
        let bin = assert_cmd::cargo::cargo_bin!("tailnet_proxy");
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
        let mut child = cmd.spawn().expect("spawn tailnet proxy");
        wait_for_listen(&listen_addr, Duration::from_secs(2));
        if let Some(status) = child.try_wait().expect("check proxy status") {
            panic!("tailnet proxy exited early: {status}");
        }
        Self { child, listen_addr }
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

fn wait_for_listen(addr: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        match TcpListener::bind(addr) {
            Ok(listener) => {
                drop(listener);
            }
            Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => {
                return;
            }
            Err(err) => {
                if Instant::now() >= deadline {
                    panic!("tailnet proxy did not start listening on {addr}: {err}");
                }
            }
        }
        if Instant::now() >= deadline {
            panic!("tailnet proxy did not start listening on {addr}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
