#![allow(dead_code)]

use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

pub struct TailnetProxy {
    child: Child,
    listen_addr: String,
}

impl TailnetProxy {
    pub fn spawn(listen_addr: String, upstream_addr: String, seed: u64) -> Self {
        let bin = assert_cmd::cargo::cargo_bin!("tailnet_proxy");
        let mut cmd = Command::new(bin);
        cmd.args([
            "--listen",
            listen_addr.as_str(),
            "--upstream",
            upstream_addr.as_str(),
            "--profile",
            "tailnet",
            "--seed",
            &seed.to_string(),
        ]);
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

fn wait_for_listen(addr: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if TcpStream::connect(addr).is_err() {
        panic!("tailnet proxy did not start listening on {addr}");
    }
}
