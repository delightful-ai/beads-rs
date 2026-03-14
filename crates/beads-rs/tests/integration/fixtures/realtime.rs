#![allow(dead_code)]

use std::path::Path;

use assert_cmd::Command;
use beads_surface::ipc::IpcClient;

use super::bd_runtime::{BdCommandProfile, BdRuntimeRepo};
use super::timing;

pub struct RealtimeFixture {
    runtime: BdRuntimeRepo,
}

impl RealtimeFixture {
    pub fn new() -> Self {
        let _phase = timing::scoped_phase("fixture.realtime.new");
        Self {
            runtime: BdRuntimeRepo::new_with_origin(),
        }
    }

    pub fn repo_path(&self) -> &Path {
        self.runtime.path()
    }

    pub fn runtime_dir(&self) -> &Path {
        self.runtime.runtime_dir()
    }

    pub fn data_dir(&self) -> &Path {
        self.runtime.data_dir()
    }

    pub fn bd(&self) -> Command {
        self.runtime
            .bd_with_profile(BdCommandProfile::fast_daemon())
    }

    pub fn ipc_client(&self) -> IpcClient {
        self.runtime.ipc_client_no_autostart()
    }

    pub fn start_daemon(&self) {
        let _phase = timing::scoped_phase("fixture.realtime.start_daemon");
        self.runtime.start_daemon(BdCommandProfile::fast_daemon());
    }
}
