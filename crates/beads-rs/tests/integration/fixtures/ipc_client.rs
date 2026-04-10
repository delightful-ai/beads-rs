use beads_surface::ipc::IpcClient;
use std::path::Path;

pub fn runtime_bound_client(runtime_dir: &Path) -> IpcClient {
    IpcClient::for_runtime_dir(runtime_dir)
}

pub fn runtime_bound_client_no_autostart(runtime_dir: &Path) -> IpcClient {
    runtime_bound_client(runtime_dir).with_autostart(false)
}
