use crate::ipc::IpcError;

#[derive(Debug, Default)]
pub struct IpcClient;

impl IpcClient {
    pub fn new() -> Self {
        Self
    }

    pub fn connect(_path: &std::path::Path) -> Result<Self, IpcError> {
        Err(IpcError::DaemonUnavailable(
            "ipc client not implemented".to_string(),
        ))
    }
}
