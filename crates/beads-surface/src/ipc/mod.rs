pub mod client;
pub mod codec;
pub mod types;

use thiserror::Error;

pub use client::IpcClient;
pub use types::*;

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("ipc not implemented")]
    NotImplemented,
}
