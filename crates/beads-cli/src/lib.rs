#![forbid(unsafe_code)]

pub mod backend;
pub mod cli;
pub mod commands;
pub mod filters;
pub mod migrate;
pub mod parsers;
pub mod paths;
pub mod render;
pub mod runtime;
pub mod upgrade;
pub mod validation;

use beads_surface::ipc::{IpcClient, IpcError, Request, Response};

/// CLI output shape for rendering/serialization decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputMode {
    #[default]
    Human,
    Json,
}

/// Minimal typed command invocation surface for CLI migration.
#[derive(Debug, Clone)]
pub struct Invocation {
    pub request: Request,
    pub output: OutputMode,
}

impl Invocation {
    pub fn new(request: Request) -> Self {
        Self {
            request,
            output: OutputMode::Human,
        }
    }

    pub fn with_output(mut self, output: OutputMode) -> Self {
        self.output = output;
        self
    }
}

pub type Result<T> = std::result::Result<T, IpcError>;

/// Boundary for request transport (IPC now, test doubles later).
pub trait Transport {
    fn send(&self, request: &Request) -> Result<Response>;
}

/// Default transport backed by the daemon IPC socket.
#[derive(Debug, Clone, Default)]
pub struct IpcTransport {
    client: IpcClient,
}

impl IpcTransport {
    pub fn new(client: IpcClient) -> Self {
        Self { client }
    }
}

impl Transport for IpcTransport {
    fn send(&self, request: &Request) -> Result<Response> {
        self.client.send_request(request)
    }
}

pub fn run_invocation<T: Transport>(transport: &T, invocation: &Invocation) -> Result<Response> {
    transport.send(&invocation.request)
}
