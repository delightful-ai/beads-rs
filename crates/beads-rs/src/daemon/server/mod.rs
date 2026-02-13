//! Server thread loops.
//!
//! Three threads:
//! - Socket acceptor (main thread) - accepts connections, spawns handlers
//! - State thread - owns Daemon, processes requests sequentially
//! - Git thread - owns GitWorker, handles all git IO

mod socket;
mod spans;
mod state_loop;
mod waiters;

use crossbeam::channel::Sender;

use super::ipc::{Request, Response};
use super::subscription::SubscribeReply;

pub(in crate::daemon) use state_loop::run_state_loop;

pub(super) use socket::handle_client;

/// Message sent from state thread to socket handlers.
pub(super) enum ServerReply {
    Response(Response),
    Subscribe(SubscribeReply),
}

/// Message sent from socket handlers to state thread.
pub(in crate::daemon) struct RequestMessage {
    request: Request,
    respond: Sender<ServerReply>,
}

impl RequestMessage {
    pub(in crate::daemon) fn new(request: Request, respond: Sender<ServerReply>) -> Self {
        Self { request, respond }
    }
}

#[cfg(test)]
mod tests;
