//! `bd-http` binary: serve the beads HTTP transport in front of a running daemon.
//!
//! Configuration via environment:
//! - `BD_HTTP_ADDR` (default `127.0.0.1:7777`) — bind address
//! - `BD_RUNTIME_DIR` — passed through to `IpcClient`'s socket discovery
//! - `BD_HTTP_NO_AUTOSTART` — if set, do not autostart the daemon on first call

use std::env;

use beads_http::serve;
use beads_surface::IpcClient;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber_init();

    let addr = env::var("BD_HTTP_ADDR").unwrap_or_else(|_| "127.0.0.1:7777".to_string());
    let listener = TcpListener::bind(&addr).await?;
    let local = listener.local_addr()?;

    let mut client = IpcClient::new();
    if env::var_os("BD_HTTP_NO_AUTOSTART").is_some() {
        client = client.with_autostart(false);
    }

    tracing::info!(%local, socket = %client.socket_path().display(), "beads-http listening");
    serve(listener, client).await
}

fn tracing_subscriber_init() {
    // Minimal: respect RUST_LOG via env_filter if the host wires it up later.
    // Avoid pulling tracing-subscriber into the dep graph just for the stub.
    let _ = std::env::var("RUST_LOG");
}
