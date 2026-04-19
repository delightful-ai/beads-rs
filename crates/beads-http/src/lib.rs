//! HTTP transport for the beads daemon `Request`/`Response` surface.
//!
//! Wraps `beads_surface::IpcClient` so that any HTTP caller can drive the same
//! enum-shaped contract the Unix-socket clients use. The wire body for the RPC
//! endpoint is a `beads_surface::Request` JSON encoding; the response body is a
//! `beads_surface::Response`. Transport-level failures (daemon unavailable,
//! version mismatch, etc.) are mapped onto `Response::Err` via
//! `IntoErrorPayload` so callers always deserialize a `Response`.

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use beads_core::{CliErrorCode, ErrorPayload, IntoErrorPayload};
use beads_surface::{IpcClient, Request, Response};
use serde::Serialize;
use tokio::net::TcpListener;

pub fn router(client: IpcClient) -> Router {
    Router::new()
        .route("/rpc", post(handle_rpc))
        .route("/healthz", get(handle_health))
        .route("/subscribe", get(handle_subscribe))
        .with_state(client)
}

pub async fn serve(listener: TcpListener, client: IpcClient) -> std::io::Result<()> {
    axum::serve(listener, router(client)).await
}

async fn handle_rpc(
    State(client): State<IpcClient>,
    Json(request): Json<Request>,
) -> Json<Response> {
    let result = tokio::task::spawn_blocking(move || client.send_request(&request)).await;
    let response = match result {
        Ok(Ok(response)) => response,
        Ok(Err(ipc_err)) => Response::err(ipc_err.into_error_payload()),
        Err(join_err) => Response::err(ErrorPayload::new(
            CliErrorCode::IoError.into(),
            format!("rpc handler join error: {join_err}"),
            false,
        )),
    };
    Json(response)
}

#[derive(Serialize)]
struct Health {
    ok: bool,
    service: &'static str,
}

async fn handle_health() -> Json<Health> {
    Json(Health {
        ok: true,
        service: "beads-http",
    })
}

async fn handle_subscribe() -> impl IntoResponse {
    // Subscribe needs a streaming connection (newline-framed Response events on
    // the socket); deferred until the in-process Dispatcher seam lands so the
    // stream does not have to re-frame across the Unix socket.
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(Response::err(ErrorPayload::new(
            CliErrorCode::IoError.into(),
            "subscribe is not yet implemented over HTTP",
            false,
        ))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request as HttpRequest, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn test_router() -> Router {
        router(IpcClient::for_socket_path("/dev/null/no-daemon".into()).with_autostart(false))
    }

    #[tokio::test]
    async fn healthz_returns_ok() {
        let app = test_router();
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["ok"], true);
        assert_eq!(body["service"], "beads-http");
    }

    #[tokio::test]
    async fn subscribe_returns_not_implemented() {
        let app = test_router();
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/subscribe")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn rpc_with_unavailable_daemon_returns_err_response() {
        let app = test_router();
        let body = serde_json::to_vec(&Request::Ping).unwrap();
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/rpc")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: Response = serde_json::from_slice(&bytes).unwrap();
        assert!(matches!(parsed, Response::Err { .. }));
    }

    #[tokio::test]
    async fn rpc_with_malformed_body_returns_4xx() {
        let app = test_router();
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/rpc")
                    .header("content-type", "application/json")
                    .body(Body::from("not a request"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_client_error());
    }
}
