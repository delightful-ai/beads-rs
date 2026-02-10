use std::cell::RefCell;
use std::fmt;
use std::path::PathBuf;

use beads_core::{
    ActorId, Applied, ClientRequestId, DurabilityClass, ErrorPayload, NamespaceId,
    ValidatedActorId, Watermarks,
};
use beads_surface::ipc::{
    IpcClient, IpcConnection, MutationCtx, MutationMeta, ReadConsistency, ReadCtx, RepoCtx,
    Request, Response, ResponsePayload,
};

use crate::validation::{self, ValidationError};

pub type ValidationResult<T> = std::result::Result<T, ValidationError>;

#[derive(Debug, Clone, PartialEq)]
pub struct DaemonResponseError {
    payload: Box<ErrorPayload>,
}

impl DaemonResponseError {
    pub fn new(payload: ErrorPayload) -> Self {
        Self {
            payload: Box::new(payload),
        }
    }

    pub fn payload(&self) -> &ErrorPayload {
        self.payload.as_ref()
    }

    pub fn into_payload(self) -> ErrorPayload {
        *self.payload
    }
}

impl fmt::Display for DaemonResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let payload = self.payload.as_ref();
        if let Some(details) = &payload.details {
            write!(
                f,
                "{} - {} (details: {})",
                payload.code, payload.message, details
            )
        } else {
            write!(f, "{} - {}", payload.code, payload.message)
        }
    }
}

impl std::error::Error for DaemonResponseError {}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Ipc(#[from] beads_surface::ipc::IpcError),
    #[error(transparent)]
    Daemon(#[from] DaemonResponseError),
}

/// Typed CLI runtime context shared by command handlers.
#[derive(Clone, Debug)]
pub struct CliRuntimeCtx {
    pub repo: PathBuf,
    pub json: bool,
    pub namespace: Option<NamespaceId>,
    pub durability: Option<DurabilityClass>,
    pub client_request_id: Option<ClientRequestId>,
    pub require_min_seen: Option<Watermarks<Applied>>,
    pub wait_timeout_ms: Option<u64>,
    pub actor_id: Option<ActorId>,
}

impl CliRuntimeCtx {
    pub fn mutation_meta(&self) -> MutationMeta {
        MutationMeta {
            namespace: self.namespace.clone(),
            durability: self.durability,
            client_request_id: self.client_request_id,
            actor_id: self.actor_id.clone(),
        }
    }

    pub fn mutation_ctx(&self) -> MutationCtx {
        MutationCtx::new(self.repo.clone(), self.mutation_meta())
    }

    pub fn read_consistency(&self) -> ReadConsistency {
        ReadConsistency {
            namespace: self.namespace.clone(),
            require_min_seen: self.require_min_seen.clone(),
            wait_timeout_ms: self.wait_timeout_ms,
        }
    }

    pub fn read_ctx(&self) -> ReadCtx {
        ReadCtx::new(self.repo.clone(), self.read_consistency())
    }

    pub fn repo_ctx(&self) -> RepoCtx {
        RepoCtx::new(self.repo.clone())
    }

    pub fn actor_id(&self) -> ValidationResult<ActorId> {
        self.actor_id
            .clone()
            .map(Ok)
            .unwrap_or_else(current_actor_id)
    }

    pub fn actor_string(&self) -> ValidationResult<String> {
        Ok(self.actor_id()?.as_str().to_string())
    }
}

pub fn resolve_description(
    description: Option<String>,
    body: Option<String>,
) -> ValidationResult<Option<String>> {
    match (description, body) {
        (Some(d), Some(b)) => {
            if d != b {
                return Err(validation::validation_error(
                    "description",
                    format!(
                        "cannot specify both --description and --body with different values (--description={d:?}, --body={b:?})"
                    ),
                ));
            }
            Ok(Some(d))
        }
        (Some(d), None) => Ok(Some(d)),
        (None, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}

pub fn validate_actor_id(raw: &str) -> ValidationResult<ActorId> {
    ValidatedActorId::parse(raw)
        .map(Into::into)
        .map_err(|err| validation::validation_error("actor", err.to_string()))
}

pub fn current_actor_id() -> ValidationResult<ActorId> {
    if let Ok(actor) = std::env::var("BD_ACTOR")
        && !actor.trim().is_empty()
    {
        return validate_actor_id(&actor);
    }
    let username = whoami::username();
    let hostname = whoami::fallible::hostname().unwrap_or_else(|_| "unknown".into());
    validate_actor_id(&format!("{username}@{hostname}"))
}

thread_local! {
    static COMMAND_CONNECTION: RefCell<Option<IpcConnection>> = const { RefCell::new(None) };
}

pub fn send_raw(req: &Request) -> crate::Result<Response> {
    let response = send_raw_once(req).or_else(|err| {
        if err.transience().is_retryable() {
            COMMAND_CONNECTION.with(|conn| {
                *conn.borrow_mut() = None;
            });
            send_raw_once(req)
        } else {
            Err(err)
        }
    })?;
    Ok(response)
}

fn send_raw_once(req: &Request) -> std::result::Result<Response, beads_surface::ipc::IpcError> {
    COMMAND_CONNECTION.with(|conn| {
        let mut conn = conn.borrow_mut();
        if conn.is_none() {
            let client = IpcClient::new();
            *conn = Some(client.connect()?);
        }
        conn.as_mut()
            .expect("command connection initialized")
            .send_request(req)
    })
}

fn response_payload(response: Response) -> std::result::Result<ResponsePayload, RuntimeError> {
    match response {
        Response::Ok { ok } => Ok(ok),
        Response::Err { err } => Err(RuntimeError::Daemon(DaemonResponseError::new(err))),
    }
}

pub fn send(req: &Request) -> std::result::Result<ResponsePayload, RuntimeError> {
    let response = send_raw(req)?;
    response_payload(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use beads_core::{CliErrorCode, HeadStatus, ReplicaId, Seq0};
    use beads_surface::ipc::ResponsePayload;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn resolve_description_rejects_mismatched_alias_values() {
        let err = resolve_description(Some("a".into()), Some("b".into())).expect_err("conflict");
        assert_eq!(
            err,
            validation::validation_error(
                "description",
                "cannot specify both --description and --body with different values (--description=\"a\", --body=\"b\")"
            )
        );
    }

    #[test]
    fn mutation_meta_includes_actor_override() {
        let actor = ActorId::new("alice@example.com").expect("actor");
        let ctx = CliRuntimeCtx {
            repo: PathBuf::from("/tmp/beads"),
            json: false,
            namespace: None,
            durability: None,
            client_request_id: None,
            require_min_seen: None,
            wait_timeout_ms: None,
            actor_id: Some(actor.clone()),
        };
        let meta = ctx.mutation_meta();
        assert_eq!(meta.actor_id, Some(actor));
    }

    #[test]
    fn read_consistency_includes_read_gating() {
        let origin = ReplicaId::new(Uuid::from_bytes([4u8; 16]));
        let mut watermarks = Watermarks::<Applied>::new();
        watermarks
            .observe_at_least(
                &NamespaceId::core(),
                &origin,
                Seq0::new(2),
                HeadStatus::Known([2u8; 32]),
            )
            .expect("watermark");
        let ctx = CliRuntimeCtx {
            repo: PathBuf::from("/tmp/beads"),
            json: false,
            namespace: None,
            durability: None,
            client_request_id: None,
            require_min_seen: Some(watermarks.clone()),
            wait_timeout_ms: Some(50),
            actor_id: None,
        };
        let read = ctx.read_consistency();
        assert_eq!(read.require_min_seen, Some(watermarks));
        assert_eq!(read.wait_timeout_ms, Some(50));
    }

    #[test]
    fn response_payload_returns_ok_payload() {
        let payload = ResponsePayload::synced();
        let out = response_payload(Response::Ok {
            ok: payload.clone(),
        })
        .expect("ok payload");
        assert!(matches!(out, ResponsePayload::Synced(_)));
        assert!(matches!(payload, ResponsePayload::Synced(_)));
    }

    #[test]
    fn response_payload_returns_daemon_error_payload() {
        let payload = ErrorPayload::new(CliErrorCode::Internal.into(), "boom", false)
            .with_details(json!({"component":"cli"}));
        let err = response_payload(Response::Err {
            err: payload.clone(),
        })
        .expect_err("daemon response should error");
        match err {
            RuntimeError::Daemon(daemon) => assert_eq!(daemon.payload(), &payload),
            RuntimeError::Ipc(other) => panic!("unexpected ipc error: {other}"),
        }
    }

    #[test]
    fn daemon_response_error_display_includes_code_message_and_details() {
        let payload = ErrorPayload::new(CliErrorCode::Internal.into(), "boom", false)
            .with_details(json!({"component":"cli"}));
        let code = payload.code.to_string();
        let err = DaemonResponseError::new(payload);
        let rendered = err.to_string();
        assert!(rendered.contains(&code));
        assert!(rendered.contains("boom"));
        assert!(rendered.contains("details:"));
    }
}
