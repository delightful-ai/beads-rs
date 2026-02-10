use clap::Args;

use super::print_ok;
use super::{CommandError, CommandResult};
use crate::runtime::{CliRuntimeCtx, DaemonResponseError, RuntimeError};
use beads_core::ErrorPayload;
use beads_surface::ipc::{EmptyPayload, Request, Response, subscribe_stream};

#[derive(Args, Debug, Default)]
pub struct SubscribeArgs {}

pub fn handle(ctx: &CliRuntimeCtx, _args: SubscribeArgs) -> CommandResult<()> {
    let req = Request::Subscribe {
        ctx: ctx.read_ctx(),
        payload: EmptyPayload {},
    };

    let mut stream = subscribe_stream(&req)?;
    let Some(response) = stream.read_response()? else {
        return Ok(());
    };
    if !handle_response(response, ctx.json)? {
        return Ok(());
    }

    while let Some(response) = stream.read_response()? {
        if !handle_response(response, ctx.json)? {
            break;
        }
    }

    Ok(())
}

fn handle_response(response: Response, json: bool) -> CommandResult<bool> {
    match response {
        Response::Ok { ok } => {
            print_ok(&ok, json)?;
            Ok(true)
        }
        Response::Err { err } => {
            print_error(&err, json);
            if err.retryable {
                Ok(false)
            } else {
                Err(CommandError::Runtime(RuntimeError::Daemon(
                    DaemonResponseError::new(err),
                )))
            }
        }
    }
}

fn print_error(err: &ErrorPayload, json: bool) {
    if json {
        if let Ok(payload) = serde_json::to_string(err) {
            eprintln!("{payload}");
        } else {
            eprintln!("error: {} - {}", err.code, err.message);
        }
        return;
    }

    eprintln!("error: {} - {}", err.code, err.message);
    if let Some(details) = &err.details {
        eprintln!("details: {details}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beads_core::CliErrorCode;

    #[test]
    fn handle_response_stops_stream_on_retryable_error() {
        let response = Response::Err {
            err: ErrorPayload::new(CliErrorCode::DaemonUnavailable.into(), "temp", true),
        };
        let outcome = handle_response(response, false).expect("retryable errors stop stream");
        assert!(!outcome);
    }

    #[test]
    fn handle_response_returns_error_on_non_retryable_error() {
        let payload = ErrorPayload::new(CliErrorCode::Internal.into(), "fatal", false);
        let response = Response::Err {
            err: payload.clone(),
        };
        let err = handle_response(response, false).expect_err("non-retryable should error");
        match err {
            CommandError::Runtime(RuntimeError::Daemon(daemon)) => {
                assert_eq!(daemon.payload(), &payload);
            }
            other => panic!("unexpected error variant: {other}"),
        }
    }
}
