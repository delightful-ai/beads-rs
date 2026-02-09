use clap::Args;

use super::CommandResult;
use super::print_ok;
use crate::runtime::CliRuntimeCtx;
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
                std::process::exit(1);
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
