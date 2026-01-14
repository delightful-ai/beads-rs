use super::super::{Ctx, SubscribeArgs, print_ok};
use crate::core::{Applied, ErrorPayload, Watermarks};
use crate::daemon::OpError;
use crate::daemon::ipc::{Request, Response, subscribe_stream};
use crate::{Error, Result};

pub(crate) fn handle(ctx: &Ctx, args: SubscribeArgs) -> Result<()> {
    let require_min_seen = match args.require_min_seen.as_deref() {
        Some(raw) => Some(parse_require_min_seen(raw)?),
        None => None,
    };

    let mut read = ctx.read_consistency();
    read.require_min_seen = require_min_seen;
    read.wait_timeout_ms = args.wait_timeout_ms;

    let req = Request::Subscribe {
        repo: ctx.repo.clone(),
        read,
    };

    let mut stream = subscribe_stream(&req)?;
    let Some(response) = stream.read_response()? else {
        return Ok(());
    };
    if !handle_response(response, ctx.json)? {
        return Ok(());
    }

    loop {
        match stream.read_response()? {
            Some(response) => {
                if !handle_response(response, ctx.json)? {
                    break;
                }
            }
            None => break,
        }
    }

    Ok(())
}

fn parse_require_min_seen(raw: &str) -> Result<Watermarks<Applied>> {
    serde_json::from_str(raw).map_err(|err| {
        Error::Op(OpError::ValidationFailed {
            field: "require_min_seen".into(),
            reason: err.to_string(),
        })
    })
}

fn handle_response(response: Response, json: bool) -> Result<bool> {
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
