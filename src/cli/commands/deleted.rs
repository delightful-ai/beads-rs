use super::super::render;
use super::super::{Ctx, DeletedArgs, print_ok, send};
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::query::QueryResult;
use crate::{Error, Result};

pub(crate) fn handle(ctx: &Ctx, args: DeletedArgs) -> Result<()> {
    let since_ms = if args.all {
        None
    } else {
        Some(parse_since_ms(&args.since)?)
    };

    let req = Request::Deleted {
        repo: ctx.repo.clone(),
        since_ms,
        id: args.id.clone(),
    };
    let ok = send(&req)?;
    if ctx.json {
        return print_ok(&ok, true);
    }

    match ok {
        ResponsePayload::Query(QueryResult::Deleted(tombs)) => {
            println!(
                "{}",
                render::render_deleted_list(&tombs, &args.since, args.all)
            );
            Ok(())
        }
        other => print_ok(&other, false),
    }
}

fn parse_since_ms(s: &str) -> Result<u64> {
    let s = s.trim().to_lowercase();
    if s.is_empty() {
        return Ok(7 * 24 * 60 * 60 * 1000);
    }

    // days: "7d"
    if let Some(raw) = s.strip_suffix('d') {
        let days: u64 = raw.parse().map_err(|_| {
            Error::Op(crate::daemon::OpError::ValidationFailed {
                field: "since".into(),
                reason: format!("invalid days format: {s}"),
            })
        })?;
        return Ok(days * 24 * 60 * 60 * 1000);
    }

    // weeks: "2w"
    if let Some(raw) = s.strip_suffix('w') {
        let weeks: u64 = raw.parse().map_err(|_| {
            Error::Op(crate::daemon::OpError::ValidationFailed {
                field: "since".into(),
                reason: format!("invalid weeks format: {s}"),
            })
        })?;
        return Ok(weeks * 7 * 24 * 60 * 60 * 1000);
    }

    // hours/minutes/seconds: "12h", "30m", "45s"
    if let Some(raw) = s.strip_suffix('h') {
        let hours: u64 = raw.parse().map_err(|_| {
            Error::Op(crate::daemon::OpError::ValidationFailed {
                field: "since".into(),
                reason: format!("invalid hours format: {s}"),
            })
        })?;
        return Ok(hours * 60 * 60 * 1000);
    }
    if let Some(raw) = s.strip_suffix('m') {
        let minutes: u64 = raw.parse().map_err(|_| {
            Error::Op(crate::daemon::OpError::ValidationFailed {
                field: "since".into(),
                reason: format!("invalid minutes format: {s}"),
            })
        })?;
        return Ok(minutes * 60 * 1000);
    }
    if let Some(raw) = s.strip_suffix('s') {
        let seconds: u64 = raw.parse().map_err(|_| {
            Error::Op(crate::daemon::OpError::ValidationFailed {
                field: "since".into(),
                reason: format!("invalid seconds format: {s}"),
            })
        })?;
        return Ok(seconds * 1000);
    }

    Err(Error::Op(crate::daemon::OpError::ValidationFailed {
        field: "since".into(),
        reason: format!("invalid --since value {s:?} (try 7d, 30d, 2w)"),
    }))
}
