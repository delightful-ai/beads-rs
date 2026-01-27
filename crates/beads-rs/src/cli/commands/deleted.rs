use clap::Args;

use super::super::{Ctx, normalize_bead_id, print_ok, send};
use super::fmt_wall_ms;
use crate::api::QueryResult;
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::{Error, Result};

#[derive(Args, Debug)]
pub struct DeletedArgs {
    /// Optional issue id to show details.
    #[arg(value_name = "ISSUE_ID", required = false)]
    pub id: Option<String>,

    /// Show deletions within this time range (e.g., 7d, 30d, 2w).
    #[arg(long, default_value = "7d")]
    pub since: String,

    /// Show all tracked deletions.
    #[arg(long)]
    pub all: bool,
}

pub(crate) fn handle(ctx: &Ctx, args: DeletedArgs) -> Result<()> {
    let since_ms = if args.all {
        None
    } else {
        Some(parse_since_ms(&args.since)?)
    };

    let id = args
        .id
        .as_deref()
        .map(normalize_bead_id)
        .transpose()?
        .map(|id| id.as_str().to_string());
    let req = Request::Deleted {
        repo: ctx.repo.clone(),
        since_ms,
        id,
        read: ctx.read_consistency(),
    };
    let ok = send(&req)?;
    if ctx.json {
        return print_ok(&ok, true);
    }

    match ok {
        ResponsePayload::Query(QueryResult::Deleted(tombs)) => {
            println!("{}", render_deleted_list(&tombs, &args.since, args.all));
            Ok(())
        }
        other => print_ok(&other, false),
    }
}

pub(crate) fn render_deleted_list(
    tombs: &[crate::api::Tombstone],
    since: &str,
    all: bool,
) -> String {
    if tombs.is_empty() {
        if all {
            return "\n✨ No deletions tracked\n".into();
        }
        return format!("\n✨ No deletions in the last {since}\n");
    }

    let mut out = if all {
        format!("\n🗑️ All tracked deletions ({} total):\n\n", tombs.len())
    } else {
        format!(
            "\n🗑️ Deletions in the last {since} ({} total):\n\n",
            tombs.len()
        )
    };

    for t in tombs {
        let ts = fmt_wall_ms(t.deleted_at.wall_ms);
        let reason = t.reason.as_deref().unwrap_or("");
        let reason = if reason.is_empty() {
            "".to_string()
        } else {
            format!("  {reason}")
        };
        out.push_str(&format!(
            "  {:<12}  {}  {:<12}{}\n",
            t.id, ts, t.deleted_by, reason
        ));
    }

    out.trim_end().into()
}

pub(crate) fn render_deleted(tombs: &[crate::api::Tombstone]) -> String {
    if tombs.is_empty() {
        return "\n✨ No deletions tracked\n".into();
    }

    let mut out = format!("\n🗑️ Deleted issues ({}):\n\n", tombs.len());
    for t in tombs {
        let ts = fmt_wall_ms(t.deleted_at.wall_ms);
        let reason = t.reason.as_deref().unwrap_or("");
        if reason.is_empty() {
            out.push_str(&format!("  {:<12}  {}  {}\n", t.id, ts, t.deleted_by));
        } else {
            out.push_str(&format!(
                "  {:<12}  {}  {}  {}\n",
                t.id, ts, t.deleted_by, reason
            ));
        }
    }
    out.trim_end().into()
}

pub(crate) fn render_deleted_lookup(out: &crate::api::DeletedLookup) -> String {
    if !out.found {
        return format!(
            "Issue {} not found in tombstones\n(This could mean the issue was never deleted, or the record was pruned)",
            out.id
        );
    }
    let record = match &out.record {
        Some(r) => r,
        None => {
            return format!(
                "Issue {} not found in tombstones\n(This could mean the issue was never deleted, or the record was pruned)",
                out.id
            );
        }
    };

    let mut buf = format!("\n🗑️ Deletion record for {}:\n\n", out.id);
    buf.push_str(&format!("  ID:        {}\n", record.id));
    buf.push_str(&format!(
        "  Deleted:   {}\n",
        fmt_wall_ms(record.deleted_at.wall_ms)
    ));
    buf.push_str(&format!("  By:        {}\n", record.deleted_by));
    if let Some(reason) = &record.reason
        && !reason.is_empty()
    {
        buf.push_str(&format!("  Reason:    {}\n", reason));
    }
    buf.push('\n');
    buf
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
