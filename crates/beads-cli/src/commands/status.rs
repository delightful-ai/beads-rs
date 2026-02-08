use std::sync::LazyLock;

use beads_api::{QueryResult, StatusOutput, SyncWarning};
use beads_surface::ipc::{EmptyPayload, Request, ResponsePayload};

use super::{CommandResult, print_ok};
use crate::render::print_line;
use crate::runtime::{CliRuntimeCtx, send};

static WALL_MS_FORMAT: LazyLock<Option<Vec<time::format_description::FormatItem<'static>>>> =
    LazyLock::new(|| time::format_description::parse("[year]-[month]-[day] [hour]:[minute]").ok());

pub fn handle(ctx: &CliRuntimeCtx) -> CommandResult<()> {
    let req = Request::Status {
        ctx: ctx.read_ctx(),
        payload: EmptyPayload {},
    };
    let ok = send(&req)?;
    if !ctx.json
        && let ResponsePayload::Query(QueryResult::Status(out)) = &ok
    {
        print_line(&render_status(out))?;
        return Ok(());
    }
    print_ok(&ok, ctx.json)
}

pub fn render_status(out: &StatusOutput) -> String {
    let summary = &out.summary;
    let mut buf = String::new();
    buf.push_str("\nIssue Database Status\n=====================\n\nSummary:\n");
    buf.push_str(&format!("  Total Issues:      {}\n", summary.total_issues));
    buf.push_str(&format!("  Open:              {}\n", summary.open_issues));
    buf.push_str(&format!(
        "  In Progress:       {}\n",
        summary.in_progress_issues
    ));
    buf.push_str(&format!(
        "  Blocked:           {}\n",
        summary.blocked_issues
    ));
    buf.push_str(&format!("  Closed:            {}\n", summary.closed_issues));
    buf.push_str(&format!("  Ready to Work:     {}\n", summary.ready_issues));
    if let Some(tombstones) = summary.tombstone_issues
        && tombstones > 0
    {
        buf.push_str(&format!(
            "  Deleted:           {} (tombstones)\n",
            tombstones
        ));
    }
    if let Some(epics) = summary.epics_eligible_for_closure
        && epics > 0
    {
        buf.push_str(&format!("  Epics Ready to Close: {}\n", epics));
    }
    if let Some(sync) = &out.sync {
        let last_sync = sync
            .last_sync_wall_ms
            .map(fmt_wall_ms)
            .unwrap_or_else(|| "never".to_string());
        buf.push_str("\nSync:\n");
        buf.push_str(&format!("  dirty:             {}\n", sync.dirty));
        buf.push_str(&format!("  in_progress:       {}\n", sync.sync_in_progress));
        buf.push_str(&format!("  last_sync:         {}\n", last_sync));
        if let Some(next_retry) = sync.next_retry_wall_ms {
            let mut line = format!("  next_retry:       {}", fmt_wall_ms(next_retry));
            if let Some(in_ms) = sync.next_retry_in_ms {
                line.push_str(&format!(" (in {})", fmt_duration_ms(in_ms)));
            }
            line.push('\n');
            buf.push_str(&line);
        }
        buf.push_str(&format!(
            "  consecutive_failures: {}\n",
            sync.consecutive_failures
        ));
        if !sync.warnings.is_empty() {
            buf.push_str("  warnings:\n");
            for warning in &sync.warnings {
                match warning {
                    SyncWarning::Fetch {
                        message,
                        at_wall_ms,
                    } => {
                        buf.push_str(&format!(
                            "    fetch_error: {} (at {})\n",
                            message,
                            fmt_wall_ms(*at_wall_ms)
                        ));
                    }
                    SyncWarning::Diverged {
                        local_oid,
                        remote_oid,
                        at_wall_ms,
                    } => {
                        buf.push_str(&format!(
                            "    divergence: local {} remote {} (at {})\n",
                            local_oid,
                            remote_oid,
                            fmt_wall_ms(*at_wall_ms)
                        ));
                    }
                    SyncWarning::ForcePush {
                        previous_remote_oid,
                        remote_oid,
                        at_wall_ms,
                    } => {
                        buf.push_str(&format!(
                            "    force_push: {} -> {} (at {})\n",
                            previous_remote_oid,
                            remote_oid,
                            fmt_wall_ms(*at_wall_ms)
                        ));
                    }
                    SyncWarning::ClockSkew {
                        delta_ms,
                        at_wall_ms,
                    } => {
                        let direction = if *delta_ms >= 0 { "ahead" } else { "behind" };
                        let abs_ms = delta_ms.unsigned_abs();
                        buf.push_str(&format!(
                            "    clock_skew: {} ms {} (at {})\n",
                            abs_ms,
                            direction,
                            fmt_wall_ms(*at_wall_ms)
                        ));
                    }
                    SyncWarning::WalTailTruncated {
                        namespace,
                        segment_id,
                        truncated_from_offset,
                        at_wall_ms,
                    } => {
                        let segment = segment_id
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "unknown".to_string());
                        buf.push_str(&format!(
                            "    wal_tail_truncated: {} segment {} offset {} (at {})\n",
                            namespace,
                            segment,
                            truncated_from_offset,
                            fmt_wall_ms(*at_wall_ms)
                        ));
                    }
                }
            }
        }
    }
    buf.push('\n');
    buf
}

fn fmt_wall_ms(ms: u64) -> String {
    use time::OffsetDateTime;

    let dt = OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    match WALL_MS_FORMAT.as_deref() {
        Some(format) => dt.format(format).unwrap_or_else(|_| ms.to_string()),
        None => ms.to_string(),
    }
}

fn fmt_duration_ms(ms: u64) -> String {
    if ms < 1000 {
        return format!("{ms}ms");
    }
    let secs = ms as f64 / 1000.0;
    if secs < 60.0 {
        return format!("{secs:.1}s");
    }
    let mins = secs / 60.0;
    if mins < 60.0 {
        return format!("{mins:.1}m");
    }
    let hours = mins / 60.0;
    format!("{hours:.1}h")
}
