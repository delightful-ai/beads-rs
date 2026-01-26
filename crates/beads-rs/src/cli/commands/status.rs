use super::super::{Ctx, print_ok, send};
use super::{fmt_duration_ms, fmt_wall_ms};
use crate::Result;
use crate::daemon::ipc::Request;

pub(crate) fn handle(ctx: &Ctx) -> Result<()> {
    let req = Request::Status {
        repo: ctx.repo.clone(),
        read: ctx.read_consistency(),
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}

pub(crate) fn render_status(out: &crate::api::StatusOutput) -> String {
    let s = &out.summary;
    let mut buf = String::new();
    buf.push_str("\nIssue Database Status\n=====================\n\nSummary:\n");
    buf.push_str(&format!("  Total Issues:      {}\n", s.total_issues));
    buf.push_str(&format!("  Open:              {}\n", s.open_issues));
    buf.push_str(&format!("  In Progress:       {}\n", s.in_progress_issues));
    buf.push_str(&format!("  Blocked:           {}\n", s.blocked_issues));
    buf.push_str(&format!("  Closed:            {}\n", s.closed_issues));
    buf.push_str(&format!("  Ready to Work:     {}\n", s.ready_issues));
    if let Some(t) = s.tombstone_issues
        && t > 0
    {
        buf.push_str(&format!("  Deleted:           {} (tombstones)\n", t));
    }
    if let Some(e) = s.epics_eligible_for_closure
        && e > 0
    {
        buf.push_str(&format!("  Epics Ready to Close: {}\n", e));
    }
    if let Some(sync) = &out.sync {
        let last = sync
            .last_sync_wall_ms
            .map(fmt_wall_ms)
            .unwrap_or_else(|| "never".into());
        buf.push_str("\nSync:\n");
        buf.push_str(&format!("  dirty:             {}\n", sync.dirty));
        buf.push_str(&format!("  in_progress:       {}\n", sync.sync_in_progress));
        buf.push_str(&format!("  last_sync:         {}\n", last));
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
                    crate::api::SyncWarning::Fetch {
                        message,
                        at_wall_ms,
                    } => {
                        buf.push_str(&format!(
                            "    fetch_error: {} (at {})\n",
                            message,
                            fmt_wall_ms(*at_wall_ms)
                        ));
                    }
                    crate::api::SyncWarning::Diverged {
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
                    crate::api::SyncWarning::ForcePush {
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
                    crate::api::SyncWarning::ClockSkew {
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
                    crate::api::SyncWarning::WalTailTruncated {
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
