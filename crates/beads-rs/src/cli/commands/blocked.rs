use super::super::{Ctx, print_ok, send};
use super::fmt_issue_ref;
use crate::Result;
use crate::daemon::ipc::{EmptyPayload, Request};

pub(crate) fn handle(ctx: &Ctx) -> Result<()> {
    let req = Request::Blocked {
        ctx: ctx.read_ctx(),
        payload: EmptyPayload {},
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}

pub(crate) fn render_blocked(blocked: &[crate::api::BlockedIssue]) -> String {
    if blocked.is_empty() {
        return "\n✨ No blocked issues\n".into();
    }

    let mut out = format!("\n🚫 Blocked issues ({}):\n\n", blocked.len());
    for b in blocked {
        out.push_str(&format!(
            "[P{}] {}: {}\n",
            b.issue.priority,
            fmt_issue_ref(&b.issue.namespace, &b.issue.id),
            b.issue.title
        ));
        out.push_str(&format!(
            "  Blocked by {} open dependencies: {:?}\n\n",
            b.blocked_by_count, b.blocked_by
        ));
    }
    out.trim_end().into()
}
