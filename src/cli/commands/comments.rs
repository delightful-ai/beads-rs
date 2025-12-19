use super::super::render;
use super::super::{
    CommentAddArgs, CommentsArgs, CommentsCmd, Ctx, normalize_bead_id, print_ok, send,
};
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::query::QueryResult;
use crate::{Error, Result};

pub(crate) fn handle_comments(ctx: &Ctx, args: CommentsArgs) -> Result<()> {
    match args.cmd {
        Some(CommentsCmd::Add(add)) => handle_comment_add(ctx, add),
        None => {
            let id = args.id.ok_or_else(|| {
                Error::Op(crate::daemon::OpError::ValidationFailed {
                    field: "comments".into(),
                    reason: "missing issue id".into(),
                })
            })?;
            let id_for_render = normalize_bead_id(&id)?;
            let id = id_for_render.clone();
            let req = Request::Notes {
                repo: ctx.repo.clone(),
                id,
            };
            let ok = send(&req)?;
            if ctx.json {
                return print_ok(&ok, true);
            }
            match ok {
                ResponsePayload::Query(QueryResult::Notes(notes)) => {
                    // Render like beads-go: "Comments on <id>:" + entries.
                    println!("{}", render::render_comments_list(&id_for_render, &notes));
                    Ok(())
                }
                other => print_ok(&other, false),
            }
        }
    }
}

pub(crate) fn handle_comment_add(ctx: &Ctx, args: CommentAddArgs) -> Result<()> {
    let content = if !args.content.is_empty() {
        args.content.join(" ")
    } else {
        use std::io::Read;
        let mut s = String::new();
        std::io::stdin()
            .read_to_string(&mut s)
            .map_err(crate::daemon::IpcError::from)?;
        s.trim().to_string()
    };

    let id = normalize_bead_id(&args.id)?;
    let req = Request::AddNote {
        repo: ctx.repo.clone(),
        id,
        content,
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
