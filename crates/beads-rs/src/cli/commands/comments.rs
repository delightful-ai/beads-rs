use clap::{Args, Subcommand};

use super::super::{Ctx, normalize_bead_id, print_ok, send};
use super::fmt_wall_ms;
use crate::api::QueryResult;
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::{Error, Result};

#[derive(Args, Debug)]
pub struct CommentsArgs {
    /// Optional subcommand.
    #[command(subcommand)]
    pub cmd: Option<CommentsCmd>,

    /// Issue ID (lists comments when provided without a subcommand).
    #[arg(value_name = "ID", required = false)]
    pub id: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum CommentsCmd {
    /// Add a comment to an issue.
    Add(CommentAddArgs),
}

#[derive(Args, Debug)]
pub struct CommentAddArgs {
    pub id: String,

    /// Comment content (rest of args). If empty, reads stdin.
    #[arg(trailing_var_arg = true, num_args = 0..)]
    pub content: Vec<String>,
}

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
            let id = id_for_render.as_str().to_string();
            let req = Request::Notes {
                repo: ctx.repo.clone(),
                id,
                read: ctx.read_consistency(),
            };
            let ok = send(&req)?;
            if ctx.json {
                return print_ok(&ok, true);
            }
            match ok {
                ResponsePayload::Query(QueryResult::Notes(notes)) => {
                    // Render like beads-go: "Comments on <id>:" + entries.
                    println!("{}", render_comments_list(id_for_render.as_str(), &notes));
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
        id: id.as_str().to_string(),
        content,
        meta: ctx.mutation_meta(),
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}

pub(crate) fn render_comments_list(issue_id: &str, notes: &[crate::api::Note]) -> String {
    if notes.is_empty() {
        return format!("No comments on {issue_id}");
    }

    let mut out = format!("\nComments on {issue_id}:\n\n");
    for n in notes {
        out.push_str(&format!(
            "[{}] {} at {}\n\n",
            n.author,
            n.content,
            fmt_wall_ms(n.at.wall_ms),
        ));
    }
    out.trim_end().into()
}

pub(crate) fn render_notes(notes: &[crate::api::Note]) -> String {
    if notes.is_empty() {
        return "No comments".into();
    }
    let mut out = String::new();
    out.push_str("\nComments:\n\n");
    for n in notes {
        out.push_str(&format!(
            "[{}] {} at {}\n\n",
            n.author,
            n.content,
            fmt_wall_ms(n.at.wall_ms)
        ));
    }
    out.trim_end().into()
}

pub(crate) fn render_comment_added(issue_id: &str) -> String {
    format!("Comment added to {issue_id}")
}
