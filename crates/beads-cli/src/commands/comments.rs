use clap::{Args, Subcommand};

use super::common::fmt_wall_ms;
use super::{CommandResult, print_ok};
use crate::render::print_line;
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::{normalize_bead_id, validation_error};
use beads_api::{Note, QueryResult};
use beads_surface::ipc::{AddNotePayload, IdPayload, Request, ResponsePayload};

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

pub fn handle_comments(ctx: &CliRuntimeCtx, args: CommentsArgs) -> CommandResult<()> {
    match args.cmd {
        Some(CommentsCmd::Add(add)) => handle_comment_add(ctx, add),
        None => {
            let id = args
                .id
                .ok_or_else(|| validation_error("comments", "missing issue id"))?;
            let id_for_render = normalize_bead_id(&id)?;
            let req = Request::Notes {
                ctx: ctx.read_ctx(),
                payload: IdPayload {
                    id: id_for_render.clone(),
                },
            };
            let ok = send(&req)?;
            if ctx.json {
                return print_ok(&ok, true);
            }
            match ok {
                ResponsePayload::Query(QueryResult::Notes(notes)) => {
                    print_line(&render_comments_list(id_for_render.as_str(), &notes))?;
                    Ok(())
                }
                other => print_ok(&other, false),
            }
        }
    }
}

pub fn handle_comment_add(ctx: &CliRuntimeCtx, args: CommentAddArgs) -> CommandResult<()> {
    let content = if !args.content.is_empty() {
        args.content.join(" ")
    } else {
        use std::io::Read;
        let mut s = String::new();
        std::io::stdin()
            .read_to_string(&mut s)
            .map_err(beads_surface::ipc::IpcError::from)?;
        s.trim().to_string()
    };

    let id = normalize_bead_id(&args.id)?;
    let req = Request::AddNote {
        ctx: ctx.mutation_ctx(),
        payload: AddNotePayload {
            id: id.clone(),
            content,
        },
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}

pub fn render_comments_list(issue_id: &str, notes: &[Note]) -> String {
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

pub fn render_notes(notes: &[Note]) -> String {
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

pub fn render_comment_added(issue_id: &str) -> String {
    format!("Comment added to {issue_id}")
}
