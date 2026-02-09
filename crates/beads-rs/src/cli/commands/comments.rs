use super::super::Ctx;
use crate::Result;

pub type CommentsArgs = beads_cli::commands::comments::CommentsArgs;
pub type CommentsCmd = beads_cli::commands::comments::CommentsCmd;
pub type CommentAddArgs = beads_cli::commands::comments::CommentAddArgs;

pub(crate) fn handle_comments(ctx: &Ctx, args: CommentsArgs) -> Result<()> {
    beads_cli::commands::comments::handle_comments(ctx, args).map_err(Into::into)
}

pub(crate) fn handle_comment_add(ctx: &Ctx, args: CommentAddArgs) -> Result<()> {
    beads_cli::commands::comments::handle_comment_add(ctx, args).map_err(Into::into)
}
