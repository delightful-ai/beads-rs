use clap::Args;

use super::super::{Ctx, normalize_bead_id, print_ok, send};
use crate::Result;
use crate::daemon::ipc::Request;

#[derive(Args, Debug)]
pub struct ClaimArgs {
    pub id: String,

    /// Lease duration in seconds.
    #[arg(long, default_value_t = 3600)]
    pub lease_secs: u64,
}

pub(crate) fn handle(ctx: &Ctx, args: ClaimArgs) -> Result<()> {
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Claim {
        repo: ctx.repo.clone(),
        id: id.as_str().to_string(),
        lease_secs: args.lease_secs,
        meta: ctx.mutation_meta(),
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
