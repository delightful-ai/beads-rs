use clap::Args;

use super::super::validation::normalize_bead_id;
use super::super::{Ctx, print_ok, send};
use crate::Result;
use crate::daemon::ipc::{ClaimPayload, Request};

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
        ctx: ctx.mutation_ctx(),
        payload: ClaimPayload {
            id: id.clone(),
            lease_secs: args.lease_secs,
        },
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}

pub(crate) fn render_claimed(id: &str, expires: u64) -> String {
    format!("✓ Claimed {id} until {expires}")
}

pub(crate) fn render_claim_extended(id: &str, expires: u64) -> String {
    format!("✓ Claim extended for {id} until {expires}")
}
