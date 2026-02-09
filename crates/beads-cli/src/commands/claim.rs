use beads_surface::OpResult;
use beads_surface::ipc::{ClaimPayload, Request, ResponsePayload};
use clap::Args;

use super::{CommandResult, print_ok};
use crate::render::print_line;
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::normalize_bead_id;

#[derive(Args, Debug)]
pub struct ClaimArgs {
    pub id: String,

    /// Lease duration in seconds.
    #[arg(long, default_value_t = 3600)]
    pub lease_secs: u64,
}

pub fn handle(ctx: &CliRuntimeCtx, args: ClaimArgs) -> CommandResult<()> {
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Claim {
        ctx: ctx.mutation_ctx(),
        payload: ClaimPayload {
            id,
            lease_secs: args.lease_secs,
        },
    };
    let ok = send(&req)?;
    if ctx.json {
        return print_ok(&ok, true);
    }
    if let ResponsePayload::Op(op) = &ok {
        match &op.result {
            OpResult::Claimed { id, expires } => {
                print_line(&render_claimed(id.as_str(), expires.0))?;
                return Ok(());
            }
            OpResult::ClaimExtended { id, expires } => {
                print_line(&render_claim_extended(id.as_str(), expires.0))?;
                return Ok(());
            }
            _ => {}
        }
    }
    print_ok(&ok, false)
}

pub fn render_claimed(id: &str, expires: u64) -> String {
    format!("✓ Claimed {id} until {expires}")
}

pub fn render_claim_extended(id: &str, expires: u64) -> String {
    format!("✓ Claim extended for {id} until {expires}")
}
