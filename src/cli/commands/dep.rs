use super::super::{Ctx, DepCmd, normalize_bead_id, parse_dep_edge, print_ok, send};
use crate::daemon::ipc::Request;
use crate::{Error, Result};

pub(crate) fn handle(ctx: &Ctx, cmd: DepCmd) -> Result<()> {
    match cmd {
        DepCmd::Add(args) => {
            let (kind, from, to) =
                parse_dep_edge(args.kind, &args.from, &args.to).map_err(|msg| {
                    Error::Op(crate::daemon::OpError::ValidationFailed {
                        field: "dep".into(),
                        reason: msg,
                    })
                })?;
            let req = Request::AddDep {
                repo: ctx.repo.clone(),
                from: from.as_str().to_string(),
                to: to.as_str().to_string(),
                kind,
                meta: ctx.mutation_meta(),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        DepCmd::Rm(args) => {
            let (kind, from, to) =
                parse_dep_edge(args.kind, &args.from, &args.to).map_err(|msg| {
                    Error::Op(crate::daemon::OpError::ValidationFailed {
                        field: "dep".into(),
                        reason: msg,
                    })
                })?;
            let req = Request::RemoveDep {
                repo: ctx.repo.clone(),
                from: from.as_str().to_string(),
                to: to.as_str().to_string(),
                kind,
                meta: ctx.mutation_meta(),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        DepCmd::Tree { id } => {
            let id = normalize_bead_id(&id)?;
            let req = Request::DepTree {
                repo: ctx.repo.clone(),
                id: id.as_str().to_string(),
                read: ctx.read_consistency(),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
    }
}
