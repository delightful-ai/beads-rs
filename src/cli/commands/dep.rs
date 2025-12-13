use crate::{Error, Result};

use crate::daemon::ipc::Request;

use super::super::{parse_dep_edge, print_ok, send, Ctx, DepCmd};

pub(crate) fn handle(ctx: &Ctx, cmd: DepCmd) -> Result<()> {
    match cmd {
        DepCmd::Add(args) => {
            let (kind, from, to) = parse_dep_edge(args.kind, &args.from, &args.to).map_err(
                |msg| Error::Op(crate::daemon::OpError::ValidationFailed {
                    field: "dep".into(),
                    reason: msg,
                }),
            )?;
            let req = Request::AddDep {
                repo: ctx.repo.clone(),
                from,
                to,
                kind,
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        DepCmd::Rm(args) => {
            let (kind, from, to) = parse_dep_edge(args.kind, &args.from, &args.to).map_err(
                |msg| Error::Op(crate::daemon::OpError::ValidationFailed {
                    field: "dep".into(),
                    reason: msg,
                }),
            )?;
            let req = Request::RemoveDep {
                repo: ctx.repo.clone(),
                from,
                to,
                kind,
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        DepCmd::Tree { id } => {
            let req = Request::DepTree {
                repo: ctx.repo.clone(),
                id,
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
    }
}
