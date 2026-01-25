use clap::{Args, Subcommand};

use super::super::{Ctx, normalize_bead_id, parse_dep_edge, parse_dep_kind, print_ok, send};
use crate::core::DepKind;
use crate::daemon::ipc::Request;
use crate::{Error, Result};

#[derive(Subcommand, Debug)]
pub enum DepCmd {
    /// Add a dependency between beads.
    Add(DepAddArgs),
    /// Remove a dependency between beads.
    #[command(name = "rm")]
    Rm(DepRmArgs),
    /// Show dependency tree.
    Tree { id: String },
    /// Detect dependency cycles.
    Cycles,
}

#[derive(Args, Debug)]
pub struct DepAddArgs {
    /// From issue id.
    pub from: String,
    /// To issue id.
    pub to: String,
    /// Dependency kind (blocks/related/parent/etc).
    #[arg(long, value_parser = parse_dep_kind)]
    pub kind: Option<DepKind>,
}

#[derive(Args, Debug)]
pub struct DepRmArgs {
    /// From issue id.
    pub from: String,
    /// To issue id.
    pub to: String,
    /// Dependency kind (blocks/related/parent/etc).
    #[arg(long, value_parser = parse_dep_kind)]
    pub kind: Option<DepKind>,
}

pub(super) fn cmd_name(cmd: &DepCmd) -> &'static str {
    match cmd {
        DepCmd::Add(_) => "add",
        DepCmd::Rm(_) => "rm",
        DepCmd::Tree { .. } => "tree",
        DepCmd::Cycles => "cycles",
    }
}

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
        DepCmd::Cycles => {
            let req = Request::DepCycles {
                repo: ctx.repo.clone(),
                read: ctx.read_consistency(),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
    }
}
