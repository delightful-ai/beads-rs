use clap::{Args, Subcommand};

use super::super::{Ctx, normalize_bead_id, print_ok, send};
use crate::cli::parse::{parse_dep_edge, parse_dep_kind};
use crate::core::DepKind;
use crate::daemon::ipc::{DepPayload, EmptyPayload, IdPayload, Request};
use crate::{Error, Result};

#[derive(Subcommand, Debug)]
pub enum DepCmd {
    /// Add a dependency: FROM depends on TO (FROM waits for TO to complete).
    Add(DepAddArgs),
    /// Remove a dependency between two issues.
    Rm(DepRmArgs),
    /// Show dependency tree for an issue.
    Tree { id: String },
    /// List dependency cycles.
    Cycles,
}

#[derive(Args, Debug)]
pub struct DepAddArgs {
    /// Issue that depends on another (waits for TO to complete).
    pub from: String,
    /// Issue that must complete first (blocks FROM).
    pub to: String,
    #[arg(long, alias = "type", value_parser = parse_dep_kind)]
    pub kind: Option<DepKind>,
}

#[derive(Args, Debug)]
pub struct DepRmArgs {
    pub from: String,
    pub to: String,
    #[arg(long, alias = "type", value_parser = parse_dep_kind)]
    pub kind: Option<DepKind>,
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
                ctx: ctx.mutation_ctx(),
                payload: DepPayload {
                    from: from.as_str().to_string(),
                    to: to.as_str().to_string(),
                    kind,
                },
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
                ctx: ctx.mutation_ctx(),
                payload: DepPayload {
                    from: from.as_str().to_string(),
                    to: to.as_str().to_string(),
                    kind,
                },
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        DepCmd::Tree { id } => {
            let id = normalize_bead_id(&id)?;
            let req = Request::DepTree {
                ctx: ctx.read_ctx(),
                payload: IdPayload {
                    id: id.as_str().to_string(),
                },
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        DepCmd::Cycles => {
            let req = Request::DepCycles {
                ctx: ctx.read_ctx(),
                payload: EmptyPayload {},
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
    }
}

pub(crate) fn render_dep_tree(root: &str, edges: &[crate::api::DepEdge]) -> String {
    if edges.is_empty() {
        return format!("\n{root} has no dependencies\n");
    }
    let mut out = format!("\n🌲 Dependency tree for {root}:\n\n");
    for e in edges {
        out.push_str(&format!("{} → {} ({})\n", e.from, e.to, e.kind));
    }
    out.push('\n');
    out
}

pub(crate) fn render_deps(
    incoming: &[crate::api::DepEdge],
    outgoing: &[crate::api::DepEdge],
) -> String {
    let mut out = String::new();
    if !outgoing.is_empty() {
        out.push_str(&format!("\nDepends on ({}):\n", outgoing.len()));
        for e in outgoing {
            out.push_str(&format!("  → {} ({})\n", e.to, e.kind));
        }
    }
    if !incoming.is_empty() {
        out.push_str(&format!("\nBlocks ({}):\n", incoming.len()));
        for e in incoming {
            out.push_str(&format!("  ← {} ({})\n", e.from, e.kind));
        }
    }
    if out.is_empty() {
        "no deps".into()
    } else {
        out.trim_end().into()
    }
}

pub(crate) fn render_dep_cycles(out: &crate::api::DepCycles) -> String {
    if out.cycles.is_empty() {
        return "no dependency cycles found".into();
    }
    let mut lines = Vec::new();
    for cycle in &out.cycles {
        lines.push(format!("cycle: {}", cycle.join(" -> ")));
    }
    lines.join("\n")
}

pub(crate) fn render_dep_added(from: &str, to: &str) -> String {
    format!("✓ Added dependency: {from} depends on {to}")
}

pub(crate) fn render_dep_removed(from: &str, to: &str) -> String {
    format!("✓ Removed dependency: {from} no longer depends on {to}")
}
