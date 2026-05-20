use clap::{Args, Subcommand};

use super::{CommandResult, print_ok};
use crate::parsers::{parse_dep_edge, parse_dep_kind};
use crate::render::{dep_record_json_value, dependency_summary_json_value, print_json};
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::{normalize_bead_id, normalize_bead_ids, validation_error};
use beads_api::{DepCycles, DepEdge};
use beads_core::{BeadId, DepKind};
use beads_surface::Filters;
use beads_surface::ipc::{
    DepPayload, EmptyPayload, IdPayload, ListPayload, Request, ResponsePayload,
};
use std::collections::HashMap;

#[derive(Subcommand, Debug)]
pub enum DepCmd {
    /// Add a dependency: FROM depends on TO (FROM waits for TO to complete).
    Add(DepAddArgs),
    /// Remove a dependency between two issues.
    #[command(visible_alias = "remove")]
    Rm(DepRmArgs),
    /// List dependencies for one or more issues.
    List(DepListArgs),
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
    #[arg(long, visible_alias = "type", alias = "type", value_parser = parse_dep_kind)]
    pub kind: Option<DepKind>,
}

#[derive(Args, Debug)]
pub struct DepRmArgs {
    pub from: String,
    pub to: String,
    #[arg(long, visible_alias = "type", alias = "type", value_parser = parse_dep_kind)]
    pub kind: Option<DepKind>,
}

#[derive(Args, Debug)]
pub struct DepListArgs {
    /// Issue id(s) whose dependency edges should be listed.
    #[arg(required = true, num_args = 1..)]
    pub ids: Vec<String>,

    /// Direction: down lists dependencies; up lists dependents.
    #[arg(long, default_value = "down", value_parser = parse_direction)]
    pub direction: DepDirection,

    /// Filter by dependency type.
    #[arg(long, visible_alias = "type", alias = "type", value_parser = parse_dep_kind)]
    pub kind: Option<DepKind>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DepDirection {
    Down,
    Up,
}

pub fn handle(ctx: &CliRuntimeCtx, cmd: DepCmd) -> CommandResult<()> {
    match cmd {
        DepCmd::Add(args) => {
            let (kind, from, to) = parse_dep_edge(args.kind, &args.from, &args.to)
                .map_err(|err| validation_error("dep", err.to_string()))?;
            let req = Request::AddDep {
                ctx: ctx.mutation_ctx(),
                payload: DepPayload { from, to, kind },
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        DepCmd::Rm(args) => handle_dep_remove(ctx, args),
        DepCmd::List(args) => handle_dep_list(ctx, args),
        DepCmd::Tree { id } => {
            let id = normalize_bead_id(&id)?;
            let req = Request::DepTree {
                ctx: ctx.read_ctx(),
                payload: IdPayload { id },
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

fn handle_dep_remove(ctx: &CliRuntimeCtx, args: DepRmArgs) -> CommandResult<()> {
    let explicit_kind = args.kind.is_some() || args.from.contains(':') || args.to.contains(':');
    let (kind, from, to) = parse_dep_edge(args.kind, &args.from, &args.to)
        .map_err(|err| validation_error("dep", err.to_string()))?;

    if explicit_kind {
        let req = Request::RemoveDep {
            ctx: ctx.mutation_ctx(),
            payload: DepPayload { from, to, kind },
        };
        let ok = send(&req)?;
        return print_ok(&ok, ctx.json);
    }

    let (_, outgoing) = fetch_dep_edges(ctx, &from)?;
    let mut removed = None;
    for edge in outgoing.into_iter().filter(|edge| edge.to == to.as_str()) {
        let kind =
            DepKind::parse(&edge.kind).map_err(|err| validation_error("dep", err.to_string()))?;
        let req = Request::RemoveDep {
            ctx: ctx.mutation_ctx(),
            payload: DepPayload {
                from: from.clone(),
                to: to.clone(),
                kind,
            },
        };
        removed = Some(send(&req)?);
    }

    if let Some(ok) = removed {
        print_ok(&ok, ctx.json)
    } else {
        let req = Request::RemoveDep {
            ctx: ctx.mutation_ctx(),
            payload: DepPayload { from, to, kind },
        };
        let ok = send(&req)?;
        print_ok(&ok, ctx.json)
    }
}

fn handle_dep_list(ctx: &CliRuntimeCtx, args: DepListArgs) -> CommandResult<()> {
    let ids = normalize_bead_ids(args.ids)?;
    let kind_filter = args.kind.map(|kind| kind.as_str().to_string());

    if ctx.json && ids.len() > 1 && args.direction == DepDirection::Down {
        let mut records = Vec::new();
        for id in &ids {
            let (_, outgoing) = fetch_dep_edges(ctx, id)?;
            for edge in outgoing {
                if kind_matches(kind_filter.as_deref(), &edge.kind) {
                    records.push(dep_record_json_value(&edge.from, &edge.to, &edge.kind));
                }
            }
        }
        print_json(&serde_json::Value::Array(records))?;
        return Ok(());
    }

    let mut all_edges = Vec::new();
    for id in &ids {
        let (incoming, outgoing) = fetch_dep_edges(ctx, id)?;
        let selected = match args.direction {
            DepDirection::Down => outgoing,
            DepDirection::Up => incoming,
        };
        all_edges.extend(
            selected
                .into_iter()
                .filter(|edge| kind_matches(kind_filter.as_deref(), &edge.kind)),
        );
    }

    if ctx.json {
        let target_ids = all_edges
            .iter()
            .map(|edge| match args.direction {
                DepDirection::Down => edge.to.clone(),
                DepDirection::Up => edge.from.clone(),
            })
            .filter_map(|id| BeadId::parse(&id).ok())
            .collect::<Vec<_>>();
        let summaries = fetch_summary_map(ctx, target_ids)?;
        let mut out = Vec::new();
        for edge in all_edges {
            let target = match args.direction {
                DepDirection::Down => edge.to.as_str(),
                DepDirection::Up => edge.from.as_str(),
            };
            if let Some(summary) = summaries.get(target) {
                out.push(dependency_summary_json_value(summary, &edge.kind));
            }
        }
        print_json(&serde_json::Value::Array(out))?;
        return Ok(());
    }

    let (incoming, outgoing): (Vec<_>, Vec<_>) = match args.direction {
        DepDirection::Down => (Vec::new(), all_edges),
        DepDirection::Up => (all_edges, Vec::new()),
    };
    crate::render::print_line(&render_deps(&incoming, &outgoing))?;
    Ok(())
}

fn fetch_dep_edges(
    ctx: &CliRuntimeCtx,
    id: &BeadId,
) -> CommandResult<(Vec<DepEdge>, Vec<DepEdge>)> {
    let req = Request::Deps {
        ctx: ctx.read_ctx(),
        payload: IdPayload { id: id.clone() },
    };
    match send(&req)? {
        ResponsePayload::Query(beads_api::QueryResult::Deps { incoming, outgoing }) => {
            Ok((incoming, outgoing))
        }
        other => Err(beads_surface::ipc::IpcError::DaemonUnavailable(format!(
            "unexpected response for dep list: {other:?}"
        ))
        .into()),
    }
}

fn fetch_summary_map(
    ctx: &CliRuntimeCtx,
    ids: Vec<BeadId>,
) -> CommandResult<HashMap<String, beads_api::IssueSummary>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let req = Request::List {
        ctx: ctx.read_ctx(),
        payload: ListPayload {
            filters: Filters {
                ids: Some(ids),
                ..Filters::default()
            },
        },
    };
    match send(&req)? {
        ResponsePayload::Query(beads_api::QueryResult::Issues(issues)) => Ok(issues
            .into_iter()
            .map(|issue| (issue.id.clone(), issue))
            .collect()),
        other => Err(beads_surface::ipc::IpcError::DaemonUnavailable(format!(
            "unexpected response for dependency summaries: {other:?}"
        ))
        .into()),
    }
}

fn kind_matches(filter: Option<&str>, kind: &str) -> bool {
    filter.is_none_or(|filter| filter == kind)
}

fn parse_direction(raw: &str) -> Result<DepDirection, String> {
    match raw.trim().to_lowercase().as_str() {
        "down" | "dependencies" => Ok(DepDirection::Down),
        "up" | "dependents" => Ok(DepDirection::Up),
        other => Err(format!("unsupported direction `{other}` (use up or down)")),
    }
}

pub fn render_dep_tree(root: &beads_core::BeadRef, edges: &[DepEdge]) -> String {
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

pub fn render_deps(incoming: &[DepEdge], outgoing: &[DepEdge]) -> String {
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

pub fn render_dep_cycles(out: &DepCycles) -> String {
    if out.cycles.is_empty() {
        return "no dependency cycles found".into();
    }
    let mut lines = Vec::new();
    for cycle in &out.cycles {
        lines.push(format!("cycle: {}", cycle.join(" -> ")));
    }
    lines.join("\n")
}

pub fn render_dep_added(from: &str, to: &str) -> String {
    format!("✓ Added dependency: {from} depends on {to}")
}

pub fn render_dep_removed(from: &str, to: &str) -> String {
    format!("✓ Removed dependency: {from} no longer depends on {to}")
}
