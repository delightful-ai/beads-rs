use clap::{Args, Subcommand};

use super::{CommandResult, print_ok};
use crate::commands::common::{dep_edges_need_namespace, fmt_dep_endpoint};
use crate::parsers::parse_dep_kind;
use crate::render::{dep_record_json_value, dependency_summary_json_value_for_edge, print_json};
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::{normalize_bead_ref_for, validation_error};
use beads_api::{DepCycles, DepEdge};
use beads_core::{BeadId, BeadRef, DepKind, NamespaceId};
use beads_surface::Filters;
use beads_surface::ipc::{
    DepPayload, EmptyPayload, IdPayload, ListPayload, Request, ResponsePayload,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

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

#[derive(Clone, Debug)]
struct DepRefEdge {
    kind: DepKind,
    from: BeadRef,
    to: BeadRef,
}

fn parse_bead_ref_arg(ctx: &CliRuntimeCtx, field: &str, raw: &str) -> CommandResult<BeadRef> {
    let default_namespace = ctx.active_namespace();
    normalize_bead_ref_for(field, raw, &default_namespace).map_err(Into::into)
}

fn parse_dep_ref_edge(
    ctx: &CliRuntimeCtx,
    kind_flag: Option<DepKind>,
    from_raw: &str,
    to_raw: &str,
) -> CommandResult<DepRefEdge> {
    let default_namespace = ctx.active_namespace();
    let (kind_from, from_raw) = split_kind_ref(from_raw)?;
    let (kind_to, to_raw) = split_kind_ref(to_raw)?;
    let kind = kind_flag
        .or(kind_from)
        .or(kind_to)
        .unwrap_or(DepKind::Blocks);
    let from = normalize_bead_ref_for("from", &from_raw, &default_namespace)?;
    let to = normalize_bead_ref_for("to", &to_raw, &default_namespace)?;
    Ok(DepRefEdge { kind, from, to })
}

fn split_kind_ref(raw: &str) -> CommandResult<(Option<DepKind>, String)> {
    if let Some((kind, id)) = raw.split_once(':') {
        let kind = parse_dep_kind(kind).map_err(|err| validation_error("kind", err.to_string()))?;
        Ok((Some(kind), id.trim().to_string()))
    } else {
        Ok((None, raw.trim().to_string()))
    }
}

fn dep_payload(edge: &DepRefEdge) -> DepPayload {
    DepPayload {
        from_namespace: Some(edge.from.namespace().clone()),
        from: edge.from.id().clone(),
        to_namespace: Some(edge.to.namespace().clone()),
        to: edge.to.id().clone(),
        kind: edge.kind,
    }
}

pub fn handle(ctx: &CliRuntimeCtx, cmd: DepCmd) -> CommandResult<()> {
    match cmd {
        DepCmd::Add(args) => {
            let edge = parse_dep_ref_edge(ctx, args.kind, &args.from, &args.to)?;
            let mutation_ctx = ctx.with_namespace(edge.from.namespace().clone());
            let req = Request::AddDep {
                ctx: mutation_ctx.mutation_ctx(),
                payload: dep_payload(&edge),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        DepCmd::Rm(args) => handle_dep_remove(ctx, args),
        DepCmd::List(args) => handle_dep_list(ctx, args),
        DepCmd::Tree { id } => {
            let bead_ref = parse_bead_ref_arg(ctx, "id", &id)?;
            let read_ctx = ctx.with_namespace(bead_ref.namespace().clone());
            let req = Request::DepTree {
                ctx: read_ctx.read_ctx(),
                payload: IdPayload {
                    id: bead_ref.id().clone(),
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

fn handle_dep_remove(ctx: &CliRuntimeCtx, args: DepRmArgs) -> CommandResult<()> {
    let explicit_kind = args.kind.is_some() || args.from.contains(':') || args.to.contains(':');
    let edge = parse_dep_ref_edge(ctx, args.kind, &args.from, &args.to)?;
    let mutation_ctx = ctx.with_namespace(edge.from.namespace().clone());

    if explicit_kind {
        let req = Request::RemoveDep {
            ctx: mutation_ctx.mutation_ctx(),
            payload: dep_payload(&edge),
        };
        let ok = send(&req)?;
        return print_ok(&ok, ctx.json);
    }

    let (_, outgoing) = fetch_dep_edges(ctx, &edge.from)?;
    let mut removed = None;
    for existing in outgoing.into_iter().filter(|existing| {
        existing.to == edge.to.id().as_str()
            && existing.to_namespace == edge.to.namespace().as_str()
    }) {
        let kind = DepKind::parse(&existing.kind)
            .map_err(|err| validation_error("dep", err.to_string()))?;
        let req = Request::RemoveDep {
            ctx: mutation_ctx.mutation_ctx(),
            payload: DepPayload {
                from_namespace: Some(edge.from.namespace().clone()),
                from: edge.from.id().clone(),
                to_namespace: Some(edge.to.namespace().clone()),
                to: edge.to.id().clone(),
                kind,
            },
        };
        removed = Some(send(&req)?);
    }

    if let Some(ok) = removed {
        print_ok(&ok, ctx.json)
    } else {
        let req = Request::RemoveDep {
            ctx: mutation_ctx.mutation_ctx(),
            payload: dep_payload(&edge),
        };
        let ok = send(&req)?;
        print_ok(&ok, ctx.json)
    }
}

fn handle_dep_list(ctx: &CliRuntimeCtx, args: DepListArgs) -> CommandResult<()> {
    let ids = args
        .ids
        .into_iter()
        .map(|raw| parse_bead_ref_arg(ctx, "id", &raw))
        .collect::<CommandResult<Vec<_>>>()?;
    let kind_filter = args.kind.map(|kind| kind.as_str().to_string());

    if ctx.json && ids.len() > 1 && args.direction == DepDirection::Down {
        let mut records = Vec::new();
        for id in &ids {
            let (_, outgoing) = fetch_dep_edges(ctx, id)?;
            for edge in outgoing {
                if kind_matches(kind_filter.as_deref(), &edge.kind) {
                    records.push(dep_record_json_value(&edge));
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
        let target_refs = all_edges
            .iter()
            .map(|edge| dep_edge_target_ref(edge, args.direction))
            .collect::<CommandResult<Vec<_>>>()?;
        let summaries = fetch_summary_map(ctx, target_refs)?;
        let mut out = Vec::new();
        for edge in all_edges {
            let target = dep_edge_target_ref(&edge, args.direction)?;
            if let Some(summary) = summaries.get(&target) {
                out.push(dependency_summary_json_value_for_edge(summary, &edge));
            } else {
                out.push(dep_record_json_value(&edge));
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
    bead_ref: &BeadRef,
) -> CommandResult<(Vec<DepEdge>, Vec<DepEdge>)> {
    let read_ctx = ctx.with_namespace(bead_ref.namespace().clone());
    let req = Request::Deps {
        ctx: read_ctx.read_ctx(),
        payload: IdPayload {
            id: bead_ref.id().clone(),
        },
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
    refs: Vec<BeadRef>,
) -> CommandResult<HashMap<BeadRef, beads_api::IssueSummary>> {
    if refs.is_empty() {
        return Ok(HashMap::new());
    }

    let mut by_namespace: BTreeMap<NamespaceId, BTreeSet<BeadId>> = BTreeMap::new();
    for bead_ref in refs {
        let (namespace, id) = bead_ref.into_parts();
        by_namespace.entry(namespace).or_default().insert(id);
    }

    let mut out = HashMap::new();
    for (namespace, ids) in by_namespace {
        let read_ctx = ctx.with_namespace(namespace.clone());
        let req = Request::List {
            ctx: read_ctx.read_ctx(),
            payload: ListPayload {
                filters: Filters {
                    ids: Some(ids.into_iter().collect()),
                    ..Filters::default()
                },
            },
        };
        match send(&req)? {
            ResponsePayload::Query(beads_api::QueryResult::Issues(issues)) => {
                for issue in issues {
                    let id = BeadId::parse(&issue.id)
                        .map_err(|err| validation_error("id", err.to_string()))?;
                    out.insert(BeadRef::new(issue.namespace.clone(), id), issue);
                }
            }
            other => {
                return Err(beads_surface::ipc::IpcError::DaemonUnavailable(format!(
                    "unexpected response for dependency summaries: {other:?}"
                ))
                .into());
            }
        }
    }
    Ok(out)
}

fn dep_edge_target_ref(edge: &DepEdge, direction: DepDirection) -> CommandResult<BeadRef> {
    let (namespace, id) = match direction {
        DepDirection::Down => (&edge.to_namespace, &edge.to),
        DepDirection::Up => (&edge.from_namespace, &edge.from),
    };
    Ok(BeadRef::new(
        NamespaceId::parse(namespace)
            .map_err(|err| validation_error("namespace", err.to_string()))?,
        BeadId::parse(id).map_err(|err| validation_error("id", err.to_string()))?,
    ))
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

pub fn render_dep_tree(root: &BeadRef, edges: &[DepEdge]) -> String {
    let force_namespace =
        root.namespace() != &NamespaceId::core() || dep_edges_need_namespace(edges.iter());
    let root = fmt_dep_endpoint(
        root.namespace().as_str(),
        root.id().as_str(),
        force_namespace,
    );
    if edges.is_empty() {
        return format!("\n{root} has no dependencies\n");
    }
    let mut out = format!("\n🌲 Dependency tree for {root}:\n\n");
    for e in edges {
        let from = fmt_dep_endpoint(&e.from_namespace, &e.from, force_namespace);
        let to = fmt_dep_endpoint(&e.to_namespace, &e.to, force_namespace);
        out.push_str(&format!("{from} → {to} ({})\n", e.kind));
    }
    out.push('\n');
    out
}

pub fn render_deps(incoming: &[DepEdge], outgoing: &[DepEdge]) -> String {
    let force_namespace = dep_edges_need_namespace(incoming.iter().chain(outgoing.iter()));
    let mut out = String::new();
    if !outgoing.is_empty() {
        out.push_str(&format!("\nDepends on ({}):\n", outgoing.len()));
        for e in outgoing {
            let to = fmt_dep_endpoint(&e.to_namespace, &e.to, force_namespace);
            out.push_str(&format!("  → {to} ({})\n", e.kind));
        }
    }
    if !incoming.is_empty() {
        out.push_str(&format!("\nBlocks ({}):\n", incoming.len()));
        for e in incoming {
            let from = fmt_dep_endpoint(&e.from_namespace, &e.from, force_namespace);
            out.push_str(&format!("  ← {from} ({})\n", e.kind));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::CommandError;
    use crate::validation::ValidationError;
    use beads_core::NamespaceId;
    use std::path::PathBuf;

    fn ctx(namespace: Option<NamespaceId>) -> CliRuntimeCtx {
        CliRuntimeCtx {
            repo: PathBuf::from("/tmp/beads"),
            json: false,
            namespace,
            durability: None,
            client_request_id: None,
            require_min_seen: None,
            wait_timeout_ms: None,
            actor_id: None,
        }
    }

    fn edge(from_namespace: &str, from: &str, to_namespace: &str, to: &str) -> DepEdge {
        DepEdge {
            from_namespace: from_namespace.to_string(),
            from: from.to_string(),
            to_namespace: to_namespace.to_string(),
            to: to.to_string(),
            kind: "blocks".to_string(),
        }
    }

    #[test]
    fn render_deps_omits_core_namespace_for_all_core_edges() {
        let output = render_deps(&[], &[edge("core", "bd-a", "core", "bd-b")]);

        assert_eq!(output, "\nDepends on (1):\n  → bd-b (blocks)");
    }

    #[test]
    fn render_deps_qualifies_refs_for_mixed_namespace_edges() {
        let output = render_deps(
            &[edge("sessions", "bd-session", "core", "bd-core")],
            &[edge("core", "bd-core", "extmsg", "bd-ext")],
        );

        assert!(output.contains("→ extmsg/bd-ext (blocks)"));
        assert!(output.contains("← sessions/bd-session (blocks)"));
    }

    #[test]
    fn render_dep_tree_qualifies_root_and_edges_for_non_core_output() {
        let output = render_dep_tree(
            &BeadRef::new(
                NamespaceId::parse("sessions").unwrap(),
                BeadId::parse("bd-session").unwrap(),
            ),
            &[edge("sessions", "bd-session", "core", "bd-core")],
        );

        assert!(output.contains("Dependency tree for sessions/bd-session"));
        assert!(output.contains("sessions/bd-session → core/bd-core (blocks)"));
    }

    #[test]
    fn render_dep_tree_omits_core_namespace_for_core_root() {
        let output = render_dep_tree(
            &BeadRef::new(NamespaceId::core(), BeadId::parse("bd-core").unwrap()),
            &[edge("core", "bd-core", "core", "bd-dep")],
        );

        assert!(output.contains("Dependency tree for bd-core"));
        assert!(output.contains("bd-core → bd-dep (blocks)"));
        assert!(!output.contains("core/bd-core"));
    }

    #[test]
    fn parse_dep_ref_edge_defaults_bare_refs_to_active_namespace() {
        let sessions = NamespaceId::parse("sessions").unwrap();
        let edge = parse_dep_ref_edge(
            &ctx(Some(sessions.clone())),
            None,
            "beads-rs-a",
            "beads-rs-b",
        )
        .unwrap();

        assert_eq!(edge.from.namespace(), &sessions);
        assert_eq!(edge.to.namespace(), &sessions);
        assert_eq!(edge.kind, DepKind::Blocks);
    }

    #[test]
    fn parse_dep_ref_edge_accepts_namespaced_refs_and_kind_prefix() {
        let edge = parse_dep_ref_edge(
            &ctx(None),
            None,
            "related:sessions/beads-rs-a",
            "core/beads-rs-b",
        )
        .unwrap();

        assert_eq!(edge.from.namespace().as_str(), "sessions");
        assert_eq!(edge.from.id().as_str(), "beads-rs-a");
        assert_eq!(edge.to.namespace(), &NamespaceId::core());
        assert_eq!(edge.to.id().as_str(), "beads-rs-b");
        assert_eq!(edge.kind, DepKind::Related);
    }

    #[test]
    fn parse_dep_ref_edge_distinguishes_bad_namespace_and_bad_id() {
        let err =
            parse_dep_ref_edge(&ctx(None), None, "1bad/beads-rs-a", "core/beads-rs-b").unwrap_err();
        assert!(matches!(
            err,
            CommandError::Validation(ValidationError::Field { field, .. })
                if field == "from.namespace"
        ));

        let err = parse_dep_ref_edge(&ctx(None), None, "core/not", "core/beads-rs-b").unwrap_err();
        assert!(matches!(
            err,
            CommandError::Validation(ValidationError::Field { field, .. })
                if field == "from.id"
        ));
    }
}
