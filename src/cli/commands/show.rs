use std::collections::{BTreeSet, HashMap};

use clap::Args;

use super::super::render;
use super::super::{Ctx, normalize_bead_id, print_ok, send};
use crate::Result;
use crate::api::IssueSummary;
use crate::api::QueryResult;
use crate::core::BeadId;
use crate::daemon::Filters;
use crate::daemon::ipc::{Request, ResponsePayload};

#[derive(Args, Debug)]
pub struct ShowArgs {
    pub id: String,

    /// No-op for compatibility (children are always shown).
    #[arg(long, hide = true)]
    pub children: bool,
}

pub(crate) fn handle(ctx: &Ctx, args: ShowArgs) -> Result<()> {
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Show {
        repo: ctx.repo.clone(),
        id: id.as_str().to_string(),
        read: ctx.read_consistency(),
    };
    let ok = send(&req)?;

    match ok {
        ResponsePayload::Query(QueryResult::Issue(mut view)) => {
            // Fetch deps for richer show output.
            let deps_payload = send(&Request::Deps {
                repo: ctx.repo.clone(),
                id: view.id.clone(),
                read: ctx.read_consistency(),
            })?;
            let (incoming_edges, outgoing_edges) = match deps_payload {
                ResponsePayload::Query(QueryResult::Deps { incoming, outgoing }) => {
                    (incoming, outgoing)
                }
                _ => (Vec::new(), Vec::new()),
            };

            // For JSON mode, include deps in the response and return early
            if ctx.json {
                view.deps_incoming = incoming_edges;
                view.deps_outgoing = outgoing_edges;
                return print_ok(&ResponsePayload::Query(QueryResult::Issue(view)), true);
            }

            // Human mode: fetch notes and build richer display
            let notes_payload = send(&Request::Notes {
                repo: ctx.repo.clone(),
                id: view.id.clone(),
                read: ctx.read_consistency(),
            })?;
            let notes = match notes_payload {
                ResponsePayload::Query(QueryResult::Notes(n)) => n,
                _ => Vec::new(),
            };

            // Collect IDs for outgoing deps
            let outgoing_ids: BTreeSet<String> =
                outgoing_edges.iter().map(|e| e.to.clone()).collect();

            // Categorize incoming deps by kind
            let mut blocks_ids: BTreeSet<String> = BTreeSet::new();
            let mut children_ids: BTreeSet<String> = BTreeSet::new();
            let mut related_ids: BTreeSet<String> = BTreeSet::new();
            let mut discovered_ids: BTreeSet<String> = BTreeSet::new();
            for e in &incoming_edges {
                match e.kind.as_str() {
                    "parent" => {
                        children_ids.insert(e.from.clone());
                    }
                    "related" => {
                        related_ids.insert(e.from.clone());
                    }
                    "discovered_from" => {
                        discovered_ids.insert(e.from.clone());
                    }
                    _ => {
                        blocks_ids.insert(e.from.clone());
                    }
                }
            }

            let mut all_ids = BTreeSet::new();
            all_ids.extend(outgoing_ids.iter().cloned());
            all_ids.extend(blocks_ids.iter().cloned());
            all_ids.extend(children_ids.iter().cloned());
            all_ids.extend(related_ids.iter().cloned());
            all_ids.extend(discovered_ids.iter().cloned());
            let summary_map = fetch_summary_map(ctx, &all_ids)?;

            let outgoing_views = summaries_for(&outgoing_ids, &summary_map);
            let blocks = summaries_for(&blocks_ids, &summary_map);
            let children = summaries_for(&children_ids, &summary_map);
            let related = summaries_for(&related_ids, &summary_map);
            let discovered = summaries_for(&discovered_ids, &summary_map);

            let incoming = render::IncomingGroups {
                children,
                blocks,
                related,
                discovered,
            };

            println!(
                "{}",
                render::render_show(&view, &outgoing_views, &incoming, &notes)
            );
            Ok(())
        }
        other => print_ok(&other, false),
    }
}

fn fetch_summary_map(ctx: &Ctx, ids: &BTreeSet<String>) -> Result<HashMap<String, IssueSummary>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let bead_ids = ids
        .iter()
        .map(|id| BeadId::parse(id))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let filters = Filters {
        ids: Some(bead_ids),
        ..Filters::default()
    };
    let req = Request::List {
        repo: ctx.repo.clone(),
        filters,
        read: ctx.read_consistency(),
    };
    match send(&req)? {
        ResponsePayload::Query(QueryResult::Issues(summaries)) => Ok(summaries
            .into_iter()
            .map(|summary| (summary.id.clone(), summary))
            .collect()),
        _ => Ok(HashMap::new()),
    }
}

fn summaries_for(
    ids: &BTreeSet<String>,
    summaries: &HashMap<String, IssueSummary>,
) -> Vec<IssueSummary> {
    ids.iter()
        .filter_map(|id| summaries.get(id).cloned())
        .collect()
}
