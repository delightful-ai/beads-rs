use super::super::render;
use super::super::{Ctx, ShowArgs, fetch_issue_summaries, normalize_bead_id, print_ok, send};
use crate::Result;
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::query::QueryResult;

pub(crate) fn handle(ctx: &Ctx, args: ShowArgs) -> Result<()> {
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Show {
        repo: ctx.repo.clone(),
        id,
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

            use std::collections::BTreeSet;

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

            // Batch fetch all summaries in a few calls instead of N individual requests
            let outgoing_views =
                fetch_issue_summaries(ctx, outgoing_ids.into_iter().collect())?;
            let blocks = fetch_issue_summaries(ctx, blocks_ids.into_iter().collect())?;
            let children = fetch_issue_summaries(ctx, children_ids.into_iter().collect())?;
            let related = fetch_issue_summaries(ctx, related_ids.into_iter().collect())?;
            let discovered = fetch_issue_summaries(ctx, discovered_ids.into_iter().collect())?;

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
