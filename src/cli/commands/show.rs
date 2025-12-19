use super::super::render;
use super::super::{Ctx, ShowArgs, fetch_issue_summary, normalize_bead_id, print_ok, send};
use crate::Result;
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::query::QueryResult;

pub(crate) fn handle(ctx: &Ctx, args: ShowArgs) -> Result<()> {
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Show {
        repo: ctx.repo.clone(),
        id,
    };
    let ok = send(&req)?;
    if ctx.json {
        return print_ok(&ok, true);
    }

    match ok {
        ResponsePayload::Query(QueryResult::Issue(view)) => {
            // Fetch deps + notes for richer show output.
            let deps_payload = send(&Request::Deps {
                repo: ctx.repo.clone(),
                id: view.id.clone(),
            })?;
            let (incoming_edges, outgoing_edges) = match deps_payload {
                ResponsePayload::Query(QueryResult::Deps { incoming, outgoing }) => {
                    (incoming, outgoing)
                }
                _ => (Vec::new(), Vec::new()),
            };

            let notes_payload = send(&Request::Notes {
                repo: ctx.repo.clone(),
                id: view.id.clone(),
            })?;
            let notes = match notes_payload {
                ResponsePayload::Query(QueryResult::Notes(n)) => n,
                _ => Vec::new(),
            };

            use std::collections::BTreeSet;

            let mut outgoing_ids: BTreeSet<String> = BTreeSet::new();
            for e in &outgoing_edges {
                outgoing_ids.insert(e.to.clone());
            }
            let mut outgoing_views = Vec::new();
            for id in outgoing_ids {
                if let Ok(v) = fetch_issue_summary(ctx, &id) {
                    outgoing_views.push(v);
                }
            }

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

            let mut blocks = Vec::new();
            for id in blocks_ids {
                if let Ok(v) = fetch_issue_summary(ctx, &id) {
                    blocks.push(v);
                }
            }
            let mut children = Vec::new();
            for id in children_ids {
                if let Ok(v) = fetch_issue_summary(ctx, &id) {
                    children.push(v);
                }
            }
            let mut related = Vec::new();
            for id in related_ids {
                if let Ok(v) = fetch_issue_summary(ctx, &id) {
                    related.push(v);
                }
            }
            let mut discovered = Vec::new();
            for id in discovered_ids {
                if let Ok(v) = fetch_issue_summary(ctx, &id) {
                    discovered.push(v);
                }
            }

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
