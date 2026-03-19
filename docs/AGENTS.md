## Boundary
This subtree holds maintained reference docs, durable design doctrine, and dated working records for the repo.
NEVER: treat every file under `docs/` as equally canonical, or treat a dated plan/audit as current law without checking whether its truth was promoted elsewhere.

## Routing
- `AGENTSMD_INFO.md` is the local rubric for authoring and reviewing `AGENTS.md` files. Read it before changing any file in an `AGENTS.md` stack.
- `CRATE_DAG.md` is the canonical crate-boundary and ownership reference. Use it when routing code/tests across crates or checking forbidden dependency edges.
- `IPC_PROTOCOL.md` is the maintained IPC wire contract. Read it before changing request/response framing, admin op tagging, or protocol versioning.
- `CRDT_AUDIT.md` is a dated audit of CRDT semantics and convergence risks. Read it when changing merge/apply behavior or replay/sync correctness, but confirm its findings against current code/tests before treating them as live policy.
- `observability_schema.md` is the canonical field-key schema for daemon spans/logs. Update it when telemetry field names or required keys change.
- `PERF_HOTPATHS.md` is the benchmark/profiling playbook plus performance receipts. Use it when touching hot paths or test-suite throughput, but treat embedded run artifacts as dated evidence, not permanent policy.
- `philosophy/` holds durable design guidance. Read the child `AGENTS.md` there to pick the right philosophy doc for type, trait, error, test, or scatter decisions; not every essay in that subtree is equally polished or equally operational.
- `architecture/` holds dated inventories, migration matrices, and closeout notes. Use these as evidence for what was audited or decided at a specific time, or to understand why a boundary changed; do not treat them as the standing source of truth when a canonical reference already exists.
- `plans/` holds dated repo working plans, migration plans, and a few older plan-shaped design docs. Read the child `AGENTS.md` there before treating a file as an execution handoff versus a historical design record.
- `superpowers/` holds superpowers workflow artifacts. Read the child `AGENTS.md` there before using anything in that subtree as input to execution.
- `archived/` is historical context only. If a file there still matters for current behavior, restore the truth into a maintained doc elsewhere instead of updating the archive to describe today.

## Local rules
- Prefer one maintained doc per live concept. If current truth already has a canonical home, update it instead of adding another top-level note, audit, or closeout doc beside it.
- Promote durable truth out of dated docs. If a plan, closeout, or audit establishes policy that still governs the repo, move that policy into the maintained reference doc and leave the dated file as history.
- Updating only a dated plan or spec is never enough when shipped behavior, ownership, or workflow changed; update the maintained reference doc in the same change.
- Keep status explicit when adding docs. Durable references belong at stable paths; dated design/planning material belongs under `plans/` or `superpowers/`; superseded material belongs under `archived/`.
- When you cite a dated doc from `architecture/`, `plans/`, or `superpowers/specs/`, carry its date/scope forward in your prose and re-check the claim against the current canonical doc or current code before treating it as live guidance.
- Philosophy docs shape design decisions; they do not override crate-local ownership, protocol docs, or executable proof. When a philosophy principle becomes binding for one subsystem, encode it in the owning code/tests/docs there.
- Re-check every referenced path and every command you mention after editing. Dead links and stale verification lines make this subtree actively unsafe.
