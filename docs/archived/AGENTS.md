## Boundary
This directory is reserved for superseded docs kept for historical context only.
NEVER: treat files here as the source of truth for current architecture, workflow, verification, or surface design.

## Routing
- Current workflow and verification policy live in `../../AGENTS.md`.
- Current crate ownership and dependency law live in `../CRATE_DAG.md`.
- Current IPC/wire behavior lives in `../IPC_PROTOCOL.md`.
- Current AGENTS hierarchy guidance lives in `../AGENTSMD_INFO.md`.
- Current durable design guidance lives in `../philosophy/`.
- If archived workflow/spec docs land here, treat them as the highest-risk bait in this subtree because they can still read like live operational guidance. Treat them as superseded history unless you deliberately re-verify every claim against current docs and code.
- Use this subtree for historical questions only:
  - origin history
  - superseded requirements
  - migration archaeology
- If a file here still matters operationally, either restore that truth as a maintained canonical doc elsewhere or cite the archived file explicitly as historical context and verify it against current code/current docs before acting.

## Local rules
- Do not update archived docs to describe current behavior; either move the truth into a canonical doc or leave the archived file as a record of the old model.
- Treat commands, flags, workflow steps, and requirements in archived files as suspect until re-verified against current code and current canonical docs.
- When you cite an archived file, label it as historical and preserve its original date/scope instead of paraphrasing it into present-tense law.
- If you move another stale top-level doc here, also update any live references that still advertise it as current guidance.
