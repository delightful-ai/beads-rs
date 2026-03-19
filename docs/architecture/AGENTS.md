## Boundary
This directory holds dated architecture evidence: inventories, migration matrices, and closeout records from specific cleanup passes.
NEVER: treat this subtree as the primary home for live repo policy.

## Routing
- Use this subtree when you need dated evidence:
  - what was audited at a specific time
  - which internal symbols or couplings a cleanup pass mapped
  - what a migration or closeout claimed to leave behind
- Current boundary law lives elsewhere:
  - `../CRATE_DAG.md` for crate ownership and forbidden edges
  - `../IPC_PROTOCOL.md` for the current IPC contract
  - `../philosophy/` for durable design law
  - the owning code and nearest non-archival `AGENTS.md` files for current placement and proof loops
- If a statement from this subtree must constrain current implementation going forward, promote that truth into a maintained canonical doc and leave the dated record here as supporting evidence.

## Local rules
- Keep whatever provenance the file already has explicit, and add date/scope metadata on new records. Existing files are not perfectly uniform, so do not infer missing metadata from directory pattern alone.
- Do not silently "refresh" an old inventory or closeout to describe present-day reality. Either write a new dated document or update the canonical reference that now owns the rule.
- New files here should be obviously dated records such as inventories, closeouts, or migration matrices. Generic evergreen architecture law belongs in a maintained canonical doc, not here.
- When citing a file from this subtree in reviews, plans, or follow-up docs, say that it is evidence "as of" that document's date/scope so strangers do not mistake it for timeless policy.
