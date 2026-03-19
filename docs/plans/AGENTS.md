## Boundary
This directory holds dated working plans, not evergreen reference docs.
NEVER: treat a file here as current policy without checking current code and maintained docs.

## Routing
- New repo working plans and migration plans go here unless they are explicitly part of the superpowers spec/plan workflow.
- `docs/superpowers/plans/` is the preferred home for new superpowers-style execution handoffs, but this directory is still mixed: some older files are executable worker handoffs, some are design records, and some are product-direction notes.
- Durable philosophy, crate-boundary, protocol, or workflow docs belong outside `plans/`.

## Local rules
- Prefer dated filenames for new docs here. Existing undated files are legacy exceptions, not the naming pattern to copy.
- State what kind of plan the file is before acting on it: executable handoff, implementation/migration plan, design note, or product-direction note.
- Older files here may contain assistant-specific execution instructions or required-skill headers. Treat those as file-local contracts to re-verify, not as proof that the whole directory is executable by default.
- If a plan becomes the enduring contract, promote that contract into maintained docs and leave the plan as dated history.
- If you touch an older plan-shaped design doc, either keep its date and scope explicit or re-home it to the durable location it now belongs in.
