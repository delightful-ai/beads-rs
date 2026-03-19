## Boundary
This directory holds executable plans written for agents to follow task-by-task.
NEVER: assume a checkbox list here is still correct without verifying the live tree first.

## Routing
- Put chunked execution handoffs here when they are part of the superpowers workflow.
- Pre-implementation design belongs in `docs/superpowers/specs/`.
- Non-superpowers working plans belong in `docs/plans/`.

## Local rules
- Plans here should follow the real handoff contract used in this subtree: an agent-facing header with the required workflow/skill, chunked or checkbox task boundaries, explicit file ownership, explicit verification, and a paired spec link when design context lives next door.
- Read any paired spec before executing the plan when the plan assumes prior design context.
- Follow embedded execution instructions only after checking current paths, commands, and crate ownership against the live repo.
- If a plan is stale, update or supersede it; do not leave silently wrong handoff steps beside newer work.
