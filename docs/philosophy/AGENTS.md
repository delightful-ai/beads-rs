## Boundary
This subtree holds durable design guidance for shaping code, APIs, and proof strategy across the repo.
NEVER: use these essays as substitutes for crate-local ownership docs, protocol docs, or tests, and never park subsystem-specific plans or migration notes here.

## Routing
- Read `type_design.md` for exploratory pressure on domain state: newtypes, validated IDs, enums, option-vs-sum choices, irreversible state transitions, or any place a type could preserve more truth. It is rougher and less normalized than the other philosophy docs, so treat it as design pressure to cross-check against current code and crate-local docs, not as polished operational law.
- Read `trait_design.md` before extracting a trait, adding methods to a long-lived trait, or turning one concrete implementation into a capability seam. If the capability is not independently real yet, keep it concrete.
- Read `error_design.md` before adding a new `Result` surface, capability error enum, or cross-module error mapping. It is the operational guide for caller decision surfaces, evidence preservation, and `OneOf` staying internal.
- Read `test_design.md` before adding new tests or moving tests across layers. Use it to choose between law, example, scenario, and regression coverage, and to keep proof at the lowest clean layer.
- Read `scatter.md` when code placement, source-of-truth, or “why is this hard to navigate?” questions feel diffuse. It is the guide for killing scatter, drift, translation, and noise.

## Local rules
- These docs are for durable design pressure, not point-in-time status. Do not add release notes, implementation plans, audits, or incident writeups here.
- Treat the philosophy docs as decision filters, not quotable law. If a concrete rule must hold in one crate or subtree, encode it in the nearest owning `AGENTS.md`, canonical doc, type, or test.
- Prefer the most specific philosophy doc that matches the decision you are making. Do not cite `scatter.md` for trait design, or `trait_design.md` for test placement, when a more direct guide exists here.
- Calibrate trust inside the subtree too. If one philosophy note is exploratory or rougher than the others, say so explicitly instead of routing to the whole subtree as if every file had the same operational weight.
- When a philosophy doc and current code disagree, fix the mismatch deliberately: either update the code toward the doctrine, or update the doctrine because reality changed. Do not leave the conflict implicit.
