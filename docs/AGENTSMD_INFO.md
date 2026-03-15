yeah — grokked. and yes, this applies to root and every child `agents.md`, not just root.

the big correction is this:

there is no optimal universal static section list.

there **is** an optimal universal contract.

a god-tier `agents.md` is a location-aware, stackable context patch that gives an agent peripheral vision at a particular point in the repo. it is not just “commands + tips.” it is the local map, the local law, the local pattern language, the local hazard atlas, and the local proof strategy.

the actual bar is:

> given only the relevant code plus the stacked `agents.md` files from root to the working directory, a smart stranger should be able to make the right change in the right place, in the house style, without copying legacy nonsense, and with a cheap way to prove they didn’t break the wrong thing.

that’s the spec in one sentence.

## 1. what an `agents.md` actually is

it can be all of these at once:

* a map of the territory
* a routing table for where new code should go
* a boundary contract for what does **not** belong here
* a pattern index for what to copy
* a hazard register for what not to copy
* a verification graph for how to check work
* an exception ledger for where local rules diverge
* a maintenance contract for how it stays true over time

if a file is only a command catalog, it’s underpowered.
if it’s only philosophy, it’s decorative.
if it’s only local trivia, it’s myopic.

the sweet spot is: **it changes decisions**.

## 2. the universal spec is semantic, not sectional

fixed headings are the wrong abstraction.

different repos, and different layers inside the same repo, need different renderings. what should be universal is the set of **semantic obligations**.

for any location in a repo, the stacked `agents.md` context should answer these questions:

1. where am i, conceptually?
2. what belongs here?
3. what definitely does not belong here?
4. what invariants govern this area?
5. what local patterns are canonical?
6. what nearby code is misleading legacy bait?
7. what should i run to verify a change here?
8. what is special about this layer vs its parent and siblings?
9. when should this file itself be updated?

those are the universal slots. the section names can vary. the presence of those answers cannot.

## 3. the hierarchy law: highest stable truth, not highest possible truth

your instinct is close, but the naive version is slightly cursed.

not “put every fact at the highest layer where it can be stated.”

instead:

> put a rule at the highest layer where it is simultaneously true, actionable, and stable.

that three-part test matters.

a rule belongs higher only if:

* it is true for that whole subtree
* it changes decisions at that level
* it won’t churn constantly because of local implementation details

that avoids a bunch of fake elegance.

examples:

* “routes are thin; business logic lives below the transport layer” is a high-level rule.
* “for this webhook route, validate signature before deserializing provider-specific payloads” is not root material, even though it’s an instance of the global rule.
* “run `cargo test`” at every leaf is noise unless that leaf adds something materially more specific.

so the real heuristic is:

* global truths live high
* local tactics live low
* volatile details live low
* shared sibling truth gets promoted
* sibling-specific truth gets pushed down

call this the **truth altitude test**.

## 4. what each location in the tree is for

this is where people usually fail the spec. they treat every `agents.md` as the same kind of file. it isn’t.

every `agents.md` should declare its role, implicitly or explicitly.

### root

root is not “where the commands live.” that would be a profoundly mid reading.

root is the repo constitution and the wide-angle map. it should encode:

* the repo’s worldview
* top-level topology: what the major directories are for
* concept-to-location routing: “if you’re looking for x, start in y”
* cross-cutting architectural boundaries
* global invariants and bans
* migration status / source-of-truth statements
* default verification tiers
* how child `agents.md` files are supposed to refine root

root should answer: “how does this repo think?”

### subsystem / domain root

this is domain doctrine.

it should encode:

* what this subsystem owns
* what sibling subsystems own instead
* domain-specific invariants
* common local task families
* shared exemplars for the subtree
* domain-level test and debug strategy
* domain hazards and known bad precedent

it should answer: “how does this part of the repo interpret the global law?”

### seam / boundary directory

this is for places where systems meet: api edges, adapters, persistence boundaries, ingestion, translation layers, etc.

it should encode:

* ingress/egress contracts
* dependency direction
* allowed leakage and forbidden leakage
* mapping / translation invariants
* failure semantics
* replay / debug loops
* examples of correct boundary handling

it should answer: “what must remain true when information crosses this boundary?”

### leaf / tactical directory

this is the local playbook.

it should encode:

* exact local tasks
* one or two canonical examples
* local gotchas
* local verification deltas
* anti-patterns nearby that will tempt naive copying

it should answer: “how do i safely make the common change here?”

### tests directory

this is a different beast. do not treat it like app code.

it should encode:

* what kinds of tests live here
* what they are intended to prove
* fixture / harness rules
* determinism rules
* naming / selection patterns
* what not to test here because it belongs elsewhere

it should answer: “what is the proof model in this zone?”

### tooling / scripts / ops

it should encode:

* side effects
* dry-run expectations
* required env / credentials
* idempotence expectations
* rollback or recovery path
* what is safe to run locally vs only in controlled contexts

it should answer: “what can this script do to the world?”

### legacy / quarantine

this one is hugely useful and almost never written well.

it should encode:

* touch policy
* what edits are allowed
* what expansions are forbidden
* modernization direction
* where new work should go instead

it should answer: “how do i survive touching this without making the future worse?”

### generated / vendor / third-party

it should mostly say:

* do not hand-edit
* source of generation
* regeneration command
* what sanity check to run after regeneration

absence of a child `agents.md` is also meaningful: it means the parent context is sufficient.

## 5. the core unit should be a decision card, not a paragraph

this is the part i’d make universal.

the most useful atom inside an `agents.md` is not “a section.” it’s a **decision card**.

a decision card answers:

* when does this apply?
* what should i do?
* what property must i preserve?
* what nearby thing should i avoid copying?
* how do i verify it?

that gives you a shape like this:

```md
- when: adding a new webhook handler
  do: copy `server/routes/vapi/webhooks.rs`
  preserve: thin route, validate at edge, map to canonical event before service call
  avoid: `server/routes/vapi/legacy_handler.rs`
  verify: run `just server-fast` plus `cargo test -p server vapi_roundtrip`
```

that one card is worth more than ten paragraphs of vibes.

same rule everywhere:

* no naked commands
* no naked examples
* no naked prohibitions

every command should say **when** to run it and **what it proves**.
every example should say **why** it is canonical.
every prohibition should say **what to do instead**.

otherwise you get technically complete files that are useless in practice.

## 6. redundancy avoidance: delta, not duplication

this is subtle, and yeah, it’s where naive specs go to die.

the right rule is:

> a child `agents.md` should mostly contain the marginal signal that becomes true because you are now here.

not “repeat the parent in smaller letters.”

three concrete rules:

### a. one truth, one altitude

a fact should live at one primary layer.

if the same thing is true for multiple siblings, promote it.
if it is false for even one sibling, push it down.

### b. child files should be mostly delta

for a leaf file, most non-metadata lines should be genuinely local.
if a leaf is mostly parent restatement, delete it.

a decent target:

* root: no delta target
* subsystem: at least ~50% local-to-subtree signal
* leaf: at least ~70% local-only signal

not mathematically exact, but the smell test is good.

### c. repetition is allowed only when it buys safety

some things may be repeated on purpose because agents overweight nearby text.

that is fine for:

* safety-critical invariants
* frequently violated rules
* high-cost failure modes

but mark it as re-emphasis, not new information.

## 7. verification should be layered, not spammed

you were right to push back on “put `cargo test` everywhere.” that’s fake utility.

the model that actually works is:

### root defines verification policy and tiers

not just commands. policy.

for example:

* what “fast” means
* what “standard” means
* what “full” means
* what kinds of changes should escalate between tiers
* what global checks are expected before merge

### lower layers add verification deltas

they do not restate the whole world.
they add local checks tied to local change types.

so instead of this at a leaf:

* run `cargo test`
* run `cargo clippy`
* run formatting

you want this:

* for mapping changes here, append `cargo test -p ingest stripe_roundtrip`
* for contract changes here, append `just server-contracts`
* for perf-sensitive code here, append the local benchmark or profile command

in other words:

* root owns baseline verification semantics
* children own change-triggered verification deltas
* agents run the inherited baseline plus applicable local deltas, deduped

that’s way less noisy and way more operational.

## 8. how agents should actually consume the hierarchy

this should be part of the spec, bc otherwise people write for humans and agents misread it.

the consumption model should be:

1. load `agents.md` from root to current directory
2. treat higher levels as defaults and lower levels as refinements
3. prefer the most specific matching decision card
4. inherit baseline checks from above
5. append applicable local verification deltas
6. dedupe repeated commands and rules
7. treat explicit exception blocks as stronger than generic parent guidance
8. ignore non-actionable filler

which means authors should write with that in mind:

* nearest text gets overweighted
* concrete file paths get copied
* vague rules get ignored
* repeated commands get treated as important even if they’re just duplication
* nearby legacy code will be copied unless explicitly called out as bait

that last one matters a lot. code teaches by precedent. `agents.md` exists partly to veto bad precedent.

## 9. what makes a child `agents.md` worth creating at all

not every directory deserves one.

a child file should exist only if it adds at least one of these:

* a distinct local boundary
* a distinct invariant
* a recurring local task pattern
* a local hazard / gotcha
* a local verification delta
* a local exception to parent norms
* a local routing function for “new work goes here, not there”

if it can’t do any of that, don’t create it. let inheritance work.

## 10. a protocol for keeping them up to date

treat them like code, not like ornamental docs.

the protocol i’d use:

### on-touch rule

if a change alters any of these, update the nearest relevant `agents.md` in the same pr:

* boundary
* invariant
* canonical pattern
* verification delta
* anti-pattern
* routing guidance
* exception status

### promotion / demotion rule

during review, ask:

* is this true for siblings too? promote it.
* is this only true here? demote it.

### incident capture rule

if a bug, outage, or nasty review comment would have been prevented by a better `agents.md`, add the missing rule at the lowest stable layer that would have stopped it.

### churn review rule

high-churn or high-incident directories should get periodic review. low-churn stable ones can age more quietly.

### ownership rule

every file should have an owner, team, or at least a review affinity. otherwise nobody feels the entropy.

### lint rule

the repo should eventually validate at least:

* referenced paths exist
* referenced commands still parse/run
* duplicated exact text is flagged
* `last_reviewed` is not fossilized
* forbidden sections are not empty placeholders

## 11. the scoring model

here’s a scoring rubric that is actually about utility, not box-ticking.

call it the **context yield score**. 100 points.

### 1. orientation — 15

can an agent tell where it is in the conceptual map?
does the file define the territory, not just the folder name?

### 2. routing power — 15

can an agent tell where new work should go and where it should not go?

### 3. landmarks — 15

are there canonical examples with reasons, not just paths?

### 4. hazards — 10

does it explicitly mark bad precedent, traps, and forbidden moves?

### 5. proof closure — 15

does it provide the cheapest sufficient verification strategy tied to change types?

### 6. zoom fit — 15

is the information at the right layer of abstraction for this directory?

### 7. delta efficiency — 10

is most of the file genuinely local signal rather than inherited duplication?

### 8. freshness — 5

is there a visible mechanism for keeping it true?

penalties:

* `-20` silent contradiction with parent
* `-15` dead command or dead path
* `-10` generic filler that changes no decision
* `-10` naked command / naked example / naked prohibition
* `-10` child file that should not exist because it adds no real delta

score bands:

* `90–100`: god-tier
* `75–89`: strong
* `60–74`: useful but leaky
* `<60`: decorative repo cosplay

## 12. the acceptance tests matter more than the score

before you bless an `agents.md`, run these thought experiments:

### blind placement test

give a contributor a plausible feature. can they tell where it belongs?

### blind refusal test

give them a change that should **not** be made here. can they say no?

### blind imitation test

give them a local task. can they find the right exemplar and preserve the right properties?

### blind verification test

can they choose the right checks without either under-testing or carpet-bombing the whole repo?

### blind exception test

can they tell what is weird or special about this subtree?

if the file passes those, it’s doing real work.

## 13. a practical universal shape

not mandatory headings. just a strong rendering.

```md
---
role: root | subsystem | seam | leaf | tests | tooling | legacy | generated
scope: /path/to/directory
owners: [team-or-person]
last_reviewed: yyyy-mm-dd
change_triggers:
  - boundary changed
  - new recurring task appeared
  - verification flow changed
  - incident exposed missing guidance
---

## purpose / boundary
what this area is for, what it is not for, allowed deps, forbidden leakage

## map / routing
where related concepts live, what siblings own instead

## invariants
truths that must continue to hold here

## decision cards
- when:
  do:
  preserve:
  avoid:
  verify:

## hazards / exceptions
legacy bait, local gotchas, explicit exceptions to parent guidance

## references
deeper docs only when needed
```

for root, expand `map / routing` and `invariants`.
for subsystems, expand `decision cards`.
for leaves, keep it tiny and sharp.
for tests/tooling/legacy/generated, swap in the role-specific semantics.

## 14. the actual universal principle

if i had to compress the whole thing to one governing law, it’d be this:

> every `agents.md` should provide the maximum additional peripheral vision for its location at the minimum possible duplication cost.

that’s the whole game.

or even tighter:

> root gives horizon, children add resolution, leaves add handholds.

that’s the bar.


===

## What an AGENTS.md is

An `AGENTS.md` file is **executable context that makes a limited-visibility contributor behave correctly**.

It's not documentation. It's a **behavioral forcing function** that encodes:
- What this code is and isn't allowed to be
- How to work here without breaking invariants
- Which patterns to copy and which to avoid
- How to verify you didn't screw up

These files form a **hierarchy that mirrors your directory tree**. As an agent (or human) navigates to a specific file, they accumulate context:

```
/AGENTS.md                     → "This is how we think about software in this repo"
/server/AGENTS.md              → "...and this server specifically handles X, never Y"  
/server/routes/AGENTS.md       → "...and routes follow pattern P, verify with command C"
/server/routes/vapi/AGENTS.md  → "...and vapi has this specific gotcha G"
```

Each level **refines** but never **contradicts** the levels above. By the time you're editing `/server/routes/vapi/webhooks.rs`, you have the full gradient from philosophy → domain rules → local tactics.

---

## The AGENTS.md Spec

Each `AGENTS.md` has three moves:

### 1. BOUNDARY
*What this directory IS and ISN'T.*

```markdown
## Boundary
This directory: [single sentence of purpose]
Depends on: [what it's allowed to import/call]
Depended on by: [who can import/call this]  
NEVER: [what must not leak out or happen here]
```

This prevents architectural drift. The agent knows what belongs here vs elsewhere.

### 2. BEHAVIOR  
*How to work here correctly.*

```markdown
## How to work here
Golden example: [path/to/best_example.rs - what to copy]
When adding X: [concrete steps including verification]
Tests that matter: `[exact command that must pass]`
Invariants: [what must always remain true]
```

This is the oral tradition. The agent knows what good looks like and how to check it didn't break things.

### 3. ENTROPY CONTROL
*What not to do, even if you see it in the code.*

```markdown
## Don't copy this
Legacy pattern: [what exists but shouldn't be copied]
Use instead: [the modern pattern]
Known issues: [gotchas that will waste time]
```

This prevents the agent from mimicking the worst parts of your codebase.

---

## Hierarchy rules

1. **Root `/AGENTS.md`** sets universal laws. These are axioms—unbreakable, apply everywhere.
2. **Domain `/{subsystem}/AGENTS.md`** adds domain rules. These specialize but don't contradict root.
3. **Leaf `/{subsystem}/{module}/AGENTS.md`** adds local tactics. Maximum specificity, minimum size.

Each file is **small** (< 100 lines). Dense signal, no essays. Links to deeper docs if needed.

When an agent works in `/foo/bar/baz.rs`, it sees all AGENTS.md files from root to `baz/`, stacked. This gives it the full context gradient without needing to see the whole repo.

---

## Why this works

This spec has stupidly high utility because:

1. **Boundary** stops the agent from putting code in the wrong place (the #1 source of architectural rot)
2. **Behavior** gives the agent a concrete pattern to copy + a way to verify it worked (the Factory thesis)
3. **Entropy Control** stops the agent from propagating the bad parts that already exist
4. **Hierarchy** means you write each rule once at the right level, not 50 times

An agent following this spec will write code that looks like a senior engineer who's been on the team for years wrote it. Not because the agent is smart, but because you've installed your taste directly into its context window at exactly the right level of abstraction for where it's working.

That's what an AGENTS.md is: **a context patch that turns a talented stranger into a trusted teammate**.

===

cool, let's “spec” this thing.

Below is a *general* AGENTS.md spec you can reuse anywhere, built from everything we’ve been iterating on.

---

# AGENTS.md – Informal Spec

## 0. Core definition (one-liner)

An `AGENTS.md` is a **hierarchical, executable guide** for a slice of the repo that encodes:

- What this code **is for** (boundary & responsibilities)
- How we **work here** (patterns, style, workflows)
- How we **verify** we didn’t break anything (commands/tests/invariants)
- What we **never do** here (negative space / anti‑patterns)

Stacked across the directory tree, these files give an agent (or human) enough *worldview + tactics* to behave like a long‑tenured contributor, despite only seeing a tiny local window of code.

---

## 1. Hierarchy semantics

**1.1 Where they live**

- Any directory MAY contain an `AGENTS.md`.
- The root of the repo SHOULD contain one.
- Subtrees that define clear domains (e.g., `server/`, `core/`, `ingest/`, `tests/`) SHOULD contain one at their root.

**1.2 How they stack**

When working on a file at path:

```text
/path/to/repo/a/b/c/file.ext
```

the agent loads, in order:

1. `/AGENTS.md`              (root)
2. `/a/AGENTS.md`           
3. `/a/b/AGENTS.md`
4. `/a/b/c/AGENTS.md`

These are conceptually concatenated (or merged) into a single “context patch.”  
Deeper levels **refine / specialize**, but should not silently **contradict** higher ones.

**1.3 Precedence**

- Root defines **global axioms** (non‑negotiable truths).
- Subdirectories define **domain doctrine** (how those axioms apply in this area).
- Leaf directories define **local tactics / gotchas / workflows**.

If there is a real conflict, higher level wins, and the lower‑level file should eventually be edited to remove the conflict.

---

## 2. High-level responsibilities of an AGENTS.md

Each file should:

1. **Define the boundary** – what this directory is for, what depends on it, and what it depends on.
2. **Install a worldview** – the local taste, principles, and heuristics that drive decisions here.
3. **Encode verifiability** – how to cheaply check that code changes here are correct.
4. **Prescribe patterns** – how to structure code/tests/config in this subtree.
5. **Constrain entropy** – what must never be done or extended here, even if older code does it.
6. **Point to deeper context** – links to design/type/architecture docs that matter for this area.
7. **Stay small and operational** – high signal, no essays; deep philosophy lives elsewhere.

---

## 3. Recommended section layout

You don’t have to literally name them this way, but conceptually every AGENTS.md should cover:

1. `## Boundary` – what lives here and the dependency rules
2. `## How to work here` – patterns, code style, and workflows
3. `## Verification` – commands/tests you *must* run and what they guarantee
4. `## Don’t do this` – legacy patterns and forbidden moves
5. `## Gotchas` – traps, caveats, perf issues, non‑obvious stuff
6. `## References` – links to deeper docs

Root and domain levels will be heavier; leaf levels can be much lighter.

---

## 4. Section semantics (detailed)

### 4.1 `## Boundary`

**Goal:** make the architectural role of this directory explicit.

Include:

- **Purpose (1–2 sentences)**
  - “This directory implements X and nothing else.”
  - Avoid vague crap; name the domain and responsibility.

- **Dependencies (allowed imports/calls)**
  - “May depend on: `core/types`, stdlib.”
  - “Must NOT depend on: HTTP, DB, external services.”

- **Dependents (who can call/use this)**
  - “Used by: `server/routes`, `cli/`.”
  - This is useful both for humans and for automated refactor tools.

- **Non‑leak invariants**
  - “Provider‑specific schemas must not appear outside this folder.”
  - “Database types must not escape this layer.”
  - “No business logic here; this is mapping/transport only.”

This is your **boundary contract**. It answers: “What belongs here vs elsewhere?”

---

### 4.2 `## How to work here`

This is the main “oral tradition” section. It’s what you wish a competent stranger knew before touching anything.

Sub‑pieces:

#### 4.2.1 Golden patterns

- **Point to canonical files**:
  - “For a canonical handler pattern, see `foo/bar.rs`.”
  - “For a clean property-based test, see `tests/roundtrip.rs`.”

- Optionally show a **tiny code snippet** (inline) if the pattern is non‑obvious.

The goal is: when the agent needs to add X, it knows *exactly what to imitate*.

#### 4.2.2 Code style & structure (for this area)

High‑signal conventions that *actually matter* here:

- **Language-level style**
  - “Prefer small, pure functions; avoid 200‑line methods.”
  - “Don’t use `unwrap`/`expect` in production code here.”
  - “Explicit enums over stringly‑typed variants.”

- **Error handling patterns**
  - “All errors go through `Error` in `errors.rs` (use `thiserror`).”
  - “Bubble errors up as `Result<T, MyError>`, never `anyhow::Error` in this module.”

- **Data modeling patterns**
  - “Lifecycle is represented via typestates `Foo<Draft>`/`Foo<Final>`; don’t add boolean flags.”
  - “Keep raw provider payloads alongside canonical types (no silent lossy transforms).”

- **Local architectural style**
  - “Routes are thin; they call into services in `services/`, which call into repositories in `repo/`.”
  - “Config is read at the edge and passed in; don’t reach for globals.”

- **Formatting / linting specifics if they impact semantics**
  - “We use `rustfmt` defaults; don’t fight it.”
  - “Run `ruff` with config X; new code should follow that style automatically.”

Do **not** re-explain what the linter already enforces unless it’s subtle or controversial. Focus on *taste that the tools can’t easily enforce*.

#### 4.2.3 Local workflows

- **How you actually work here when making changes**
  - “To add a new provider mapping:
    1. Copy `foo_provider.rs`.
    2. Update the schema types.
    3. Wire it into the registry in `mod.rs`.
    4. Run `cargo test -p ingest new_provider_roundtrip`.”

- **How to debug**
  - “To debug weird trace ordering issues, log X and run Y.”
  - “Use this CLI tool to replay a webhook against this code.”

Basically: “When I (human you) need to touch this area, what do I actually do?”

---

### 4.3 `## Verification`

This is the Factory talk made concrete: how we validate work in this subtree.

Include:

- **Required commands**
  - Exact shell commands:
    - `cargo test -p my_crate ingest_roundtrip`
    - `pytest tests/ingest/test_vapi.py`
    - `just check:server`
  - Make them copy‑pastable and scoped; don’t always say “run every test in the repo.”

- **What they guarantee**
  - “This ensures provider payloads map to canonical traces without panicking.”
  - “This checks that all HTTP routes still follow the contract.”

- **When they must be run**
  - “Run this whenever you:
    - Add a new provider.
    - Touch mapping logic.
    - Change trace types.”
  - VS. “Only needed for larger refactors; small comment changes don’t require this.”

Optional but nice:

- **Non‑test validators**
  - “If you change migrations, run `sqlx prepare --check`.”
  - “Run `cargo clippy -p core` for new public APIs.”

The agent should be able to finish its edits, run the commands listed here, and know: “I am probably safe.”

---

### 4.4 `## Don’t do this`

This is entropy control / negative space.

Include:

- **Legacy patterns not to copy**
  - “You will see usage of `OldFixtureFactory`; don’t use it. Use `NewTestHarness` instead.”
  - “Old code may parse JSON by hand; new code must use typed adapters.”

- **Forbidden operations**
  - “No direct DB access from this layer.”
  - “No new global env vars; thread config through function parameters.”
  - “Do not introduce new public fields to `CoreType` without updating its docs/tests.”

- **Banned style (where needed)**
  - “No `unwrap()`/`expect()` here except in tests.”
  - “No `anyhow::Error` in this module; use specific error types.”

This is where you explicitly say: “Even if it compiles and there’s precedent in old code, *don’t* do it.”

---

### 4.5 `## Gotchas`

Short list of things that have bitten you before:

- Flaky tests and how to think about them.
- Env quirks (“needs REDIS_URL set or tests will hang”).
- Non‑obvious performance cliffs.
- Dangerous partial refactors (“if you change X but not Y, everything still compiles but behavior is wrong”).

Keep this tight. Each bullet should be something you wish previous‑you had known, not random trivia.

---

### 4.6 `## References`

Pointers, not walls of text:

- Deep design docs:
  - `/docs/types/principles.md`
  - `/docs/architecture/ingest.md`
- External specs / API docs:
  - “Provider X webhook schema: <url>”
- Local glossaries if you have jargon (“trace”, “span”, “segment” etc).

This is how you connect the small operational view to your big philosophical essays.

---

## 5. Root vs Domain vs Leaf specifics

### 5.1 Root `/AGENTS.md` (Constitution)

Contains:

- **Global worldview / taste (very compact)**
  - 3–7 bullets that define how this repo thinks about software:
    - “Types should tell the truth, especially uncomfortable truths.”
    - “Information is never silently dropped; lossy transforms are explicit.”
    - “Rust is source of truth; Python is legacy/delete‑only.”

- **Repo topology**
  - What the major top-level directories are for.
  - Maybe a tiny map: `core/`, `server/`, `ui/`, `scripts/`, `tests/`.

- **Global axioms / bans**
  - E.g., “No `unwrap`/`expect` in production code anywhere.”
  - “Raw external payloads must always be storable and introspectable.”

- **Global verification norm**
  - A couple of commands you basically always run before pushing to main.
  - High‑level testing story (unit vs integration vs e2e).

This file is rarely changed and has the highest level of abstraction.

### 5.2 Domain `AGENTS.md` (subsystem roots)

For directories like `core/`, `ingest/`, `server/`, `tests/`, etc.

Heavier on:

- Boundary: domain responsibility and deps.
- Local taste: how this domain interprets the global principles.
- Good patterns/bad patterns specific to this domain.
- Domain‑specific verification (e.g., property tests in core, e2e tests in server).

This is where most of the interesting content lives.

### 5.3 Leaf `AGENTS.md` (deep modules)

Very focused; often:

- A 1–2 sentence boundary remark if needed.
- Exact commands to run when touching this.
- 1–2 golden exemplar file paths.
- 1–2 don’ts / gotchas.

Think of these as “micro‑playbooks” for the really weird/dense areas.

---

## 6. Code style: where it belongs

Code style has two homes:

1. **Global style (root-level)**
   - Language‑wide norms:
     - Rust: prefer enums + typestates; no `unwrap`; explicit errors.
     - Python: type hints required in `app/`; ruff config is canonical style.
   - Formatting rules:
     - “Always run `rustfmt` / `ruff format` / `prettier` on save.”
   - High‑level patterns:
     - “Keep functions short; long ones must be heavily structured.”
     - “Prefer composition over inheritance.”
   - If your linter config already enforces most of this, just:
     - link to it; and
     - note any *non-linted* taste you care about.

2. **Local style (per-domain AGENTS.md)**
   - Only include style rules that are *domain‑specific*:
     - Test style in `/tests/`.
     - Error mapping style in `/ingest/`.
     - API response shaping style in `/server/`.

Don’t duplicate linter manuals. Focus on how style + architecture + domain semantics interact.

---

## 7. Writing guidelines

A few meta‑rules for authoring AGENTS.md files:

- **Write for a smart stranger with local eyesight.**
  - Assume they can read code and tests.
  - Assume they can search the repo.
  - Don’t assume they know the history or tribal rules.

- **Be concrete.**
  - Prefer “run `cargo test -p ingest roundtrip`” over “run the tests.”
  - Prefer “copy `foo.rs`’s pattern” over “follow good FP practices.”

- **Keep them small but dense.**
  - Aggressively cut fluff.
  - If something is important but long, move it to `docs/` and link it.

- **Don’t repeat yourself across levels.**
  - Put global stuff in root.
  - Put domain stuff in domain root.
  - Leaf files should specialize, not re-explain.

- **Update with behavior.**
  - Whenever you realize “AGENTS.md lied to me” during a change:
    - update it in the *same PR* to reflect reality.
  - Treat these files like code, not like static docs.

---

## 8. Tooling expectations (for later)

If you (or a platform) wire this into an agent:

- For a given edit operation, the agent should:
  - Load **all applicable AGENTS.md files** along the path.
  - Use them to:
    - shape its system prompt / instructions;
    - decide which commands/tests to run after editing;
    - decide what patterns to look at in the repo as few‑shot examples.
- CI / bots can:
  - Run the commands specified under `## Verification` for touched directories.
  - Enforce banned patterns under `## Don’t do this`.

---

If a contributor (human or LLM) **actually follows** a well-written AGENTS.md that obeys this spec, you get:

- Architecture that doesn’t silently rot.
- Style that stays coherent without bikeshedding.
- Changes that are verifiable with cheap, local loops.
- Agents that behave like opinionated seniors, not autocomplete with a GPU.

That’s the bar.
