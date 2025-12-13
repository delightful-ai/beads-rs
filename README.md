# beads-rs

beads-rs is a rust redesign of [beads](https://github.com/steveyegge/beads). it's a task coordination system for agent swarms. it's a distributed issue tracker backed by git. conflicts are impossible.

## install

```bash
curl -fsSL https://raw.githubusercontent.com/delightful-ai/beads-rs/main/scripts/install.sh | bash
```

or with cargo:
```bash
cargo install beads-rs
```

or with nix (run directly):
```bash
nix run github:delightful-ai/beads-rs -- --help
```

or in your flake:
```nix
{
  inputs.beads-rs.url = "github:delightful-ai/beads-rs";

  outputs = { self, nixpkgs, beads-rs, ... }: {
    # use the overlay
    nixpkgs.overlays = [ beads-rs.overlays.default ];
    # then pkgs.beads-rs or pkgs.bd is available

    # or reference directly
    environment.systemPackages = [ beads-rs.packages.${system}.default ];
  };
}
```

to update: `nix flake update beads-rs`

## why

beads is for agents to coordinate arbitrary work. it lets agents defer fixing problems to the next session, where you can start with a fresh context window. beads is a substrate for long-horizon planning for any form of work that can live in git.

50 agents working on one codebase need to see the same task list. who's working on what. what's blocked. what's done. with multiple local clones or worktrees, beads has instant sync. over multiple machines, seconds, backed by CRDTs.

agents love using beads. they will use it of their own accord.


## why rewrite

the go version works for one human, some clones, occasional sync. but agents have to manage syncing themselves. i had to spawn up 50 agents at a time, and the architecture makes it hard to work in that setting.

beads-rs can't corrupt. conflicts are impossible. one daemon per machine shares state across all clones instantly. there is no sqlite, everything lives in git. beads are stored on /refs/heads/beads/store. 

this also means that there is no reason for agents to push to main when updating beads. there will never be merge conflicts on your code branches. beads-rs (hopefully) just works, is reliable infrastructure, and gets our of the way.

## status

alpha. 

but designed for reliability first, and really hard to fuck up using if you're an agent.

stress tested and works on my workflows. the type design means it should... in theory, just work^tm and i have made every effort to make that the case.

## differences from beads-go

beads-rs is a **drop-in replacement for core workflows** but doesn't have everything yet:

| feature | beads-rs | beads-go |
|---------|----------|----------|
| core CRUD (create/show/list/update/close) | yes | yes |
| dependencies (blocks/parent) | yes | yes |
| claims/leases | yes | yes |
| labels | yes | yes |
| epics | yes | yes |
| JSON output (`--json`) | yes | yes |
| multi-clone instant sync | yes | yes |
| git-backed CRDT storage | yes | sqlite + jsonl |
| **agent mail** (real-time multi-agent) | no | yes |
| **multi-repo** state sharing | no | yes |
| **jira integration** | no | yes |
| **doctor/repair** commands | no | yes |
| **config system** | no | yes |
| **templates** | no | yes |
| **compaction/decay** | no | yes |

if you need agent mail or multi-repo, use the original. if you want simpler architecture and don't mind missing features, try beads-rs.

## requirements

- git repo with an `origin` remote (recommended; local-only works too)
- linux + macos are supported targets; windows support is not a focus yet


## quick start

in any git repo:

```bash
bd init
bd create "try beads-rs" -t task -p 2 -d "smoke test"
bd ready
```

machine-readable output for agents:

```bash
bd --json ready
```

## where the data lives

- canonical state is stored in git on the ref: `refs/heads/beads/store`
- that ref contains: `state.jsonl`, `tombstones.jsonl`, `deps.jsonl`, `meta.json`
- the daemon listens on a unix socket at:
  - `$XDG_RUNTIME_DIR/beads/daemon.sock`, else `~/.beads/daemon.sock`, else `/tmp/beads-$uid/daemon.sock`

## sync model (tldr)

- the cli auto-starts a local daemon on demand
- mutations are debounced and pushed in the background
- `bd sync` exists, but it’s a "wait for flush" convenience, not a "git-sync-your-main-branch" workflow

## editor integration

beads-rs can install lightweight integrations so your agent gets context automatically:

```bash
bd setup claude
bd setup cursor
bd setup aider
```

## docs

- `CLI_SPEC.md` (cli surface / compatibility goals)
- `SPEC.md` (storage model + invariants)

## license

MIT
