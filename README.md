# beads-rs

a rust redesign of [beads](https://github.com/steveyegge/beads). distributed issue tracker for agent swarms, backed by git. conflicts are impossible.

## install

```bash
curl -fsSL https://raw.githubusercontent.com/delightful-ai/beads-rs/main/scripts/install.sh | bash
```

prebuilt binaries for x86_64 linux and apple silicon. other platforms auto-fallback to cargo.

```bash
cargo install beads-rs                              # cargo
mise use -g ubi:delightful-ai/beads-rs[exe=bd]      # mise
nix run github:delightful-ai/beads-rs               # nix
```

## quick start

in any git repo:

```bash
bd init
bd setup claude # or cursor or aider
```

and you're done!

## why

beads lets agents coordinate work and defer problems to the next session with a fresh context window. 50 agents on one codebase need to see the same task list - who's working on what, what's blocked, what's done.

the go version works for one human with occasional sync. beads-rs is designed for swarms: one daemon per machine shares state across all clones instantly. no sqlite, everything lives in git on `refs/heads/beads/store`.

this means agents never push to main when updating beads. no merge conflicts on your code branches. beads-rs just works, and gets out of the way.

## status

alpha, but designed for reliability first. stress tested on my workflows.

## differences from beads-go

beads-rs is a **drop-in replacement** for core workflows. the main difference is the sync model - agents never need to run `bd sync`, beads can't have merge conflicts, and it works across machines automatically.

**what's missing:**

| feature | status |
|---------|--------|
| agent mail (real-time multi-agent) | not yet |
| multi-repo state sharing | not yet |
| jira integration | not yet |
| doctor/repair commands | not yet |
| config system | not yet |
| templates | not yet |
| compaction/decay | not yet |

if you need agent mail or multi-repo, use [the original](https://github.com/steveyegge/beads).

## technical details

**requirements:**
- git repo with an `origin` remote (recommended; local-only works too)
- linux + macos supported; windows not yet

**where data lives:**
- canonical state: `refs/heads/beads/store`
- files: `state.jsonl`, `tombstones.jsonl`, `deps.jsonl`, `meta.json`
- daemon socket: `$XDG_RUNTIME_DIR/beads/daemon.sock` or `~/.beads/daemon.sock` or `/tmp/beads-$uid/daemon.sock`

**sync model:**
- cli auto-starts a local daemon on demand
- mutations are debounced and pushed in the background
- `bd sync` is just "wait for flush", not a workflow step

## editor integration

```bash
bd setup claude
bd setup cursor
bd setup aider
```

## docs

- `CLI_SPEC.md` - cli surface / compatibility goals
- `SPEC.md` - storage model + invariants

## nix flake

```nix
{
  inputs.beads-rs.url = "github:delightful-ai/beads-rs";

  outputs = { self, nixpkgs, beads-rs, ... }: {
    nixpkgs.overlays = [ beads-rs.overlays.default ];
    # then pkgs.beads-rs or pkgs.bd is available
  };
}
```

to update: `nix flake update beads-rs`

## license

MIT
