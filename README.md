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

then, ask your coding agent to run `bd onboard`, and you're done!

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
| config system | partial (auto-upgrade) |
| templates | not yet |
| compaction/decay | not yet |

if you need agent mail or multi-repo, use [the original](https://github.com/steveyegge/beads).

## migration path

```bash
bd migrate from-go --input .beads/issues.jsonl --dry-run
# if that looks good, run
bd migrate from-go --input .beads/issues.jsonl
# and you're good!
```
more details in `MIGRATION.md`.


## technical details

**requirements:**
- git repo with an `origin` remote (recommended; local-only works too)
- linux + macos supported; windows not yet

**where data lives:**
- canonical state: `refs/heads/beads/store`
- bounded backup refs: `refs/beads/backup/*` (latest 64 retained)
- files: `state.jsonl`, `tombstones.jsonl`, `deps.jsonl`, `meta.json`
- daemon socket: `$XDG_RUNTIME_DIR/beads/daemon.sock` or `~/.beads/daemon.sock` or `/tmp/beads-$uid/daemon.sock`

**sync model:**
- cli auto-starts a local daemon on demand
- mutations are debounced and pushed in the background
- `bd sync` is just "wait for flush", not a workflow step
- backup ref maintenance is best-effort under lock contention and uses age/PID-aware stale lock cleanup

## editor integration

```bash
bd setup claude
bd setup cursor
bd setup aider
```

## upgrade

```bash
bd upgrade
```

auto-upgrade is enabled by default and controlled by a toml config:
`$XDG_CONFIG_HOME/beads-rs/config.toml` (or `~/.config/beads-rs/config.toml`).

## config

beads reads a single config file:

```
~/.config/beads-rs/config.toml
```

example:

```toml
auto_upgrade = true
```

override location with `BD_CONFIG_DIR`. disable auto-upgrade for a single run with
`BD_NO_AUTO_UPGRADE=1`.

## docs

- `CLI_SPEC.md` - cli surface / compatibility goals
- `SPEC.md` - storage model + invariants
- `MIGRATION.md` - migrate from beads-go
- `BENCHMARKING.md` - hotpath benchmark workflow and artifact interpretation

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
