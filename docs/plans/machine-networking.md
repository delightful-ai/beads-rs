# Machine-Level Networking UX

## Overview

Users think in terms of **machines**, not stores or repos. The current replication model is per-store, which creates friction when managing multiple repos across multiple machines. This plan introduces a machine-level abstraction that makes networking feel natural while preserving the underlying store-based architecture.

## Design Principles

1. **Zero config works** — `bd init` with no machine setup just works (local + git sync)
2. **Machine-level setup, not per-repo** — configure networking once per machine, all repos inherit
3. **Two commands max** — onboarding a machine should be trivial
4. **Scripting-first** — everything works non-interactively for infra/provisioning
5. **Failures are opt-in** — default durability never blocks on network

---

## Durability Model

### Classes

| Class | Meaning | When you get "ok" |
|-------|---------|-------------------|
| `local` | Written to this machine's WAL | After local fsync (~2ms) |
| `replicated(k)` | Written to this machine + k peers | After local fsync + k peer acks |

### Defaults

- **Default durability**: `local`
- **Core namespace**: Also syncs to git asynchronously (~500ms debounce). Git is the safety net.
- **Non-core namespaces**: WAL only. Replication matters more here.

### Behavior with unavailable peers

| Durability | Peers offline | Result |
|------------|---------------|--------|
| `local` (default) | N/A | Always succeeds |
| `replicated(k)` | Yes | Fails (explicit request couldn't be satisfied) |

Since `local` is the default, peer availability never blocks normal usage.

---

## Machine Configuration

### Files

```
~/.config/beads-rs/
  machine.toml      # Machine identity and role
  peers.toml        # Known peer machines
```

### machine.toml

```toml
name = "laptop"
machine_id = "uuid-..."  # Auto-generated
role = "peer"            # "peer" | "ephemeral"
listen = "0.0.0.0:7337"
```

### peers.toml

```toml
[server]
address = "server.local:7337"
machine_id = "uuid-..."
added_at = "2025-01-15T..."
```

---

## CLI Commands

### `bd machine init`

Initialize this machine for networking.

```bash
bd machine init [OPTIONS]
```

**Options:**
- `--name NAME` — Human-readable name (default: hostname)
- `--listen ADDR` — Listen address (default: `0.0.0.0:7337`)
- `--ephemeral` — Mark as not durability-eligible (for containers)
- `--yes` — Skip confirmation prompts

**Environment variables:**
- `BD_MACHINE_NAME`
- `BD_MACHINE_LISTEN`
- `BD_MACHINE_EPHEMERAL=1`

**Output:**
```
Machine 'laptop' initialized.
Listening on 0.0.0.0:7337
Run 'bd machine invite' to add other machines.
```

**JSON output (`--json`):**
```json
{
  "status": "ok",
  "name": "laptop",
  "machine_id": "550e8400-...",
  "listen": "0.0.0.0:7337"
}
```

**Exit codes:**
- `0` — Success
- `1` — Error
- `4` — Already initialized (idempotent)

---

### `bd machine invite`

Generate an invite for another machine to join.

```bash
bd machine invite [OPTIONS]
```

**Options:**
- `--ephemeral` — Invite for an ephemeral machine (pre-marks as not durability-eligible)
- `--expires DURATION` — Invite expiration (e.g., `24h`, `7d`)
- `--reusable` — Allow multiple uses
- `--count N` — Max uses for reusable invite
- `--token-only` — Output just the token (for scripting)

**Output:**
```
Invite code (share with the other machine):

  bd machine join bd-invite:server.local:7337:x8f2k9d...

Or manually:
  Address: server.local:7337
  Token: x8f2k9d...
```

**Token-only output:**
```bash
bd machine invite --token-only
# x8f2k9d...
```

---

### `bd machine join`

Join an existing network.

```bash
bd machine join [INVITE_CODE | --address ADDR]
```

**Arguments:**
- `INVITE_CODE` — Full invite code from `bd machine invite`

**Options:**
- `--address ADDR` — Peer address (alternative to invite code)
- `--token TOKEN` — Auth token (required with `--address`)
- `--yes` — Skip confirmation prompts

**Environment variables:**
- `BD_MACHINE_PEERS` — Comma-separated addresses
- `BD_MACHINE_TOKEN` — Auth token

**Output:**
```
Connected to: server (10.0.0.5:7337)
Machine 'laptop' is now part of the network.

Stores in common: 2
  myproject — replicating
  notes — replicating
```

**Exit codes:**
- `0` — Success
- `1` — Error
- `2` — Connection failed
- `3` — Auth failed
- `4` — Already joined (idempotent)

---

### `bd machine doctor`

Diagnose machine networking health.

```bash
bd machine doctor [OPTIONS]
```

**Options:**
- `--json` — JSON output

**Output:**
```
Machine: laptop
Listen: 0.0.0.0:7337 ✓

Peers:
  server (10.0.0.5:7337)
    Status: connected ✓
    Lag: <1s
    Stores in common: 3

Stores on this machine: 3
  myproject — replicated ✓
  notes — replicated ✓
  experiments — local only (peer doesn't have it)
```

---

### `bd machine status`

Quick one-liner status.

```bash
bd machine status
# laptop: 1 peer connected, 3 stores replicated
```

---

### `bd machine list`

List known peers.

```bash
bd machine list
# server  10.0.0.5:7337  connected  lag <1s
# backup  10.0.0.6:7337  offline    last seen 2h ago
```

---

### `bd machine forget`

Remove a peer.

```bash
bd machine forget <NAME>
```

---

## User Flows

### Single machine (no networking)

```bash
cd myrepo
bd init
# Works immediately. Local WAL + git sync.
```

No machine config needed. This is the default experience.

### First machine in a network

```bash
bd machine init --name server
bd machine invite
# → bd-invite:server.local:7337:x8f2k...
```

### Second machine joining

```bash
bd machine init --name laptop
bd machine join bd-invite:server.local:7337:x8f2k...
# → Connected to: server
```

### New repo on either machine

```bash
cd newrepo
bd init
# Auto-discovers peers from machine config
# → "Store created. Replicating with: server"
```

### Check health

```bash
bd machine doctor
```

---

## Provisioning & Automation

### Environment variables

```bash
export BD_MACHINE_NAME="worker-$(hostname)"
export BD_MACHINE_LISTEN="0.0.0.0:7337"
export BD_MACHINE_EPHEMERAL=1
export BD_MACHINE_PEERS="anchor.internal:7337"
export BD_MACHINE_TOKEN="x8f2k..."

bd machine init  # Picks up from env
```

### Idempotent execution

All commands are safe to re-run:

```bash
bd machine init --name worker  # Creates config
bd machine init --name worker  # No-op, exit 4

bd machine join --address server:7337 --token x
bd machine join --address server:7337 --token x  # No-op, exit 4
```

### Dockerfile

```dockerfile
FROM ubuntu:22.04

RUN curl -sSL https://beads.dev/install.sh | sh

ENV BD_MACHINE_NAME="worker"
ENV BD_MACHINE_EPHEMERAL=1

CMD ["sh", "-c", "bd machine init --yes && bd machine join --address $ANCHOR_ADDRESS --token $JOIN_TOKEN --yes && bd daemon run"]
```

### Ansible

```yaml
- name: Initialize beads machine
  command: >
    bd machine init
    --name {{ inventory_hostname }}
    --ephemeral
    --yes
  environment:
    BD_MACHINE_PEERS: "{{ anchor_address }}"
    BD_MACHINE_TOKEN: "{{ hostvars['anchor']['join_token'] }}"
  register: result
  changed_when: result.rc == 0
  failed_when: result.rc not in [0, 4]
```

### Terraform

```hcl
resource "null_resource" "beads_init" {
  provisioner "remote-exec" {
    inline = [
      "bd machine init --name ${var.machine_name} --ephemeral --yes",
      "bd machine join --address ${var.anchor_address} --token ${var.join_token} --yes"
    ]
  }
}
```

---

## Storing Network Config in Git (Optional)

Anchor info can be stored in the repo for discoverability:

```bash
# On anchor
bd machine init --name server --advertise server.local:7337
bd init --write-network-config
# Writes .beads/network.toml to repo
```

```toml
# .beads/network.toml (committed to git)
[anchors]
server = "server.local:7337"
```

On clone:
```bash
git clone <repo>
bd init
# Reads .beads/network.toml
# "Found network config. Connect to 'server'? [Y/n]"
```

With `--yes` (scripting):
```bash
bd init --yes  # Auto-connects if network.toml exists
```

---

## How It Works Internally

### Store ↔ Machine relationship

1. Machine config lives at `~/.config/beads-rs/`
2. Each store (per repo) registers with the machine layer on `bd init`
3. Machine layer maintains peer connections
4. When stores load, they discover which peers have the same store (by store_id)
5. Replication sessions are established per-store, but connection management is per-machine

### Handshake

When machines connect:
1. Exchange machine identities (name, machine_id)
2. Exchange list of store_ids each has
3. For each store in common, establish replication session
4. Stores not in common are ignored (no error)

This means:
- Adding a peer is a one-time machine operation
- New repos on either machine auto-discover each other
- Repos that only exist on one machine just don't replicate (no error)

---

## Migration from Current Model

Current users with per-repo replication config:
1. Config continues to work
2. Machine config takes precedence if present
3. `bd machine init` can optionally migrate existing peer configs

---

## Open Questions

1. **Auth model** — Are invite tokens sufficient? Need mTLS for production?
2. **Discovery** — Should we support mDNS/Bonjour for LAN discovery?
3. **NAT traversal** — Support for STUN/TURN or relay for machines behind NAT?
4. **Machine groups** — Ability to group peers (e.g., "prod" vs "dev")?
