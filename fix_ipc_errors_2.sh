#!/bin/bash
sed -i 's/\.map_err(|e| IpcError::Transport { source: e })?/?/g' crates/beads-surface/src/ipc/client.rs
sed -i 's/IpcError::Transport { source: \([^}]*\) }/IpcError::Transport { source: \1 }/g' crates/beads-surface/src/ipc/client.rs
