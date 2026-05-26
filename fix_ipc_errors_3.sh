#!/bin/bash
# Revert the first buggy sed that used map_err wrong, it was better to just add the correct error mappings.

sed -i 's/\.map_err(|source| IpcError::Transport { source })?/?/g' crates/beads-surface/src/ipc/client.rs

# Let's write a python script to fix this.
