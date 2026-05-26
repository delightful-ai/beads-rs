#!/bin/bash
sed -i 's/IpcError::Io(\([^)]*\))/IpcError::Transport { source: \1 }/g' crates/beads-surface/src/ipc/client.rs
sed -i 's/IpcError::Parse(\([^)]*\))/IpcError::PayloadDecode { source: \1 }/g' crates/beads-surface/src/ipc/client.rs
