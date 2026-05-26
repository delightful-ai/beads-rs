with open("crates/beads-surface/src/ipc/codec.rs", "r") as f:
    content = f.read()
content = content.replace("let line = line?;", "let line = line.map_err(|source| IpcError::Transport { source })?;")
with open("crates/beads-surface/src/ipc/codec.rs", "w") as f:
    f.write(content)
