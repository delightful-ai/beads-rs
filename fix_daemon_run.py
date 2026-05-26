with open("crates/beads-daemon/src/runtime/run.rs", "r") as f:
    content = f.read()

content = content.replace("IpcError::from", "|source| IpcError::Transport { source }")

with open("crates/beads-daemon/src/runtime/run.rs", "w") as f:
    f.write(content)
