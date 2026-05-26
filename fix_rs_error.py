with open("crates/beads-rs/src/error.rs", "r") as f:
    content = f.read()

content = content.replace("IpcError::from(err)", "IpcError::Transport { source: err }")

with open("crates/beads-rs/src/error.rs", "w") as f:
    f.write(content)
