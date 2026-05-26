with open("crates/beads-daemon/src/runtime/run.rs", "r") as f:
    content = f.read()

content = content.replace("IpcError::Transport { source: std::io::Error::new(\n            std::io::ErrorKind::AlreadyExists,\n            format!(\"daemon socket already in use: {}\", socket.display()),\n        ))", "IpcError::Transport { source: std::io::Error::new(\n            std::io::ErrorKind::AlreadyExists,\n            format!(\"daemon socket already in use: {}\", socket.display()),\n        ) }")

with open("crates/beads-daemon/src/runtime/run.rs", "w") as f:
    f.write(content)
