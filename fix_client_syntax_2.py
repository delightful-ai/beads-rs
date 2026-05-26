with open("crates/beads-surface/src/ipc/client.rs", "r") as f:
    content = f.read()
content = content.replace("IpcError::DaemonUnavailable(_ }", "IpcError::DaemonUnavailable(_) ")
content = content.replace("IpcError::DaemonVersionMismatch { .. }", "IpcError::DaemonVersionMismatch { .. }")
with open("crates/beads-surface/src/ipc/client.rs", "w") as f:
    f.write(content)
