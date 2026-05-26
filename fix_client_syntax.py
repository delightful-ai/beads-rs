with open("crates/beads-surface/src/ipc/client.rs", "r") as f:
    lines = f.readlines()
for i in range(len(lines)):
    lines[i] = lines[i].replace("IpcError::Transport { source: e } }", "IpcError::Transport { source: e })")
    lines[i] = lines[i].replace("IpcError::Transport { source: source } }", "IpcError::Transport { source: source })")
    lines[i] = lines[i].replace("IpcError::InvalidId(_ }", "IpcError::InvalidId(_) ")
with open("crates/beads-surface/src/ipc/client.rs", "w") as f:
    f.writelines(lines)
