with open("crates/beads-daemon/src/runtime/run.rs", "r") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "IpcError::Transport { source: std::io::Error::new(" in line:
        pass
    if "        ))" in line:
        lines[i] = "        ) }\n"
        break

with open("crates/beads-daemon/src/runtime/run.rs", "w") as f:
    f.writelines(lines)
