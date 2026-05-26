with open("crates/beads-rs/src/error.rs", "r") as f:
    content = f.read()

content = content.replace("GoImportError::Parse(err) => Error::Ipc(IpcError::Transport { source: err })", "GoImportError::Parse(err) => Error::Ipc(IpcError::PayloadDecode { source: err })")

with open("crates/beads-rs/src/error.rs", "w") as f:
    f.write(content)
