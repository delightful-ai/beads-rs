with open("crates/beads-surface/src/ipc/client.rs", "r") as f:
    content = f.read()

# Fix some remaining ? issues
content = content.replace("UnixStream::connect(&socket).map_err(IpcError::from)", "UnixStream::connect(&socket).map_err(|source| IpcError::Transport { source })")
content = content.replace("self.writer.set_write_timeout(timeout)?", "self.writer.set_write_timeout(timeout).map_err(|source| IpcError::Transport { source })?")
content = content.replace("ensure_autostart_socket_parent(socket, dir)?", "ensure_autostart_socket_parent(socket, dir).map_err(|source| IpcError::Transport { source })?")
content = content.replace("stream.write_all(json.as_bytes())?", "stream.write_all(json.as_bytes()).map_err(|source| IpcError::Transport { source })?")
content = content.replace("reader.read_line(&mut line)?", "reader.read_line(&mut line).map_err(|source| IpcError::Transport { source })?")
content = content.replace("self.reader.read_line(&mut line)?", "self.reader.read_line(&mut line).map_err(|source| IpcError::Transport { source })?")

# Fix pattern match
content = content.replace("IpcError::PayloadDecode { source: _ }", "IpcError::PayloadDecode { source: _ }\n                | IpcError::PayloadEncode { source: _ }")

with open("crates/beads-surface/src/ipc/client.rs", "w") as f:
    f.write(content)
