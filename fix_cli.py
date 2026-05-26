import re

paths = [
    'crates/beads-cli/src/commands/comments.rs',
    'crates/beads-cli/src/commands/create.rs',
    'crates/beads-cli/src/commands/store.rs',
    'crates/beads-cli/src/render.rs'
]

for path in paths:
    with open(path, 'r') as f:
        content = f.read()

    # Replaces
    content = content.replace("beads_surface::ipc::IpcError::from", "|source| beads_surface::ipc::IpcError::Transport { source }")
    content = content.replace("beads_surface::IpcError::from", "|source| beads_surface::IpcError::Transport { source }")
    # Actually wait, in render.rs, one is serde_json::Error
    # We can be more precise
    pass

def process(path):
    with open(path, 'r') as f:
        content = f.read()

    if "render.rs" in path:
        content = content.replace(".map_err(beads_surface::ipc::IpcError::from)?", ".map_err(|source| beads_surface::ipc::IpcError::PayloadEncode { source })?")
        content = content.replace("beads_surface::ipc::IpcError::from(err)", "beads_surface::ipc::IpcError::Transport { source: err }")
    else:
        content = content.replace(".map_err(beads_surface::ipc::IpcError::from)?", ".map_err(|source| beads_surface::ipc::IpcError::Transport { source })?")
        content = content.replace(".map_err(beads_surface::IpcError::from)?", ".map_err(|source| beads_surface::IpcError::Transport { source })?")
        content = content.replace("beads_surface::ipc::IpcError::from(err)", "beads_surface::ipc::IpcError::Transport { source: err }")

    with open(path, 'w') as f:
        f.write(content)

for path in paths:
    process(path)
