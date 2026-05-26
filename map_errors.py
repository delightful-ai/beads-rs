import re

def process(content):
    # Regex to match `?` that return IpcError for std::io::Error
    # We will use regex to find common patterns.
    # Actually, we can just replace specific patterns.

    # .read_exact(..)? -> .read_exact(..).map_err(|source| IpcError::Transport { source })?
    content = re.sub(r'\.read_exact\(([^)]+)\)\?', r'.read_exact(\1).map_err(|source| IpcError::Transport { source })?', content)
    content = re.sub(r'\.write_all\(([^)]+)\)\?', r'.write_all(\1).map_err(|source| IpcError::Transport { source })?', content)
    content = re.sub(r'serde_json::to_vec\(([^)]+)\)\?', r'serde_json::to_vec(\1).map_err(|source| IpcError::PayloadEncode { source })?', content)
    content = re.sub(r'serde_json::from_slice\(([^)]+)\)\?', r'serde_json::from_slice(\1).map_err(|source| IpcError::PayloadDecode { source })?', content)
    content = re.sub(r'serde_json::to_string\(([^)]+)\)\?', r'serde_json::to_string(\1).map_err(|source| IpcError::PayloadEncode { source })?', content)
    content = re.sub(r'serde_json::from_str\(([^)]+)\)\?', r'serde_json::from_str(\1).map_err(|source| IpcError::PayloadDecode { source })?', content)
    content = re.sub(r'\.try_clone\(\)\?', r'.try_clone().map_err(|source| IpcError::Transport { source })?', content)
    content = re.sub(r'\.set_read_timeout\(([^)]+)\)\?', r'.set_read_timeout(\1).map_err(|source| IpcError::Transport { source })?', content)
    content = re.sub(r'\.set_nonblocking\(([^)]+)\)\?', r'.set_nonblocking(\1).map_err(|source| IpcError::Transport { source })?', content)

    content = content.replace('IpcError::Parse(', 'IpcError::PayloadDecode { source: ')
    content = content.replace('IpcError::Io(', 'IpcError::Transport { source: ')
    # Fix the ones from the manual sed replace that were left with missing }
    content = re.sub(r'IpcError::PayloadDecode \{ source: ([^)]+)\)', r'IpcError::PayloadDecode { source: \1 }', content)
    content = re.sub(r'IpcError::Transport \{ source: ([^)]+)\)', r'IpcError::Transport { source: \1 }', content)
    return content

for path in ['crates/beads-surface/src/ipc/client.rs', 'crates/beads-surface/src/ipc/codec.rs']:
    with open(path, 'r') as f:
        content = f.read()
    content = process(content)
    with open(path, 'w') as f:
        f.write(content)
