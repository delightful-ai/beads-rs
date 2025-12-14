//! Remote URL identity for multi-clone shared state.
//!
//! We key daemon state by a normalized remote URL so that multiple clones of the
//! same repo on one machine share in-memory state instantly.

use std::fmt;

/// Normalized remote URL used as a stable key.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RemoteUrl(pub(crate) String);

impl RemoteUrl {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RemoteUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RemoteUrl({:?})", self.0)
    }
}

impl fmt::Display for RemoteUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Normalize a git remote URL into a canonical host/path-ish string.
///
/// Examples:
/// - `git@github.com:foo/bar.git`     → `github.com/foo/bar`
/// - `https://github.com/foo/bar.git` → `github.com/foo/bar`
/// - `ssh://git@github.com/foo/bar`  → `github.com/foo/bar`
pub fn normalize_url(url: &str) -> String {
    let mut u = url.trim();
    if let Some(stripped) = u.strip_suffix(".git") {
        u = stripped;
    }

    // SSH scp-style: git@host:path
    if let Some(rest) = u.strip_prefix("git@")
        && let Some((host, path)) = rest.split_once(':')
    {
        return format!("{}/{}", host, path.trim_start_matches('/'));
    }

    // Scheme URLs: https://host/path or ssh://git@host/path
    if let Some((_, after_scheme)) = u.split_once("://") {
        let after_at = after_scheme
            .rsplit_once('@')
            .map(|(_, r)| r)
            .unwrap_or(after_scheme);
        let mut parts = after_at.splitn(2, '/');
        let host = parts.next().unwrap_or("");
        let path = parts.next().unwrap_or("");
        if !host.is_empty() && !path.is_empty() {
            return format!("{}/{}", host, path.trim_start_matches('/'));
        }
    }

    // Fallback: use as-is (covers file:// and local path remotes).
    u.to_string()
}
