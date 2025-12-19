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
    if u.is_empty() {
        return String::new();
    }

    if let Some(stripped) = u.strip_suffix(".git") {
        u = stripped;
    }
    u = u.trim_end_matches('/');

    if let Some((scheme, after_scheme)) = u.split_once("://") {
        if scheme.eq_ignore_ascii_case("file") {
            return after_scheme.trim_end_matches('/').to_string();
        }
        let after_at = after_scheme
            .rsplit_once('@')
            .map(|(_, r)| r)
            .unwrap_or(after_scheme);
        let mut parts = after_at.splitn(2, '/');
        let host = parts.next().unwrap_or("");
        let path = parts.next().unwrap_or("");
        if !host.is_empty() && !path.is_empty() {
            return format!("{}/{}", normalize_host(host), path.trim_start_matches('/'));
        }
    }

    if let Some((left, right)) = u.split_once(':')
        && !right.is_empty()
        && !left.contains('/')
        && (left.contains('@') || left.contains('.'))
    {
        let host = left.rsplit_once('@').map(|(_, h)| h).unwrap_or(left);
        return format!("{}/{}", normalize_host(host), right.trim_start_matches('/'));
    }

    u.to_string()
}

fn normalize_host(host: &str) -> String {
    if let Some((host_part, port)) = host.split_once(':') {
        format!("{}:{}", host_part.to_lowercase(), port)
    } else {
        host.to_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_https_and_ssh() {
        assert_eq!(
            normalize_url("https://github.com/foo/bar.git"),
            "github.com/foo/bar"
        );
        assert_eq!(
            normalize_url("git@github.com:foo/bar.git"),
            "github.com/foo/bar"
        );
        assert_eq!(
            normalize_url("ssh://git@github.com/foo/bar"),
            "github.com/foo/bar"
        );
    }

    #[test]
    fn normalize_with_port_and_case() {
        assert_eq!(
            normalize_url("ssh://git@GITHUB.com:2222/foo/bar"),
            "github.com:2222/foo/bar"
        );
    }

    #[test]
    fn normalize_trims_slashes() {
        assert_eq!(
            normalize_url("git@github.com:foo/bar/"),
            "github.com/foo/bar"
        );
    }

    #[test]
    fn normalize_file_urls_are_stable() {
        assert_eq!(normalize_url("file:///tmp/example.git"), "/tmp/example");
    }
}
