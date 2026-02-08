//! Remote URL identity for multi-clone shared state.
//!
//! We key daemon state by a normalized remote URL so that multiple clones of the
//! same repo on one machine share in-memory state instantly.

use std::fmt;

/// Normalized remote URL used as a stable key.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RemoteUrl(pub(crate) String);

impl RemoteUrl {
    pub fn new(raw: &str) -> Self {
        Self(normalize_url(raw))
    }

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
    let raw = url.trim();
    if raw.is_empty() {
        return String::new();
    }

    if let Some((scheme, rest)) = split_scheme(raw) {
        if scheme.eq_ignore_ascii_case("file") {
            return normalize_file_url(rest);
        }

        if let Some((host, path)) = split_host_path(rest) {
            let path = normalize_repo_path(path, false);
            if !host.is_empty() && !path.is_empty() {
                return format!("{}/{}", normalize_host(host), path);
            }
        }
    }

    if let Some((left, right)) = raw.split_once(':')
        && !right.is_empty()
        && !left.contains('/')
        && (left.contains('@') || left.contains('.') || left.starts_with('['))
    {
        let host = left.rsplit_once('@').map(|(_, h)| h).unwrap_or(left);
        let path = normalize_repo_path(right, false);
        if !host.is_empty() && !path.is_empty() {
            return format!("{}/{}", normalize_host(host), path);
        }
    }

    if let Some((user_host, path)) = split_user_host_path(raw) {
        let host = user_host
            .rsplit_once('@')
            .map(|(_, h)| h)
            .unwrap_or(user_host);
        let path = normalize_repo_path(path, false);
        if !host.is_empty() && !path.is_empty() {
            return format!("{}/{}", normalize_host(host), path);
        }
    }

    normalize_local_path(raw)
}

fn split_scheme(raw: &str) -> Option<(&str, &str)> {
    raw.split_once("://")
}

fn split_host_path(after_scheme: &str) -> Option<(&str, &str)> {
    let after_at = after_scheme
        .rsplit_once('@')
        .map(|(_, r)| r)
        .unwrap_or(after_scheme);
    let mut parts = after_at.splitn(2, '/');
    let host = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    if host.is_empty() || path.is_empty() {
        return None;
    }
    Some((host, path))
}

fn split_user_host_path(raw: &str) -> Option<(&str, &str)> {
    if raw.contains("://") {
        return None;
    }
    if !raw.contains('@') {
        return None;
    }
    let mut parts = raw.splitn(2, '/');
    let user_host = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    if user_host.is_empty() || path.is_empty() {
        return None;
    }
    Some((user_host, path))
}

fn normalize_repo_path(path: &str, keep_leading_slash: bool) -> String {
    let mut p = path.trim();
    if !keep_leading_slash {
        p = p.trim_start_matches('/');
    }
    p = p.trim_end_matches('/');
    if let Some(stripped) = p.strip_suffix(".git") {
        p = stripped;
    }
    p.to_string()
}

fn normalize_local_path(raw: &str) -> String {
    normalize_repo_path(raw, true)
}

fn normalize_file_url(rest: &str) -> String {
    let rest = rest.trim();
    if rest.is_empty() {
        return String::new();
    }

    let (host, path) = if rest.starts_with('/') {
        ("", rest)
    } else if let Some((h, p)) = rest.split_once('/') {
        (h, p)
    } else {
        (rest, "")
    };

    let path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    };
    let path = normalize_repo_path(&path, true);

    if host.is_empty() || host.eq_ignore_ascii_case("localhost") {
        return path;
    }

    format!("//{}{}", host.to_lowercase(), path)
}

fn normalize_host(host: &str) -> String {
    if host.starts_with('[')
        && let Some((inner, port)) = host.split_once(']')
    {
        let inner = inner.trim_start_matches('[').to_lowercase();
        if let Some(port) = port.strip_prefix(':') {
            return format!("[{}]:{}", inner, port);
        }
        return format!("[{}]", inner);
    }

    if host.matches(':').count() == 1
        && let Some((host_part, port)) = host.rsplit_once(':')
    {
        return format!("{}:{}", host_part.to_lowercase(), port);
    }

    host.to_lowercase()
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

    #[test]
    fn normalize_file_url_localhost_matches_path() {
        assert_eq!(
            normalize_url("file://localhost/tmp/example.git/"),
            "/tmp/example"
        );
    }

    #[test]
    fn normalize_user_and_scheme_variants_match() {
        let a = normalize_url("SSH://git@GitHub.com/foo/bar.git");
        let b = normalize_url("https://github.com/foo/bar/");
        let c = normalize_url("git@github.com:foo/bar");
        let d = normalize_url("git@github.com/foo/bar.git");
        assert_eq!(a, b);
        assert_eq!(b, c);
        assert_eq!(c, d);
    }

    #[test]
    fn normalize_https_userinfo_matches_host_only() {
        assert_eq!(
            normalize_url("https://user@github.com/foo/bar.git"),
            "github.com/foo/bar"
        );
    }

    #[test]
    fn normalize_file_url_with_host_preserves_host() {
        assert_eq!(
            normalize_url("file://server/share/repo.git"),
            "//server/share/repo"
        );
    }

    #[test]
    fn normalize_local_paths_trim_git_suffix() {
        assert_eq!(normalize_url("/tmp/repo.git/"), "/tmp/repo");
        assert_eq!(normalize_url("./repo.git"), "./repo");
    }
}
