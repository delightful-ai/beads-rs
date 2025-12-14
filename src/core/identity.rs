//! Layer 1: Identity atoms
//!
//! ActorId: agent self-identification
//! BeadId: issue identifier with prefix
//! NoteId: note identifier within a bead

use std::fmt;

use serde::{Deserialize, Serialize};

use super::error::{CoreError, InvalidId};

/// Actor identifier - non-empty string.
///
/// Agents name themselves. No validation beyond non-empty.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ActorId(String);

impl ActorId {
    pub fn new(s: impl Into<String>) -> Result<Self, CoreError> {
        let s = s.into();
        if s.is_empty() {
            Err(InvalidId::Actor {
                raw: s,
                reason: "empty".into(),
            }
            .into())
        } else {
            Ok(Self(s))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorId({:?})", self.0)
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Alphabet for bead IDs (legacy + hash IDs).
///
/// beads-go hash IDs are lowercase hex, but legacy short IDs were lowercase
/// alphanumeric. We accept the superset for parsing.
const BEAD_ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";

/// Base58 alphabet (Bitcoin-style, no 0OIl) for internal note IDs.
const NOTE_ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/// Bead identifier - "{slug}-{suffix}" format.
///
/// Slug is a per-repo prefix (beads-go used the repo name; beads-rs historically used `bd`).
/// Suffix is lowercase alphanumeric. Hierarchical children append `.N`.
/// Only daemon generates new IDs (pub(crate)).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BeadId(String);

impl BeadId {
    const SLUG_ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz-._";

    /// Parse and validate a bead ID string.
    ///
    /// Accepted forms:
    /// - `<slug>-<root>` where:
    ///   - slug is 1+ lowercase `[a-z0-9]` with optional `-._` separators
    ///   - root is 1+ lowercase alphanumeric (legacy or hash)
    /// - hierarchical children: `<slug>-<root>.<n>[.<n>...]`
    pub fn parse(s: &str) -> Result<Self, CoreError> {
        let s = s.trim();
        if s.is_empty() {
            return Err(InvalidId::Bead {
                raw: s.to_string(),
                reason: "empty".into(),
            }
            .into());
        }

        let Some((slug_raw, rest_raw)) = s.rsplit_once('-') else {
            return Err(InvalidId::Bead {
                raw: s.to_string(),
                reason: "must contain '-' separator".into(),
            }
            .into());
        };

        let slug = normalize_bead_slug(slug_raw)?;
        if rest_raw.is_empty() {
            return Err(InvalidId::Bead {
                raw: s.to_string(),
                reason: "missing suffix".into(),
            }
            .into());
        }

        let mut parts = rest_raw.split('.');
        let root_raw = parts.next().unwrap_or("");
        if root_raw.is_empty() {
            return Err(InvalidId::Bead {
                raw: s.to_string(),
                reason: "missing root suffix".into(),
            }
            .into());
        }

        let root = root_raw.to_lowercase();
        for c in root.bytes() {
            if !BEAD_ALPHABET.contains(&c) {
                return Err(InvalidId::Bead {
                    raw: s.to_string(),
                    reason: "contains non-alphanumeric character".into(),
                }
                .into());
            }
        }

        let mut canonical = format!("{}-{}", slug, root);
        for seg in parts {
            if seg.is_empty() || !seg.chars().all(|c| c.is_ascii_digit()) {
                return Err(InvalidId::Bead {
                    raw: s.to_string(),
                    reason: "invalid hierarchical suffix".into(),
                }
                .into());
            }
            canonical.push('.');
            canonical.push_str(seg);
        }

        Ok(Self(canonical))
    }

    /// Generate a new bead ID with given suffix length.
    ///
    /// Only daemon should call this.
    pub(crate) fn generate_with_slug(slug: &str, len: usize) -> Self {
        use rand::Rng;
        assert!(len >= 3, "bead id suffix must be >=3 chars");

        let slug = normalize_bead_slug(slug).expect("internal slug must be valid");
        let mut rng = rand::rng();
        let suffix: String = (0..len)
            .map(|_| {
                let idx = rng.random_range(0..BEAD_ALPHABET.len());
                BEAD_ALPHABET[idx] as char
            })
            .collect();

        Self(format!("{}-{}", slug, suffix))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn slug(&self) -> &str {
        self.0.rfind('-').map(|i| &self.0[..i]).unwrap_or("bd")
    }

    pub fn is_top_level(&self) -> bool {
        self.rest().map(|r| !r.contains('.')).unwrap_or(true)
    }

    pub fn with_slug(&self, slug: &str) -> Result<Self, CoreError> {
        let slug = normalize_bead_slug(slug)?;
        let Some(rest) = self.rest() else {
            return Err(InvalidId::Bead {
                raw: self.0.clone(),
                reason: "missing suffix".into(),
            }
            .into());
        };
        BeadId::parse(&format!("{}-{}", slug, rest))
    }

    fn rest(&self) -> Option<&str> {
        self.0.rsplit_once('-').map(|(_, rest)| rest)
    }

    /// Root suffix length (excluding prefix and any hierarchical children).
    pub fn root_len(&self) -> usize {
        self.rest()
            .and_then(|r| r.split('.').next())
            .map(|s| s.len())
            .unwrap_or(0)
    }
}

fn normalize_bead_slug(raw: &str) -> Result<String, CoreError> {
    let slug = raw.trim().to_lowercase();
    if slug.is_empty() {
        return Err(InvalidId::Bead {
            raw: raw.to_string(),
            reason: "missing slug".into(),
        }
        .into());
    }
    let bytes = slug.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() || !bytes[bytes.len() - 1].is_ascii_alphanumeric() {
        return Err(InvalidId::Bead {
            raw: raw.to_string(),
            reason: "slug must start and end with alphanumeric".into(),
        }
        .into());
    }
    for &b in bytes {
        if !BeadId::SLUG_ALPHABET.contains(&b) {
            return Err(InvalidId::Bead {
                raw: raw.to_string(),
                reason: "slug contains invalid character".into(),
            }
            .into());
        }
    }
    Ok(slug)
}

impl fmt::Debug for BeadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BeadId({:?})", self.0)
    }
}

impl fmt::Display for BeadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Note identifier - unique within a bead.
///
/// Daemon-generated, no specific format required.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NoteId(String);

impl NoteId {
    pub fn new(s: impl Into<String>) -> Result<Self, CoreError> {
        let s = s.into();
        if s.is_empty() {
            Err(InvalidId::Note {
                raw: s,
                reason: "empty".into(),
            }
            .into())
        } else {
            Ok(Self(s))
        }
    }

    /// Generate a new note ID.
    pub(crate) fn generate() -> Self {
        use rand::Rng;
        let mut rng = rand::rng();
        let suffix: String = (0..8)
            .map(|_| {
                let idx = rng.random_range(0..NOTE_ALPHABET.len());
                NOTE_ALPHABET[idx] as char
            })
            .collect();
        Self(suffix)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for NoteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NoteId({:?})", self.0)
    }
}

impl fmt::Display for NoteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Content hash - SHA256 of bead content for CAS.
///
/// Used for optimistic concurrency control.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContentHash([u8; 32]);

impl ContentHash {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// Parse from hex string.
    pub fn from_hex(s: &str) -> Result<Self, CoreError> {
        if s.len() != 64 {
            return Err(InvalidId::ContentHash {
                raw: s.to_string(),
                reason: format!("must be 64 hex chars (got {})", s.len()),
            }
            .into());
        }
        let mut bytes = [0u8; 32];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            let hex = std::str::from_utf8(chunk).map_err(|_| InvalidId::ContentHash {
                raw: s.to_string(),
                reason: "contains invalid UTF-8".into(),
            })?;
            bytes[i] = u8::from_str_radix(hex, 16).map_err(|_| InvalidId::ContentHash {
                raw: s.to_string(),
                reason: format!("contains invalid hex: {}", hex),
            })?;
        }
        Ok(Self(bytes))
    }
}

impl fmt::Debug for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentHash({})", self.to_hex())
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl serde::Serialize for ContentHash {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> serde::Deserialize<'de> for ContentHash {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        ContentHash::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

/// Git branch name - non-empty string.
///
/// Provides minimal validation to catch obviously invalid branch names.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct BranchName(String);

impl BranchName {
    /// Parse and validate a branch name.
    pub fn parse(s: impl Into<String>) -> Result<Self, CoreError> {
        let s = s.into().trim().to_string();
        if s.is_empty() {
            return Err(InvalidId::Branch {
                raw: s,
                reason: "empty".into(),
            }
            .into());
        }
        // Basic validation: no newlines, no spaces (git branches can't have spaces)
        if s.contains('\n') || s.contains('\r') || s.contains(' ') {
            return Err(InvalidId::Branch {
                raw: s,
                reason: "cannot contain newlines or spaces".into(),
            }
            .into());
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BranchName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BranchName({:?})", self.0)
    }
}

impl fmt::Display for BranchName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for BranchName {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        BranchName::parse(s)
    }
}

impl From<BranchName> for String {
    fn from(b: BranchName) -> String {
        b.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_name_parse_valid() {
        let name = BranchName::parse("main").unwrap();
        assert_eq!(name.as_str(), "main");

        let name = BranchName::parse("feature/add-thing").unwrap();
        assert_eq!(name.as_str(), "feature/add-thing");
    }

    #[test]
    fn branch_name_trims() {
        let name = BranchName::parse("  main  ").unwrap();
        assert_eq!(name.as_str(), "main");
    }

    #[test]
    fn branch_name_rejects_empty() {
        assert!(BranchName::parse("").is_err());
        assert!(BranchName::parse("   ").is_err());
    }

    #[test]
    fn branch_name_rejects_invalid() {
        assert!(BranchName::parse("main branch").is_err());
        assert!(BranchName::parse("main\nbranch").is_err());
    }

    #[test]
    fn bead_id_parse_with_custom_slug() {
        let id = BeadId::parse("beads-rs-abc1").unwrap();
        assert_eq!(id.as_str(), "beads-rs-abc1");
        assert_eq!(id.slug(), "beads-rs");
        assert!(id.is_top_level());

        let child = BeadId::parse("beads-rs-epic.12.3").unwrap();
        assert_eq!(child.slug(), "beads-rs");
        assert!(!child.is_top_level());
    }

    #[test]
    fn bead_id_slug_is_canonicalized() {
        let id = BeadId::parse("BeAds-Rs-abc1").unwrap();
        assert_eq!(id.as_str(), "beads-rs-abc1");
    }

    #[test]
    fn bead_id_with_slug_rewrites_prefix() {
        let id = BeadId::parse("beads-rs-abc1.2").unwrap();
        let rewritten = id.with_slug("other-repo").unwrap();
        assert_eq!(rewritten.as_str(), "other-repo-abc1.2");
    }
}
