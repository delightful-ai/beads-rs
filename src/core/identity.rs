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

/// Bead identifier - "bd-{suffix}" format.
///
/// Suffix is lowercase alphanumeric. Hierarchical children append `.N`.
/// Only daemon generates new IDs (pub(crate)).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BeadId(String);

impl BeadId {
    /// Parse and validate a bead ID string.
    ///
    /// Accepted forms:
    /// - `bd-<root>` where root is 1+ lowercase alphanumeric (legacy or hash)
    /// - hierarchical children: `bd-<root>.<n>[.<n>...]`
    pub fn parse(s: &str) -> Result<Self, CoreError> {
        if !s.starts_with("bd-") {
            return Err(InvalidId::Bead {
                raw: s.to_string(),
                reason: "must start with 'bd-'".into(),
            }
            .into());
        }

        let rest = &s[3..];
        if rest.is_empty() {
            return Err(InvalidId::Bead {
                raw: s.to_string(),
                reason: "missing suffix".into(),
            }
            .into());
        }

        let mut parts = rest.split('.');
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

        let mut canonical = format!("bd-{}", root);
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
    pub(crate) fn generate(len: usize) -> Self {
        use rand::Rng;
        assert!(len >= 3, "bead id suffix must be >=3 chars");

        let mut rng = rand::rng();
        let suffix: String = (0..len)
            .map(|_| {
                let idx = rng.random_range(0..BEAD_ALPHABET.len());
                BEAD_ALPHABET[idx] as char
            })
            .collect();

        Self(format!("bd-{}", suffix))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Root suffix length (excluding prefix and any hierarchical children).
    pub fn root_len(&self) -> usize {
        self.0[3..]
            .split('.')
            .next()
            .map(|s| s.len())
            .unwrap_or(0)
    }
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
            let hex = std::str::from_utf8(chunk).map_err(|_| {
                InvalidId::ContentHash {
                    raw: s.to_string(),
                    reason: "contains invalid UTF-8".into(),
                }
            })?;
            bytes[i] = u8::from_str_radix(hex, 16).map_err(|_| {
                InvalidId::ContentHash {
                    raw: s.to_string(),
                    reason: format!("contains invalid hex: {}", hex),
                }
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
}
