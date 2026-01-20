//! Layer 4: Canonical collections
//!
//! Label: validated label string (non-empty, no newlines)
//! Labels: sorted set of Label values

use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};

use super::error::{CoreError, InvalidLabel};
use super::orset::OrSetValue;

/// Validated label - non-empty, no newlines.
///
/// Labels are trimmed on parse. Empty or newline-containing labels are rejected.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Label(String);

impl Label {
    /// Parse and validate a label string.
    pub fn parse(s: impl Into<String>) -> Result<Self, CoreError> {
        let s = s.into().trim().to_string();
        if s.is_empty() {
            return Err(InvalidLabel {
                raw: s,
                reason: "empty".into(),
            }
            .into());
        }
        if s.contains('\n') || s.contains('\r') {
            return Err(InvalidLabel {
                raw: s,
                reason: "cannot contain newlines".into(),
            }
            .into());
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Label({:?})", self.0)
    }
}

impl fmt::Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for Label {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Label::parse(s)
    }
}

impl From<Label> for String {
    fn from(l: Label) -> String {
        l.0
    }
}

impl OrSetValue for Label {
    fn collision_bytes(&self) -> Vec<u8> {
        self.as_str().as_bytes().to_vec()
    }
}

/// Labels as sorted set - principal form by construction.
///
/// BTreeSet ensures sorted, deduplicated output.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Labels(BTreeSet<Label>);

impl Labels {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, label: Label) {
        self.0.insert(label);
    }

    pub fn remove(&mut self, label: &str) -> bool {
        // Find and remove by string value
        self.0.retain(|l| l.as_str() != label);
        true // BTreeSet doesn't tell us if it changed, but retain always succeeds
    }

    pub fn contains(&self, label: &str) -> bool {
        self.0.iter().any(|l| l.as_str() == label)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Label> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<Label> for Labels {
    fn from_iter<T: IntoIterator<Item = Label>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_parse_valid() {
        let label = Label::parse("bug").unwrap();
        assert_eq!(label.as_str(), "bug");
    }

    #[test]
    fn label_trims_whitespace() {
        let label = Label::parse("  bug  ").unwrap();
        assert_eq!(label.as_str(), "bug");
    }

    #[test]
    fn label_rejects_empty() {
        assert!(Label::parse("").is_err());
        assert!(Label::parse("   ").is_err());
    }

    #[test]
    fn label_rejects_newlines() {
        assert!(Label::parse("bug\nfix").is_err());
        assert!(Label::parse("bug\rfix").is_err());
    }

    #[test]
    fn labels_serde_roundtrip() {
        let mut labels = Labels::new();
        labels.insert(Label::parse("bug").unwrap());
        labels.insert(Label::parse("feature").unwrap());

        let json = serde_json::to_string(&labels).unwrap();
        let parsed: Labels = serde_json::from_str(&json).unwrap();

        assert!(parsed.contains("bug"));
        assert!(parsed.contains("feature"));
    }
}
