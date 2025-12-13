//! Layer 4: Canonical collections
//!
//! Label: validated label string (non-empty, no newlines)
//! Labels: sorted set of Label values
//! NoteLog: notes keyed by id, sorted output by (stamp, id)

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};

use super::composite::Note;
use super::identity::NoteId;
use super::error::{CoreError, InvalidLabel};

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

/// Notes keyed by ID - enforces uniqueness, sorted output.
///
/// Merge semantics: union by ID (notes are immutable).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NoteLog(BTreeMap<NoteId, Note>);

impl NoteLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, note: Note) {
        self.0.insert(note.id.clone(), note);
    }

    pub fn get(&self, id: &NoteId) -> Option<&Note> {
        self.0.get(id)
    }

    pub fn contains(&self, id: &NoteId) -> bool {
        self.0.contains_key(id)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Merge = union by ID.
    ///
    /// Notes are immutable, so if same ID exists in both, they should be identical.
    /// We keep left's version in case of mismatch (shouldn't happen in practice).
    pub fn join(a: &Self, b: &Self) -> Self {
        let mut merged = a.0.clone();
        for (id, note) in &b.0 {
            merged.entry(id.clone()).or_insert_with(|| note.clone());
        }
        Self(merged)
    }

    /// Principal form: sorted by (at, id) for stable output.
    pub fn sorted(&self) -> Vec<&Note> {
        let mut v: Vec<_> = self.0.values().collect();
        v.sort_by(|a, b| a.at.cmp(&b.at).then_with(|| a.id.cmp(&b.id)));
        v
    }

    pub fn iter(&self) -> impl Iterator<Item = (&NoteId, &Note)> {
        self.0.iter()
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
