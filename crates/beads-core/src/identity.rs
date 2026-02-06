//! Layer 1: Identity atoms
//!
//! ActorId: agent self-identification
//! BeadId: issue identifier with prefix
//! NoteId: note identifier within a bead

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::event::sha256_bytes;
use crate::json_canon::{CanonJsonError, to_canon_json_bytes};
use crate::state::CanonicalState;

use super::error::{CoreError, InvalidId};
use super::{NamespaceId, Seq1};

/// Actor identifier - non-empty string after trimming.
///
/// Agents name themselves. Validation only rejects empty/whitespace-only values.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ActorId(String);

impl ActorId {
    pub fn new(s: impl Into<String>) -> Result<Self, CoreError> {
        let s = s.into();
        if s.trim().is_empty() {
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

    #[cfg(test)]
    pub fn new_unchecked(s: impl Into<String>) -> Self {
        Self(s.into())
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

impl TryFrom<String> for ActorId {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        ActorId::new(s)
    }
}

impl From<ActorId> for String {
    fn from(id: ActorId) -> String {
        id.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreId(Uuid);

impl StoreId {
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    pub fn parse_str(s: &str) -> Result<Self, CoreError> {
        parse_uuid_id(s, |raw, reason| InvalidId::StoreId { raw, reason }).map(Self)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for StoreId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StoreId({})", self.0)
    }
}

impl fmt::Display for StoreId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for StoreId {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        StoreId::parse_str(&s)
    }
}

impl From<Uuid> for StoreId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

impl From<StoreId> for Uuid {
    fn from(id: StoreId) -> Uuid {
        id.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreEpoch(u64);

impl StoreEpoch {
    pub const ZERO: StoreEpoch = StoreEpoch(0);

    pub fn new(epoch: u64) -> Self {
        Self(epoch)
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

impl fmt::Debug for StoreEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StoreEpoch({})", self.0)
    }
}

impl fmt::Display for StoreEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for StoreEpoch {
    fn from(epoch: u64) -> Self {
        Self(epoch)
    }
}

impl From<StoreEpoch> for u64 {
    fn from(epoch: StoreEpoch) -> u64 {
        epoch.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StoreIdentity {
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
}

impl StoreIdentity {
    pub fn new(store_id: StoreId, store_epoch: StoreEpoch) -> Self {
        Self {
            store_id,
            store_epoch,
        }
    }
}

impl fmt::Debug for StoreIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StoreIdentity({}, {})", self.store_id, self.store_epoch)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReplicaId(Uuid);

impl ReplicaId {
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    pub fn parse_str(s: &str) -> Result<Self, CoreError> {
        parse_uuid_id(s, |raw, reason| InvalidId::ReplicaId { raw, reason }).map(Self)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for ReplicaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ReplicaId({})", self.0)
    }
}

impl fmt::Display for ReplicaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for ReplicaId {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        ReplicaId::parse_str(&s)
    }
}

impl From<Uuid> for ReplicaId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

impl From<ReplicaId> for Uuid {
    fn from(id: ReplicaId) -> Uuid {
        id.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventId {
    pub origin_replica_id: ReplicaId,
    pub namespace: NamespaceId,
    pub origin_seq: Seq1,
}

impl EventId {
    pub fn new(origin_replica_id: ReplicaId, namespace: NamespaceId, origin_seq: Seq1) -> Self {
        Self {
            origin_replica_id,
            namespace,
            origin_seq,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TxnId(Uuid);

impl TxnId {
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    pub fn parse_str(s: &str) -> Result<Self, CoreError> {
        parse_uuid_id(s, |raw, reason| InvalidId::TxnId { raw, reason }).map(Self)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxnId({})", self.0)
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for TxnId {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        TxnId::parse_str(&s)
    }
}

impl From<Uuid> for TxnId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

impl From<TxnId> for Uuid {
    fn from(id: TxnId) -> Uuid {
        id.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClientRequestId(Uuid);

impl ClientRequestId {
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    pub fn parse_str(s: &str) -> Result<Self, CoreError> {
        parse_uuid_id(s, |raw, reason| InvalidId::ClientRequestId { raw, reason }).map(Self)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for ClientRequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClientRequestId({})", self.0)
    }
}

impl fmt::Display for ClientRequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for ClientRequestId {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        ClientRequestId::parse_str(&s)
    }
}

impl From<Uuid> for ClientRequestId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

impl From<ClientRequestId> for Uuid {
    fn from(id: ClientRequestId) -> Uuid {
        id.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TraceId(Uuid);

impl TraceId {
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    pub fn parse_str(s: &str) -> Result<Self, CoreError> {
        parse_uuid_id(s, |raw, reason| InvalidId::TraceId { raw, reason }).map(Self)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TraceId({})", self.0)
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for TraceId {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        TraceId::parse_str(&s)
    }
}

impl From<Uuid> for TraceId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

impl From<TraceId> for Uuid {
    fn from(id: TraceId) -> Uuid {
        id.0
    }
}

impl From<ClientRequestId> for TraceId {
    fn from(id: ClientRequestId) -> Self {
        Self(Uuid::from(id))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SegmentId(Uuid);

impl SegmentId {
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    pub fn parse_str(s: &str) -> Result<Self, CoreError> {
        parse_uuid_id(s, |raw, reason| InvalidId::SegmentId { raw, reason }).map(Self)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SegmentId({})", self.0)
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for SegmentId {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        SegmentId::parse_str(&s)
    }
}

impl From<Uuid> for SegmentId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

impl From<SegmentId> for Uuid {
    fn from(id: SegmentId) -> Uuid {
        id.0
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
#[serde(try_from = "String", into = "String")]
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
    pub fn generate_with_slug(slug: &BeadSlug, len: usize) -> Self {
        use rand::Rng;
        assert!(len >= 3, "bead id suffix must be >=3 chars");

        let mut rng = rand::rng();
        let suffix: String = (0..len)
            .map(|_| {
                let idx = rng.random_range(0..BEAD_ALPHABET.len());
                BEAD_ALPHABET[idx] as char
            })
            .collect();

        Self(format!("{}-{}", slug.as_str(), suffix))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn slug(&self) -> &str {
        self.0.rfind('-').map(|i| &self.0[..i]).unwrap_or("bd")
    }

    /// Return a strongly-typed slug derived from this ID.
    pub fn slug_value(&self) -> BeadSlug {
        BeadSlug(self.slug().to_string())
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

/// Bead slug (validated).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct BeadSlug(String);

impl BeadSlug {
    pub fn parse(raw: &str) -> Result<Self, CoreError> {
        Ok(Self(normalize_bead_slug(raw)?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BeadSlug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BeadSlug({:?})", self.0)
    }
}

impl fmt::Display for BeadSlug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for BeadSlug {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        BeadSlug::parse(&s)
    }
}

impl From<BeadSlug> for String {
    fn from(slug: BeadSlug) -> String {
        slug.0
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

impl TryFrom<String> for BeadId {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        BeadId::parse(&s)
    }
}

impl From<BeadId> for String {
    fn from(id: BeadId) -> String {
        id.0
    }
}

/// Note identifier - unique within a bead.
///
/// Daemon-generated, no specific format required.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
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
    pub fn generate() -> Self {
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

impl TryFrom<String> for NoteId {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        NoteId::new(s)
    }
}

impl From<NoteId> for String {
    fn from(id: NoteId) -> String {
        id.0
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

/// SHA256 of JSONL state representation (state/tombstones/deps/notes).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StateJsonlSha256(ContentHash);

impl StateJsonlSha256 {
    pub fn from_hex(s: &str) -> Result<Self, CoreError> {
        ContentHash::from_hex(s).map(Self)
    }

    pub fn from_jsonl_bytes(bytes: &[u8]) -> Self {
        Self(ContentHash::from_bytes(sha256_bytes(bytes).0))
    }

    pub fn as_content_hash(&self) -> &ContentHash {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        self.0.to_hex()
    }
}

impl fmt::Debug for StateJsonlSha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StateJsonlSha256({})", self.to_hex())
    }
}

impl fmt::Display for StateJsonlSha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// SHA256 of canonical JSON representation of CanonicalState.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StateCanonicalJsonSha256(ContentHash);

impl StateCanonicalJsonSha256 {
    pub fn from_hex(s: &str) -> Result<Self, CoreError> {
        ContentHash::from_hex(s).map(Self)
    }

    pub fn from_canonical_json_bytes(bytes: &[u8]) -> Self {
        Self(ContentHash::from_bytes(sha256_bytes(bytes).0))
    }

    pub fn from_canonical_state(state: &CanonicalState) -> Result<Self, CanonJsonError> {
        let bytes = to_canon_json_bytes(state)?;
        Ok(Self::from_canonical_json_bytes(&bytes))
    }

    pub fn as_content_hash(&self) -> &ContentHash {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        self.0.to_hex()
    }
}

impl fmt::Debug for StateCanonicalJsonSha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StateCanonicalJsonSha256({})", self.to_hex())
    }
}

impl fmt::Display for StateCanonicalJsonSha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// SHA256 of checkpoint meta preimage canonical JSON.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckpointContentSha256(ContentHash);

impl CheckpointContentSha256 {
    pub fn from_hex(s: &str) -> Result<Self, CoreError> {
        ContentHash::from_hex(s).map(Self)
    }

    pub fn from_checkpoint_preimage_bytes(bytes: &[u8]) -> Self {
        Self(ContentHash::from_bytes(sha256_bytes(bytes).0))
    }

    pub fn as_content_hash(&self) -> &ContentHash {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        self.0.to_hex()
    }
}

impl fmt::Debug for CheckpointContentSha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CheckpointContentSha256({})", self.to_hex())
    }
}

impl fmt::Display for CheckpointContentSha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StateDigest {
    JsonlSha256(StateJsonlSha256),
    CanonicalJsonSha256(StateCanonicalJsonSha256),
    CheckpointContentSha256(CheckpointContentSha256),
}

impl StateDigest {
    pub fn jsonl_sha256(bytes: &[u8]) -> Self {
        StateDigest::JsonlSha256(StateJsonlSha256::from_jsonl_bytes(bytes))
    }

    pub fn canonical_json_sha256(bytes: &[u8]) -> Self {
        StateDigest::CanonicalJsonSha256(StateCanonicalJsonSha256::from_canonical_json_bytes(bytes))
    }

    pub fn checkpoint_content_sha256(bytes: &[u8]) -> Self {
        StateDigest::CheckpointContentSha256(CheckpointContentSha256::from_checkpoint_preimage_bytes(
            bytes,
        ))
    }

    pub fn to_hex(&self) -> String {
        match self {
            StateDigest::JsonlSha256(digest) => digest.to_hex(),
            StateDigest::CanonicalJsonSha256(digest) => digest.to_hex(),
            StateDigest::CheckpointContentSha256(digest) => digest.to_hex(),
        }
    }
}

impl From<StateJsonlSha256> for StateDigest {
    fn from(digest: StateJsonlSha256) -> Self {
        StateDigest::JsonlSha256(digest)
    }
}

impl From<StateCanonicalJsonSha256> for StateDigest {
    fn from(digest: StateCanonicalJsonSha256) -> Self {
        StateDigest::CanonicalJsonSha256(digest)
    }
}

impl From<CheckpointContentSha256> for StateDigest {
    fn from(digest: CheckpointContentSha256) -> Self {
        StateDigest::CheckpointContentSha256(digest)
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

fn parse_uuid_id<F>(raw: &str, invalid: F) -> Result<Uuid, CoreError>
where
    F: FnOnce(String, String) -> InvalidId,
{
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(invalid(raw.to_string(), "empty".into()).into());
    }
    Uuid::parse_str(trimmed).map_err(|err| invalid(raw.to_string(), err.to_string()).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_canon::to_canon_json_bytes;
    use serde::de::DeserializeOwned;

    fn roundtrip<T>(value: &T)
    where
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let json = serde_json::to_string(value).unwrap();
        let parsed: T = serde_json::from_str(&json).unwrap();
        assert_eq!(value, &parsed);
    }

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
    fn bead_slug_parse_normalizes() {
        let slug = BeadSlug::parse("  BeAds-Rs  ").unwrap();
        assert_eq!(slug.as_str(), "beads-rs");
    }

    #[test]
    fn bead_slug_rejects_invalid() {
        assert!(BeadSlug::parse("bad slug").is_err());
        assert!(BeadSlug::parse("-bad").is_err());
    }

    #[test]
    fn bead_id_with_slug_rewrites_prefix() {
        let id = BeadId::parse("beads-rs-abc1.2").unwrap();
        let rewritten = id.with_slug("other-repo").unwrap();
        assert_eq!(rewritten.as_str(), "other-repo-abc1.2");
    }

    // Serde boundary validation tests
    #[test]
    fn actor_id_serde_validates_on_deserialize() {
        // Valid
        let json = r#""some-actor""#;
        let actor: ActorId = serde_json::from_str(json).unwrap();
        assert_eq!(actor.as_str(), "some-actor");

        // Invalid: empty
        let json = r#""""#;
        let err = serde_json::from_str::<ActorId>(json).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn bead_id_serde_validates_on_deserialize() {
        // Valid
        let json = r#""bd-abc123""#;
        let id: BeadId = serde_json::from_str(json).unwrap();
        assert_eq!(id.as_str(), "bd-abc123");

        // Invalid: missing separator
        let json = r#""invalid""#;
        let err = serde_json::from_str::<BeadId>(json).unwrap_err();
        assert!(err.to_string().contains("separator"));

        // Invalid: empty
        let json = r#""""#;
        let err = serde_json::from_str::<BeadId>(json).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn note_id_serde_validates_on_deserialize() {
        // Valid
        let json = r#""note123""#;
        let id: NoteId = serde_json::from_str(json).unwrap();
        assert_eq!(id.as_str(), "note123");

        // Invalid: empty
        let json = r#""""#;
        let err = serde_json::from_str::<NoteId>(json).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn identity_types_serde_roundtrip() {
        let actor = ActorId::new("test-actor").unwrap();
        let json = serde_json::to_string(&actor).unwrap();
        let back: ActorId = serde_json::from_str(&json).unwrap();
        assert_eq!(actor, back);

        let bead = BeadId::parse("bd-xyz").unwrap();
        let json = serde_json::to_string(&bead).unwrap();
        let back: BeadId = serde_json::from_str(&json).unwrap();
        assert_eq!(bead, back);

        let note = NoteId::new("note-id").unwrap();
        let json = serde_json::to_string(&note).unwrap();
        let back: NoteId = serde_json::from_str(&json).unwrap();
        assert_eq!(note, back);
    }

    #[test]
    fn actor_id_rejects_whitespace_only() {
        let err = ActorId::new("   ").expect_err("whitespace actor id must fail");
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn actor_id_serde_rejects_whitespace_only() {
        let err =
            serde_json::from_str::<ActorId>(r#""   ""#).expect_err("whitespace actor id must fail");
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn store_id_serde_roundtrip() {
        let id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        roundtrip(&id);
    }

    #[test]
    fn store_epoch_serde_roundtrip() {
        let epoch = StoreEpoch::new(42);
        roundtrip(&epoch);
    }

    #[test]
    fn store_identity_serde_roundtrip() {
        let id = StoreId::new(Uuid::from_bytes([2u8; 16]));
        let identity = StoreIdentity::new(id, StoreEpoch::new(7));
        roundtrip(&identity);
    }

    #[test]
    fn replica_id_serde_roundtrip() {
        let id = ReplicaId::new(Uuid::from_bytes([3u8; 16]));
        roundtrip(&id);
    }

    #[test]
    fn txn_id_serde_roundtrip() {
        let id = TxnId::new(Uuid::from_bytes([4u8; 16]));
        roundtrip(&id);
    }

    #[test]
    fn client_request_id_serde_roundtrip() {
        let id = ClientRequestId::new(Uuid::from_bytes([5u8; 16]));
        roundtrip(&id);
    }

    #[test]
    fn trace_id_serde_roundtrip() {
        let id = TraceId::new(Uuid::from_bytes([6u8; 16]));
        roundtrip(&id);
    }

    #[test]
    fn segment_id_serde_roundtrip() {
        let id = SegmentId::new(Uuid::from_bytes([7u8; 16]));
        roundtrip(&id);
    }

    #[test]
    fn state_jsonl_digest_is_stable() {
        let digest = StateJsonlSha256::from_jsonl_bytes(b"state.jsonl\n");
        assert_eq!(
            digest.to_hex(),
            "14a69ad0a1e52c12f352264102871a7f75ced4fe94dea34a3b85dc7dde437706"
        );
    }

    #[test]
    fn state_canonical_json_digest_is_stable() {
        let digest = StateCanonicalJsonSha256::from_canonical_json_bytes(b"{\"state\":{}}");
        assert_eq!(
            digest.to_hex(),
            "f5ff958086348e8a6780cce4a1daf3d2a4037c95bb6e0bb5bb2eda93ccc9e5a1"
        );
    }

    #[test]
    fn checkpoint_content_digest_is_stable() {
        let digest =
            CheckpointContentSha256::from_checkpoint_preimage_bytes(b"{\"checkpoint\":\"meta\"}");
        assert_eq!(
            digest.to_hex(),
            "0b5009d2e29a22aee5ac1913547fd5525ff45127dd687f65d27e529397df78c2"
        );
    }

    #[test]
    fn canonical_state_digest_matches_canon_bytes() {
        let state = CanonicalState::new();
        let bytes = to_canon_json_bytes(&state).expect("canonical json bytes");
        let via_state =
            StateCanonicalJsonSha256::from_canonical_state(&state).expect("state digest");
        let via_bytes = StateCanonicalJsonSha256::from_canonical_json_bytes(&bytes);
        assert_eq!(via_state, via_bytes);
    }
}
