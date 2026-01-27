//! Applied and durable watermark tracking.

use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{NamespaceId, ReplicaId};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Applied;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Durable;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Seq0(u64);

impl Seq0 {
    pub const ZERO: Seq0 = Seq0(0);

    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Seq1 {
        let next = self
            .0
            .checked_add(1)
            .expect("seq0 overflow computing next seq1");
        Seq1(NonZeroU64::new(next).expect("seq1 cannot be zero"))
    }
}

impl fmt::Debug for Seq0 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Seq0({})", self.0)
    }
}

impl fmt::Display for Seq0 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Seq0> for u64 {
    fn from(value: Seq0) -> u64 {
        value.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Seq1(NonZeroU64);

impl Seq1 {
    pub fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    pub fn from_u64(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }

    pub fn next(self) -> Seq1 {
        let next = self
            .0
            .get()
            .checked_add(1)
            .expect("seq1 overflow computing next");
        Seq1(NonZeroU64::new(next).expect("seq1 cannot be zero"))
    }

    pub fn prev(self) -> Option<Seq1> {
        self.0
            .get()
            .checked_sub(1)
            .and_then(NonZeroU64::new)
            .map(Seq1)
    }

    pub fn prev_seq0(self) -> Seq0 {
        Seq0(self.0.get() - 1)
    }
}

impl fmt::Debug for Seq1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Seq1({})", self.0)
    }
}

impl fmt::Display for Seq1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Seq1> for u64 {
    fn from(value: Seq1) -> u64 {
        value.0.get()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HeadStatus {
    Genesis,
    Known([u8; 32]),
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Watermark<K> {
    seq: Seq0,
    head: HeadStatus,
    _kind: PhantomData<K>,
}

impl<K> Copy for Watermark<K> {}

impl<K> Clone for Watermark<K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K> Watermark<K> {
    pub fn new(seq: Seq0, head: HeadStatus) -> Result<Self, WatermarkError> {
        validate_head(seq, head)?;
        Ok(Self {
            seq,
            head,
            _kind: PhantomData,
        })
    }

    pub fn genesis() -> Self {
        Self {
            seq: Seq0::ZERO,
            head: HeadStatus::Genesis,
            _kind: PhantomData,
        }
    }

    pub fn seq(self) -> Seq0 {
        self.seq
    }

    pub fn head(self) -> HeadStatus {
        self.head
    }
}

impl<K> Serialize for Watermark<K> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let wire = WatermarkWire {
            seq: self.seq,
            head: self.head,
        };
        wire.serialize(serializer)
    }
}

impl<'de, K> Deserialize<'de> for Watermark<K> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = WatermarkWire::deserialize(deserializer)?;
        Watermark::new(wire.seq, wire.head).map_err(serde::de::Error::custom)
    }
}

impl<K> Default for Watermark<K> {
    fn default() -> Self {
        Self::genesis()
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WatermarkError {
    #[error("expected contiguous seq {expected}, got {got}")]
    NonContiguous { expected: Seq1, got: Seq1 },
    #[error("head hash required for seq {seq}")]
    MissingHead { seq: Seq0 },
    #[error("head hash must be absent at genesis (seq {seq})")]
    UnexpectedHead { seq: Seq0 },
    #[error("head mismatch for seq {seq}: {left:?} vs {right:?}")]
    HeadMismatch {
        seq: Seq0,
        left: HeadStatus,
        right: HeadStatus,
    },
}

fn validate_head(seq: Seq0, head: HeadStatus) -> Result<(), WatermarkError> {
    if seq.get() == 0 {
        return match head {
            HeadStatus::Genesis => Ok(()),
            _ => Err(WatermarkError::UnexpectedHead { seq }),
        };
    }

    match head {
        HeadStatus::Known(_) => Ok(()),
        _ => Err(WatermarkError::MissingHead { seq }),
    }
}

fn head_key(head: HeadStatus) -> (u8, [u8; 32]) {
    match head {
        HeadStatus::Genesis => (0, [0u8; 32]),
        HeadStatus::Known(bytes) => (1, bytes),
    }
}

fn canonicalize_heads(a: HeadStatus, b: HeadStatus) -> (HeadStatus, HeadStatus) {
    if head_key(a) <= head_key(b) {
        (a, b)
    } else {
        (b, a)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Watermarks<K> {
    inner: BTreeMap<NamespaceId, BTreeMap<ReplicaId, Watermark<K>>>,
}

impl<K> Watermarks<K> {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    pub fn namespaces(&self) -> impl Iterator<Item = &NamespaceId> {
        self.inner.keys()
    }

    pub fn get(&self, namespace: &NamespaceId, origin: &ReplicaId) -> Option<&Watermark<K>> {
        self.inner
            .get(namespace)
            .and_then(|origins| origins.get(origin))
    }

    pub fn origins(
        &self,
        namespace: &NamespaceId,
    ) -> impl Iterator<Item = (&ReplicaId, &Watermark<K>)> {
        self.inner
            .get(namespace)
            .into_iter()
            .flat_map(|origins| origins.iter())
    }

    pub fn advance_contiguous(
        &mut self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        next: Seq1,
        head: [u8; 32],
    ) -> Result<(), WatermarkError> {
        let current = self
            .get(namespace, origin)
            .copied()
            .unwrap_or_else(Watermark::genesis);
        let expected = current.seq().next();
        if next != expected {
            return Err(WatermarkError::NonContiguous {
                expected,
                got: next,
            });
        }

        let updated = Watermark::new(Seq0::new(next.get()), HeadStatus::Known(head))?;
        *self.entry_mut(namespace, origin) = updated;
        Ok(())
    }

    pub fn observe_at_least(
        &mut self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        seq: Seq0,
        head: HeadStatus,
    ) -> Result<(), WatermarkError> {
        let current = self
            .get(namespace, origin)
            .copied()
            .unwrap_or_else(Watermark::genesis);

        if seq < current.seq() {
            return Ok(());
        }

        if seq == current.seq() {
            validate_head(seq, head)?;
            if head != current.head() {
                let (left, right) = canonicalize_heads(current.head(), head);
                return Err(WatermarkError::HeadMismatch { seq, left, right });
            }
            return Ok(());
        }

        let updated = Watermark::new(seq, head)?;
        *self.entry_mut(namespace, origin) = updated;
        Ok(())
    }

    pub fn merge_at_least(&mut self, other: &Watermarks<K>) -> Result<(), WatermarkError> {
        for (namespace, origins) in &other.inner {
            for (origin, watermark) in origins {
                self.observe_at_least(namespace, origin, watermark.seq(), watermark.head())?;
            }
        }
        Ok(())
    }

    pub fn satisfies_at_least(&self, required: &Watermarks<K>) -> bool {
        for (namespace, origins) in &required.inner {
            for (origin, watermark) in origins {
                let current = self
                    .get(namespace, origin)
                    .copied()
                    .unwrap_or_else(Watermark::genesis);
                if current.seq() < watermark.seq() {
                    return false;
                }
            }
        }
        true
    }

    fn entry_mut(&mut self, namespace: &NamespaceId, origin: &ReplicaId) -> &mut Watermark<K> {
        self.inner
            .entry(namespace.clone())
            .or_default()
            .entry(*origin)
            .or_default()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WatermarkWire {
    seq: Seq0,
    head: HeadStatus,
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn seq_helpers_work() {
        let seq0 = Seq0::new(0);
        let seq1 = seq0.next();
        assert_eq!(seq1.get(), 1);
        assert_eq!(seq1.prev_seq0().get(), 0);
        assert!(seq1.prev().is_none());

        let seq2 = Seq1::from_u64(2).unwrap();
        assert_eq!(seq2.prev().unwrap().get(), 1);
        assert_eq!(seq2.next().get(), 3);
    }

    #[test]
    fn watermark_rejects_genesis_for_nonzero_seq() {
        let seq = Seq0::new(1);
        let err = Watermark::<Applied>::new(seq, HeadStatus::Genesis).unwrap_err();
        assert_eq!(err, WatermarkError::MissingHead { seq });
    }

    #[test]
    fn watermark_rejects_head_at_genesis() {
        let seq = Seq0::ZERO;
        let err = Watermark::<Applied>::new(seq, HeadStatus::Known([1u8; 32])).unwrap_err();
        assert_eq!(err, WatermarkError::UnexpectedHead { seq });
    }

    #[test]
    fn watermark_rejects_unknown_on_deserialize() {
        let raw = r#"{"seq":2,"head":"Unknown"}"#;
        let err = serde_json::from_str::<Watermark<Applied>>(raw).unwrap_err();
        assert!(err.to_string().contains("unknown variant"));
    }

    #[test]
    fn advance_contiguous_rejects_gaps() {
        let mut watermarks = Watermarks::<Applied>::new();
        let ns = NamespaceId::parse("core").unwrap();
        let origin = ReplicaId::new(Uuid::from_bytes([9u8; 16]));
        let next = Seq1::from_u64(2).unwrap();
        let err = watermarks
            .advance_contiguous(&ns, &origin, next, [0u8; 32])
            .unwrap_err();
        assert!(matches!(err, WatermarkError::NonContiguous { .. }));
    }

    #[test]
    fn advance_contiguous_sets_seq0_to_seq1() {
        let mut watermarks = Watermarks::<Applied>::new();
        let ns = NamespaceId::parse("core").unwrap();
        let origin = ReplicaId::new(Uuid::from_bytes([10u8; 16]));
        let next = Seq1::from_u64(1).unwrap();

        watermarks
            .advance_contiguous(&ns, &origin, next, [3u8; 32])
            .unwrap();

        let watermark = watermarks.get(&ns, &origin).expect("watermark");
        assert_eq!(watermark.seq().get(), next.get());
        assert!(matches!(watermark.head(), HeadStatus::Known(_)));
    }

    #[test]
    fn observe_at_least_rejects_head_mismatch_same_seq() {
        let mut watermarks = Watermarks::<Applied>::new();
        let ns = NamespaceId::parse("core").unwrap();
        let origin = ReplicaId::new(Uuid::from_bytes([11u8; 16]));
        let seq = Seq0::new(2);
        let head_a = HeadStatus::Known([1u8; 32]);
        let head_b = HeadStatus::Known([2u8; 32]);

        watermarks
            .observe_at_least(&ns, &origin, seq, head_a)
            .unwrap();
        let err = watermarks
            .observe_at_least(&ns, &origin, seq, head_b)
            .unwrap_err();

        let (left, right) = canonicalize_heads(head_a, head_b);
        assert_eq!(err, WatermarkError::HeadMismatch { seq, left, right });
    }

    #[test]
    fn merge_at_least_head_mismatch_is_order_independent() {
        let ns = NamespaceId::parse("core").unwrap();
        let origin = ReplicaId::new(Uuid::from_bytes([12u8; 16]));
        let seq = Seq0::new(4);

        let mut a = Watermarks::<Applied>::new();
        a.observe_at_least(&ns, &origin, seq, HeadStatus::Known([3u8; 32]))
            .unwrap();

        let mut b = Watermarks::<Applied>::new();
        b.observe_at_least(&ns, &origin, seq, HeadStatus::Known([9u8; 32]))
            .unwrap();

        let mut ab = a.clone();
        let err_ab = ab.merge_at_least(&b).unwrap_err();
        let mut ba = b.clone();
        let err_ba = ba.merge_at_least(&a).unwrap_err();

        assert_eq!(err_ab, err_ba);
    }

    #[test]
    fn observe_at_least_validates_head_on_equal_seq() {
        let mut watermarks = Watermarks::<Applied>::new();
        let ns = NamespaceId::parse("core").unwrap();
        let origin = ReplicaId::new(Uuid::from_bytes([13u8; 16]));
        let seq = Seq0::ZERO;

        watermarks
            .observe_at_least(&ns, &origin, seq, HeadStatus::Genesis)
            .unwrap();

        let err = watermarks
            .observe_at_least(&ns, &origin, seq, HeadStatus::Known([7u8; 32]))
            .unwrap_err();
        assert_eq!(err, WatermarkError::UnexpectedHead { seq });
    }
}
