//! Thread-safe SessionStore adapter for replication threads.

use std::sync::{Arc, Mutex};

use super::{IngestOutcome, SessionStore, WatermarkSnapshot};
use crate::core::{
    EventId, EventShaLookupError, NamespaceId, PrevVerified, ReplicaId, Sha256, VerifiedEvent,
};

#[derive(Clone)]
pub struct SharedSessionStore<S> {
    inner: Arc<Mutex<S>>,
}

impl<S> SharedSessionStore<S> {
    pub fn new(store: S) -> Self {
        Self {
            inner: Arc::new(Mutex::new(store)),
        }
    }

    pub fn from_arc(inner: Arc<Mutex<S>>) -> Self {
        Self { inner }
    }

    pub fn lock(&self) -> std::sync::MutexGuard<'_, S> {
        self.inner.lock().expect("session store lock poisoned")
    }
}

impl<S: SessionStore> SessionStore for SharedSessionStore<S> {
    fn watermark_snapshot(&self, namespaces: &[NamespaceId]) -> WatermarkSnapshot {
        self.lock().watermark_snapshot(namespaces)
    }

    fn lookup_event_sha(&self, eid: &EventId) -> Result<Option<Sha256>, EventShaLookupError> {
        self.lock().lookup_event_sha(eid)
    }

    fn ingest_remote_batch(
        &mut self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        batch: &[VerifiedEvent<PrevVerified>],
        now_ms: u64,
    ) -> Result<IngestOutcome, Box<crate::core::ErrorPayload>> {
        self.lock()
            .ingest_remote_batch(namespace, origin, batch, now_ms)
    }
}
