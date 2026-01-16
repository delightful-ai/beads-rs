//! Admission control and overload shedding.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::core::error::details::{OverloadedDetails, OverloadedSubsystem};
use crate::core::{ErrorPayload, Limits, ProtocolErrorCode};
use crate::daemon::metrics;

const DEFAULT_RETRY_AFTER_MS: u64 = 100;

#[derive(Clone, Debug)]
pub struct AdmissionController {
    inner: Arc<AdmissionState>,
}

#[derive(Debug)]
struct AdmissionState {
    limits: Limits,
    ipc_inflight: AtomicUsize,
    repl_ingest_bytes: AtomicU64,
    repl_ingest_events: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionClass {
    IpcMutation,
    ReplIngest { bytes: u64, events: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionRejection {
    pub subsystem: OverloadedSubsystem,
    pub retry_after_ms: u64,
    pub queue_bytes: Option<u64>,
    pub queue_events: Option<u64>,
}

impl AdmissionRejection {
    pub fn to_error_payload(&self) -> ErrorPayload {
        ErrorPayload::new(ProtocolErrorCode::Overloaded.into(), "overloaded", true).with_details(
            OverloadedDetails {
                subsystem: Some(self.subsystem.clone()),
                retry_after_ms: Some(self.retry_after_ms),
                queue_bytes: self.queue_bytes,
                queue_events: self.queue_events,
            },
        )
    }
}

#[derive(Debug)]
pub struct AdmissionPermit {
    controller: AdmissionController,
    class: AdmissionClass,
}

impl AdmissionController {
    pub fn new(limits: &Limits) -> Self {
        Self {
            inner: Arc::new(AdmissionState {
                limits: limits.clone(),
                ipc_inflight: AtomicUsize::new(0),
                repl_ingest_bytes: AtomicU64::new(0),
                repl_ingest_events: AtomicU64::new(0),
            }),
        }
    }

    pub fn try_admit_ipc_mutation(&self) -> Result<AdmissionPermit, AdmissionRejection> {
        let limit = self.inner.limits.max_ipc_inflight_mutations;
        let next = self.inner.ipc_inflight.fetch_add(1, Ordering::AcqRel) + 1;
        metrics::set_ipc_inflight(next);
        if next > limit {
            self.inner.ipc_inflight.fetch_sub(1, Ordering::AcqRel);
            metrics::set_ipc_inflight(next.saturating_sub(1));
            return Err(AdmissionRejection {
                subsystem: OverloadedSubsystem::Ipc,
                retry_after_ms: DEFAULT_RETRY_AFTER_MS,
                queue_bytes: None,
                queue_events: Some(next as u64),
            });
        }

        Ok(AdmissionPermit {
            controller: self.clone(),
            class: AdmissionClass::IpcMutation,
        })
    }

    pub fn ipc_inflight(&self) -> usize {
        self.inner.ipc_inflight.load(Ordering::Acquire)
    }

    pub fn try_admit_repl_ingest(
        &self,
        bytes: u64,
        events: u64,
    ) -> Result<AdmissionPermit, AdmissionRejection> {
        let max_bytes = self.inner.limits.max_repl_ingest_queue_bytes as u64;
        let max_events = self.inner.limits.max_repl_ingest_queue_events as u64;
        let next_bytes = self
            .inner
            .repl_ingest_bytes
            .fetch_add(bytes, Ordering::AcqRel)
            .saturating_add(bytes);
        let next_events = self
            .inner
            .repl_ingest_events
            .fetch_add(events, Ordering::AcqRel)
            .saturating_add(events);
        metrics::set_repl_queue_bytes(next_bytes);
        metrics::set_repl_queue_events(next_events);

        if next_bytes > max_bytes || next_events > max_events {
            self.inner
                .repl_ingest_bytes
                .fetch_sub(bytes, Ordering::AcqRel);
            self.inner
                .repl_ingest_events
                .fetch_sub(events, Ordering::AcqRel);
            metrics::set_repl_queue_bytes(next_bytes.saturating_sub(bytes));
            metrics::set_repl_queue_events(next_events.saturating_sub(events));
            return Err(AdmissionRejection {
                subsystem: OverloadedSubsystem::Repl,
                retry_after_ms: DEFAULT_RETRY_AFTER_MS,
                queue_bytes: Some(next_bytes),
                queue_events: Some(next_events),
            });
        }

        Ok(AdmissionPermit {
            controller: self.clone(),
            class: AdmissionClass::ReplIngest { bytes, events },
        })
    }

    fn release(&self, class: AdmissionClass) {
        match class {
            AdmissionClass::IpcMutation => {
                let prev = self.inner.ipc_inflight.fetch_sub(1, Ordering::AcqRel);
                debug_assert!(prev > 0, "ipc inflight underflow");
                metrics::set_ipc_inflight(prev.saturating_sub(1));
            }
            AdmissionClass::ReplIngest { bytes, events } => {
                let prev_bytes = self
                    .inner
                    .repl_ingest_bytes
                    .fetch_sub(bytes, Ordering::AcqRel);
                let prev_events = self
                    .inner
                    .repl_ingest_events
                    .fetch_sub(events, Ordering::AcqRel);
                debug_assert!(prev_bytes >= bytes, "repl ingest bytes underflow");
                debug_assert!(prev_events >= events, "repl ingest events underflow");
                metrics::set_repl_queue_bytes(prev_bytes.saturating_sub(bytes));
                metrics::set_repl_queue_events(prev_events.saturating_sub(events));
            }
        }
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        self.controller.release(self.class);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipc_limit_enforced_and_released() {
        let mut limits = Limits::default();
        limits.max_ipc_inflight_mutations = 1;
        let admission = AdmissionController::new(&limits);

        let permit = admission.try_admit_ipc_mutation().unwrap();
        let err = admission.try_admit_ipc_mutation().unwrap_err();
        assert_eq!(err.subsystem, OverloadedSubsystem::Ipc);
        let payload = err.to_error_payload();
        assert_eq!(payload.code, ProtocolErrorCode::Overloaded.into());
        drop(permit);
        assert!(admission.try_admit_ipc_mutation().is_ok());
    }

    #[test]
    fn repl_sheds_before_ipc_mutations() {
        let mut limits = Limits::default();
        limits.max_repl_ingest_queue_bytes = 10;
        limits.max_repl_ingest_queue_events = 1;
        limits.max_ipc_inflight_mutations = 1;
        let admission = AdmissionController::new(&limits);

        let _repl = admission.try_admit_repl_ingest(10, 1).unwrap();
        let err = admission.try_admit_repl_ingest(1, 1).unwrap_err();
        assert_eq!(err.subsystem, OverloadedSubsystem::Repl);
        assert!(admission.try_admit_ipc_mutation().is_ok());
    }

    #[test]
    fn repl_limits_report_queue_metrics() {
        let mut limits = Limits::default();
        limits.max_repl_ingest_queue_bytes = 8;
        limits.max_repl_ingest_queue_events = 2;
        let admission = AdmissionController::new(&limits);

        let _repl = admission.try_admit_repl_ingest(8, 2).unwrap();
        let err = admission.try_admit_repl_ingest(1, 1).unwrap_err();
        assert_eq!(err.subsystem, OverloadedSubsystem::Repl);
        assert!(err.queue_bytes.unwrap() >= 9);
        assert!(err.queue_events.unwrap() >= 3);
    }
}
