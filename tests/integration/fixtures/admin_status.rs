#![allow(dead_code)]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use thiserror::Error;

use beads_rs::{
    ErrorCode, HeadStatus, NamespaceId, ProtocolErrorCode, Seq0, WatermarkError, Watermarks,
};
use beads_rs::api::AdminStatusOutput;
use beads_rs::api::QueryResult;
use beads_rs::daemon::ipc::{
    IpcClient, IpcConnection, IpcError, ReadConsistency, Request, Response, ResponsePayload,
};

#[derive(Debug, Error)]
pub enum StatusError {
    #[error(transparent)]
    Ipc(#[from] IpcError),
    #[error("remote error: {0:?}")]
    Remote(Box<beads_rs::ErrorPayload>),
    #[error("unexpected response payload: {0:?}")]
    Unexpected(Box<ResponsePayload>),
    #[error(transparent)]
    Watermark(#[from] WatermarkError),
}

#[derive(Default)]
pub struct StatusCollector {
    repo: PathBuf,
    read: ReadConsistency,
    samples: Vec<AdminStatusOutput>,
    client: IpcClient,
    connection: Option<IpcConnection>,
}

impl StatusCollector {
    pub fn new(repo: PathBuf) -> Self {
        Self::with_client(repo, IpcClient::new())
    }

    pub fn with_client(repo: PathBuf, client: IpcClient) -> Self {
        Self {
            repo,
            read: ReadConsistency::default(),
            samples: Vec::new(),
            client,
            connection: None,
        }
    }

    pub fn with_read(mut self, read: ReadConsistency) -> Self {
        self.read = read;
        self
    }

    pub fn sample(&mut self) -> Result<&AdminStatusOutput, StatusError> {
        let read = self.read.clone();
        self.sample_with_read(read)
    }

    pub fn sample_when_applied_advances(
        &mut self,
        wait_timeout: Duration,
    ) -> Result<Option<&AdminStatusOutput>, StatusError> {
        if self.samples.is_empty() {
            return self.sample().map(Some);
        }

        let last = self
            .samples
            .last()
            .expect("samples should be non-empty");
        let required = next_applied_requirement(last, &self.read)?;

        let mut read = self.read.clone();
        read.require_min_seen = Some(required);
        read.wait_timeout_ms = Some(wait_timeout_ms(wait_timeout));

        match self.sample_with_read(read) {
            Ok(sample) => Ok(Some(sample)),
            Err(StatusError::Remote(err)) if is_require_min_seen_timeout(err.as_ref()) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn sample_with_read(
        &mut self,
        read: ReadConsistency,
    ) -> Result<&AdminStatusOutput, StatusError> {
        let request = Request::AdminStatus {
            repo: self.repo.clone(),
            read,
        };
        let response = self.send_request(&request)?;
        let status = parse_admin_status(response)?;
        self.samples.push(status);
        Ok(self.samples.last().expect("sample inserted"))
    }

    pub fn collect_for(
        &mut self,
        duration: Duration,
        interval: Duration,
    ) -> Result<&[AdminStatusOutput], StatusError> {
        if self.samples.is_empty() {
            let _ = self.sample()?;
        }
        let deadline = Instant::now() + duration;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait = if interval.is_zero() {
                remaining
            } else {
                interval.min(remaining)
            };
            let _ = self.sample_when_applied_advances(wait)?;
        }
        Ok(&self.samples)
    }

    pub fn samples(&self) -> &[AdminStatusOutput] {
        &self.samples
    }

    fn send_request(&mut self, request: &Request) -> Result<Response, StatusError> {
        if self.connection.is_none() {
            self.connection = Some(self.client.connect()?);
        }
        let Some(connection) = self.connection.as_mut() else {
            return Err(IpcError::Disconnected.into());
        };
        match connection.send_request(request) {
            Ok(response) => Ok(response),
            Err(err) => {
                self.connection = None;
                Err(StatusError::Ipc(err))
            }
        }
    }
}

pub fn assert_monotonic_watermarks(samples: &[AdminStatusOutput]) {
    let mut prev_applied: Option<Watermarks<beads_rs::Applied>> = None;
    let mut prev_durable: Option<Watermarks<beads_rs::Durable>> = None;
    for sample in samples {
        if let Some(prev) = &prev_applied {
            assert!(
                sample.watermarks_applied.satisfies_at_least(prev),
                "applied watermarks regressed"
            );
        }
        if let Some(prev) = &prev_durable {
            assert!(
                sample.watermarks_durable.satisfies_at_least(prev),
                "durable watermarks regressed"
            );
        }
        prev_applied = Some(sample.watermarks_applied.clone());
        prev_durable = Some(sample.watermarks_durable.clone());
    }
}

fn parse_admin_status(response: Response) -> Result<AdminStatusOutput, StatusError> {
    match response {
        Response::Ok { ok } => match ok {
            ResponsePayload::Query(QueryResult::AdminStatus(status)) => Ok(status),
            other => Err(StatusError::Unexpected(Box::new(other))),
        },
        Response::Err { err } => Err(StatusError::Remote(Box::new(err))),
    }
}

fn wait_timeout_ms(wait_timeout: Duration) -> u64 {
    let ms = wait_timeout.as_millis().clamp(1, u128::from(u64::MAX));
    ms as u64
}

fn is_require_min_seen_timeout(err: &beads_rs::ErrorPayload) -> bool {
    matches!(
        err.code,
        ErrorCode::Protocol(ProtocolErrorCode::RequireMinSeenTimeout)
    )
}

fn next_applied_requirement(
    status: &AdminStatusOutput,
    read: &ReadConsistency,
) -> Result<Watermarks<beads_rs::Applied>, StatusError> {
    let namespace = read
        .namespace
        .as_deref()
        .and_then(|raw| NamespaceId::parse(raw.to_string()).ok())
        .unwrap_or_else(NamespaceId::core);
    let current_seq = status
        .watermarks_applied
        .get(&namespace, &status.replica_id)
        .map(|mark| mark.seq().get())
        .unwrap_or(0);
    let next_seq = current_seq.saturating_add(1);
    let mut required = Watermarks::new();
    required.observe_at_least(
        &namespace,
        &status.replica_id,
        Seq0::new(next_seq),
        HeadStatus::Known([0u8; 32]),
    )?;
    Ok(required)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixtures_admin_status_monotonic_empty_samples() {
        assert_monotonic_watermarks(&[]);
    }
}
