#![allow(dead_code)]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use thiserror::Error;

use beads_rs::Watermarks;
use beads_rs::api::AdminStatusOutput;
use beads_rs::daemon::ipc::{
    IpcClient, IpcError, ReadConsistency, Request, Response, ResponsePayload,
};
use beads_rs::daemon::query::QueryResult;

#[derive(Debug, Error)]
pub enum StatusError {
    #[error(transparent)]
    Ipc(#[from] IpcError),
    #[error("remote error: {0:?}")]
    Remote(beads_rs::ErrorPayload),
    #[error("unexpected response payload: {0:?}")]
    Unexpected(ResponsePayload),
}

#[derive(Debug, Default, Clone)]
pub struct StatusCollector {
    repo: PathBuf,
    read: ReadConsistency,
    samples: Vec<AdminStatusOutput>,
    client: IpcClient,
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
        }
    }

    pub fn with_read(mut self, read: ReadConsistency) -> Self {
        self.read = read;
        self
    }

    pub fn sample(&mut self) -> Result<&AdminStatusOutput, StatusError> {
        let request = Request::AdminStatus {
            repo: self.repo.clone(),
            read: self.read.clone(),
        };
        let response = self.client.send_request_no_autostart(&request)?;
        let status = parse_admin_status(response)?;
        self.samples.push(status);
        Ok(self.samples.last().expect("sample inserted"))
    }

    pub fn collect_for(
        &mut self,
        duration: Duration,
        interval: Duration,
    ) -> Result<&[AdminStatusOutput], StatusError> {
        let deadline = Instant::now() + duration;
        while Instant::now() < deadline {
            let _ = self.sample()?;
            std::thread::sleep(interval);
        }
        Ok(&self.samples)
    }

    pub fn samples(&self) -> &[AdminStatusOutput] {
        &self.samples
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
            other => Err(StatusError::Unexpected(other)),
        },
        Response::Err { err } => Err(StatusError::Remote(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixtures_admin_status_monotonic_empty_samples() {
        assert_monotonic_watermarks(&[]);
    }
}
