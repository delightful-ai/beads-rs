use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoCtx {
    #[serde(rename = "repo")]
    pub path: PathBuf,
}

impl RepoCtx {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationCtx {
    #[serde(flatten)]
    pub repo: RepoCtx,
    #[serde(default, flatten)]
    pub meta: super::MutationMeta,
}

impl MutationCtx {
    pub fn new(repo: PathBuf, meta: super::MutationMeta) -> Self {
        Self {
            repo: RepoCtx::new(repo),
            meta,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadCtx {
    #[serde(flatten)]
    pub repo: RepoCtx,
    #[serde(default, flatten)]
    pub read: super::ReadConsistency,
}

impl ReadCtx {
    pub fn new(repo: PathBuf, read: super::ReadConsistency) -> Self {
        Self {
            repo: RepoCtx::new(repo),
            read,
        }
    }
}
