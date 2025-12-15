//! Core capability errors (parsing, validation, CRDT invariants).
//!
//! These are bounded and stable: core errors represent domain/refusal states,
//! not library implementation details.

use thiserror::Error;

use crate::error::{Effect, Transience};

/// Invalid ID or content identifier.
#[derive(Debug, Error, Clone)]
#[non_exhaustive]
pub enum InvalidId {
    #[error("bead id `{raw}` is invalid: {reason}")]
    Bead { raw: String, reason: String },
    #[error("actor id `{raw}` is invalid: {reason}")]
    Actor { raw: String, reason: String },
    #[error("note id `{raw}` is invalid: {reason}")]
    Note { raw: String, reason: String },
    #[error("branch name `{raw}` is invalid: {reason}")]
    Branch { raw: String, reason: String },
    #[error("content hash `{raw}` is invalid: {reason}")]
    ContentHash { raw: String, reason: String },
}

/// Invalid label string.
#[derive(Debug, Error, Clone)]
#[error("label `{raw}` is invalid: {reason}")]
pub struct InvalidLabel {
    pub raw: String,
    pub reason: String,
}

/// Generic range violation.
#[derive(Debug, Error, Clone)]
#[error("{field} value {value} out of range {min}..={max}")]
pub struct RangeError {
    pub field: &'static str,
    pub value: u8,
    pub min: u8,
    pub max: u8,
}

/// ID collision between independently-created beads.
#[derive(Debug, Error, Clone)]
#[error("bead id collision: {id} has conflicting creation stamps")]
pub struct CollisionError {
    pub id: String,
}

/// Invalid dependency edge.
#[derive(Debug, Error, Clone)]
#[error("invalid dependency: {reason}")]
pub struct InvalidDependency {
    pub reason: String,
}

/// Canonical error enum for core capability.
#[derive(Debug, Error, Clone)]
#[non_exhaustive]
pub enum CoreError {
    #[error(transparent)]
    InvalidId(#[from] InvalidId),
    #[error(transparent)]
    InvalidLabel(#[from] InvalidLabel),
    #[error(transparent)]
    Range(#[from] RangeError),
    #[error(transparent)]
    Collision(#[from] CollisionError),
    #[error(transparent)]
    InvalidDependency(#[from] InvalidDependency),
}

impl CoreError {
    pub fn transience(&self) -> Transience {
        // Core errors are pure domain/input failures.
        Transience::Permanent
    }

    pub fn effect(&self) -> Effect {
        Effect::None
    }
}
