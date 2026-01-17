//! Beads realtime lane: Stateright models.
//!
//! This crate is intentionally small but must stay in sync with production.
//! Prefer production-backed adapters from `beads_rs::model` and keep any toy
//! helpers confined to deprecated examples or tests.
//!
//! The goal is to model-check correctness invariants (contiguity, monotonicity,
//! no-equivocation, handshake gating) without re-implementing core logic.
//!
//! Each example in `examples/` focuses on one "little machine" at a time.

pub mod drift_guard;
