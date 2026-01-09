//! Beads realtime lane: Stateright models.
//!
//! This crate is intentionally small and "toy". The goal is to model-check the
//! correctness invariants (contiguity, monotonicity, no-equivocation, handshake
//! gating) without pulling in the full production implementation.
//!
//! Each example in `examples/` focuses on one "little machine" at a time.

pub mod spec;
pub mod toy_codec;
pub mod realtime_types_sketch;
