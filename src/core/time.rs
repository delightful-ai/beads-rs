//! Layer 0: Time primitives
//!
//! HLC (Hybrid Logical Clock) for causal ordering.
//! WallClock for TTL/lease (not ordering).

use std::cmp::Ordering;
use std::sync::{Arc, RwLock, OnceLock};

use serde::{Deserialize, Serialize};

use super::identity::ActorId;

/// HLC timestamp - the ordering primitive.
///
/// (wall_ms, counter) forms total order within an actor.
/// !Copy intentional - forces explicit .clone() to think about causality.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteStamp {
    pub wall_ms: u64,
    pub counter: u32,
}

impl WriteStamp {
    pub fn new(wall_ms: u64, counter: u32) -> Self {
        Self { wall_ms, counter }
    }
}

impl PartialOrd for WriteStamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WriteStamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.wall_ms
            .cmp(&other.wall_ms)
            .then_with(|| self.counter.cmp(&other.counter))
    }
}

/// Wall clock for TTL/lease - NOT for causal ordering.
///
/// Copy is fine here - it's just a measurement, not causality.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WallClock(pub u64);

pub trait WallClockSource: Send + Sync {
    fn now_ms(&self) -> u64;
}

struct SystemWallClockSource;

impl WallClockSource for SystemWallClockSource {
    fn now_ms(&self) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

fn wall_clock_source() -> &'static RwLock<Arc<dyn WallClockSource>> {
    static SOURCE: OnceLock<RwLock<Arc<dyn WallClockSource>>> = OnceLock::new();
    SOURCE.get_or_init(|| RwLock::new(Arc::new(SystemWallClockSource)))
}

impl WallClock {
    pub fn now() -> Self {
        let source = wall_clock_source()
            .read()
            .unwrap_or_else(|err| err.into_inner());
        Self(source.now_ms())
    }
}

#[cfg(feature = "test-harness")]
static WALL_CLOCK_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

#[cfg(feature = "test-harness")]
pub struct WallClockGuard {
    prev: Arc<dyn WallClockSource>,
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(feature = "test-harness")]
impl Drop for WallClockGuard {
    fn drop(&mut self) {
        let mut guard = wall_clock_source()
            .write()
            .unwrap_or_else(|err| err.into_inner());
        *guard = self.prev.clone();
    }
}

#[cfg(feature = "test-harness")]
pub fn set_wall_clock_source_for_tests(source: Arc<dyn WallClockSource>) -> WallClockGuard {
    let lock = WALL_CLOCK_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    let mut guard = wall_clock_source()
        .write()
        .unwrap_or_else(|err| err.into_inner());
    let prev = guard.clone();
    *guard = source;
    WallClockGuard { prev, _lock: lock }
}

/// Stamp = WriteStamp + attribution.
///
/// This is what you compare for LWW - includes actor for deterministic tiebreak.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stamp {
    pub at: WriteStamp,
    pub by: ActorId,
}

impl Stamp {
    pub fn new(at: WriteStamp, by: ActorId) -> Self {
        Self { at, by }
    }
}

impl PartialOrd for Stamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Stamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.at.cmp(&other.at).then_with(|| self.by.cmp(&other.by)) // deterministic tiebreak
    }
}
