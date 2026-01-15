//! Simple token-bucket limiter for I/O budgets.

use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub(crate) struct TokenBucket {
    rate_bytes_per_sec: u64,
    burst_bytes: u64,
    available: u64,
    last_refill: Instant,
}

impl TokenBucket {
    pub(crate) fn new(rate_bytes_per_sec: u64) -> Self {
        let burst_bytes = rate_bytes_per_sec.max(1);
        Self {
            rate_bytes_per_sec,
            burst_bytes,
            available: burst_bytes,
            last_refill: Instant::now(),
        }
    }

    pub(crate) fn reserve_at(&mut self, bytes: u64, now: Instant) -> Duration {
        if self.rate_bytes_per_sec == 0 || bytes == 0 {
            self.last_refill = now;
            return Duration::ZERO;
        }

        if now > self.last_refill {
            let elapsed_ms = now.duration_since(self.last_refill).as_millis() as u64;
            if elapsed_ms > 0 {
                let added = self
                    .rate_bytes_per_sec
                    .saturating_mul(elapsed_ms)
                    .saturating_div(1000);
                self.available = self.available.saturating_add(added).min(self.burst_bytes);
            }
            self.last_refill = now;
        }

        if bytes <= self.available {
            self.available = self.available.saturating_sub(bytes);
            return Duration::ZERO;
        }

        let deficit = bytes.saturating_sub(self.available);
        self.available = 0;
        let wait_ms = ((deficit as u128) * 1000 + (self.rate_bytes_per_sec as u128 - 1))
            / self.rate_bytes_per_sec as u128;
        let wait_ms = wait_ms.min(u64::MAX as u128) as u64;
        let wait = Duration::from_millis(wait_ms);
        self.last_refill = now + wait;
        wait
    }

    pub(crate) fn throttle(&mut self, bytes: u64) -> Duration {
        let now = Instant::now();
        let wait = self.reserve_at(bytes, now);
        if !wait.is_zero() {
            std::thread::sleep(wait);
        }
        wait
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_returns_wait_for_deficit() {
        let start = Instant::now();
        let mut bucket = TokenBucket::new(100);

        assert_eq!(bucket.reserve_at(50, start), Duration::ZERO);
        assert_eq!(bucket.reserve_at(60, start), Duration::from_millis(100));

        let later = start + Duration::from_millis(1100);
        assert_eq!(bucket.reserve_at(100, later), Duration::ZERO);
    }

    #[test]
    fn rate_zero_disables_throttle() {
        let mut bucket = TokenBucket::new(0);
        let now = Instant::now();
        assert_eq!(bucket.reserve_at(10_000, now), Duration::ZERO);
    }
}
