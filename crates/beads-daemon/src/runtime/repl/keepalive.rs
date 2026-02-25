//! Replication keepalive tracking.

use crate::core::Limits;

use beads_daemon_core::repl::proto::Ping;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum KeepaliveDecision {
    SendPing(Ping),
    Close,
}

#[derive(Clone, Debug)]
pub(crate) struct KeepaliveTracker {
    keepalive_ms: u64,
    dead_ms: u64,
    last_recv_ms: u64,
    last_send_ms: u64,
    next_nonce: u64,
}

impl KeepaliveTracker {
    pub(crate) fn new(limits: &Limits, now_ms: u64) -> Self {
        Self {
            keepalive_ms: limits.keepalive_ms,
            dead_ms: limits.dead_ms,
            last_recv_ms: now_ms,
            last_send_ms: now_ms,
            next_nonce: 1,
        }
    }

    pub(crate) fn note_recv(&mut self, now_ms: u64) {
        self.last_recv_ms = now_ms;
    }

    pub(crate) fn note_send(&mut self, now_ms: u64) {
        self.last_send_ms = now_ms;
    }

    pub(crate) fn poll(&mut self, now_ms: u64) -> Option<KeepaliveDecision> {
        if self.dead_ms > 0 && now_ms.saturating_sub(self.last_recv_ms) >= self.dead_ms {
            return Some(KeepaliveDecision::Close);
        }
        if self.keepalive_ms > 0 && now_ms.saturating_sub(self.last_send_ms) >= self.keepalive_ms {
            let nonce = self.next_nonce;
            self.next_nonce = self.next_nonce.saturating_add(1);
            return Some(KeepaliveDecision::SendPing(Ping { nonce }));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keepalive_emits_ping_after_idle() {
        let limits = Limits {
            keepalive_ms: 100,
            dead_ms: 1_000,
            ..Default::default()
        };
        let mut tracker = KeepaliveTracker::new(&limits, 0);

        assert!(tracker.poll(99).is_none());
        let decision = tracker.poll(100).expect("ping");
        match decision {
            KeepaliveDecision::SendPing(ping) => assert_eq!(ping.nonce, 1),
            KeepaliveDecision::Close => panic!("unexpected close"),
        }
        tracker.note_send(100);

        assert!(tracker.poll(150).is_none());
        let decision = tracker.poll(201).expect("ping");
        match decision {
            KeepaliveDecision::SendPing(ping) => assert_eq!(ping.nonce, 2),
            KeepaliveDecision::Close => panic!("unexpected close"),
        }
    }

    #[test]
    fn keepalive_deadline_trumps_ping() {
        let limits = Limits {
            keepalive_ms: 50,
            dead_ms: 100,
            ..Default::default()
        };
        let mut tracker = KeepaliveTracker::new(&limits, 0);

        assert!(matches!(tracker.poll(101), Some(KeepaliveDecision::Close)));
    }

    #[test]
    fn keepalive_recv_resets_deadline() {
        let limits = Limits {
            keepalive_ms: 0,
            dead_ms: 100,
            ..Default::default()
        };
        let mut tracker = KeepaliveTracker::new(&limits, 0);

        tracker.note_recv(80);
        assert!(tracker.poll(150).is_none());
        assert!(matches!(tracker.poll(181), Some(KeepaliveDecision::Close)));
    }
}
