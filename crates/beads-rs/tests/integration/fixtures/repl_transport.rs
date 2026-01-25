#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crossbeam::channel::{Receiver, Sender, unbounded};

use beads_rs::Limits;
use beads_rs::daemon::repl::proto::{ReplEnvelope, ReplMessage};

use super::repl_frames;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    AtoB,
    BtoA,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NextFrameAction {
    Deliver,
    Drop,
    Reorder,
}

#[derive(Clone, Debug)]
struct ScheduledFrame {
    deliver_at_ms: u64,
    frame: Vec<u8>,
}

#[derive(Clone, Debug)]
struct SimulatedLink {
    queue: VecDeque<ScheduledFrame>,
    next_action: NextFrameAction,
    delay_next_ms: u64,
}

impl SimulatedLink {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            next_action: NextFrameAction::Deliver,
            delay_next_ms: 0,
        }
    }

    fn enqueue(&mut self, now_ms: u64, frame: Vec<u8>) {
        if matches!(self.next_action, NextFrameAction::Drop) {
            self.next_action = NextFrameAction::Deliver;
            self.delay_next_ms = 0;
            return;
        }

        let deliver_at_ms = now_ms.saturating_add(self.delay_next_ms);
        self.delay_next_ms = 0;
        let entry = ScheduledFrame {
            deliver_at_ms,
            frame,
        };

        if matches!(self.next_action, NextFrameAction::Reorder) && !self.queue.is_empty() {
            self.queue.insert(0, entry);
        } else {
            self.queue.push_back(entry);
        }
        self.next_action = NextFrameAction::Deliver;
    }

    fn drain_ready(&mut self, now_ms: u64, deliver: &Sender<Vec<u8>>) {
        while let Some(front) = self.queue.front() {
            if front.deliver_at_ms > now_ms {
                break;
            }
            let entry = self.queue.pop_front().expect("queued frame");
            let _ = deliver.send(entry.frame);
        }
    }
}

#[derive(Debug)]
struct NetworkSimulator {
    now_ms: u64,
    a_to_b: SimulatedLink,
    b_to_a: SimulatedLink,
    a_tx: Sender<Vec<u8>>,
    b_tx: Sender<Vec<u8>>,
}

impl NetworkSimulator {
    fn new(a_tx: Sender<Vec<u8>>, b_tx: Sender<Vec<u8>>) -> Self {
        Self {
            now_ms: 0,
            a_to_b: SimulatedLink::new(),
            b_to_a: SimulatedLink::new(),
            a_tx,
            b_tx,
        }
    }

    fn send(&mut self, direction: Direction, frame: Vec<u8>) {
        match direction {
            Direction::AtoB => self.a_to_b.enqueue(self.now_ms, frame),
            Direction::BtoA => self.b_to_a.enqueue(self.now_ms, frame),
        }
    }

    fn flush(&mut self) {
        self.a_to_b.drain_ready(self.now_ms, &self.b_tx);
        self.b_to_a.drain_ready(self.now_ms, &self.a_tx);
    }

    fn advance(&mut self, delta_ms: u64) {
        self.now_ms = self.now_ms.saturating_add(delta_ms);
        self.flush();
    }

    fn drop_next(&mut self, direction: Direction) {
        match direction {
            Direction::AtoB => self.a_to_b.next_action = NextFrameAction::Drop,
            Direction::BtoA => self.b_to_a.next_action = NextFrameAction::Drop,
        }
    }

    fn delay_next(&mut self, direction: Direction, delay_ms: u64) {
        match direction {
            Direction::AtoB => self.a_to_b.delay_next_ms = delay_ms,
            Direction::BtoA => self.b_to_a.delay_next_ms = delay_ms,
        }
    }

    fn reorder_next(&mut self, direction: Direction) {
        match direction {
            Direction::AtoB => self.a_to_b.next_action = NextFrameAction::Reorder,
            Direction::BtoA => self.b_to_a.next_action = NextFrameAction::Reorder,
        }
    }
}

#[derive(Clone)]
pub struct NetworkController {
    inner: Arc<Mutex<NetworkSimulator>>,
}

impl NetworkController {
    pub fn flush(&self) {
        self.inner.lock().expect("network lock").flush();
    }

    pub fn advance(&self, delta_ms: u64) {
        self.inner.lock().expect("network lock").advance(delta_ms);
    }

    pub fn drop_next(&self, direction: Direction) {
        self.inner
            .lock()
            .expect("network lock")
            .drop_next(direction);
    }

    pub fn delay_next(&self, direction: Direction, delay_ms: u64) {
        self.inner
            .lock()
            .expect("network lock")
            .delay_next(direction, delay_ms);
    }

    pub fn reorder_next(&self, direction: Direction) {
        self.inner
            .lock()
            .expect("network lock")
            .reorder_next(direction);
    }
}

#[derive(Clone)]
struct NetworkHandle {
    inner: Arc<Mutex<NetworkSimulator>>,
    direction: Direction,
}

impl NetworkHandle {
    fn send(&self, frame: Vec<u8>) {
        self.inner
            .lock()
            .expect("network lock")
            .send(self.direction, frame);
    }
}

pub struct ChannelEndpoint {
    sender: NetworkHandle,
    receiver: Receiver<Vec<u8>>,
    max_frame_bytes: usize,
}

impl ChannelEndpoint {
    pub fn send_message(&self, message: &ReplMessage) {
        let frame = repl_frames::encode_message_frame(message.clone(), self.max_frame_bytes);
        self.sender.send(frame);
    }

    pub fn try_recv_message(&self) -> Option<ReplEnvelope> {
        let frame = self.receiver.try_recv().ok()?;
        Some(repl_frames::decode_message_frame(
            &frame,
            self.max_frame_bytes,
        ))
    }

    pub fn drain_messages(&self) -> Vec<ReplEnvelope> {
        let mut out = Vec::new();
        while let Ok(frame) = self.receiver.try_recv() {
            out.push(repl_frames::decode_message_frame(
                &frame,
                self.max_frame_bytes,
            ));
        }
        out
    }
}

pub struct ChannelTransport {
    pub a: ChannelEndpoint,
    pub b: ChannelEndpoint,
    pub network: NetworkController,
}

impl ChannelTransport {
    pub fn pair(max_frame_bytes: usize) -> Self {
        let (a_tx, a_rx) = unbounded();
        let (b_tx, b_rx) = unbounded();

        let simulator = Arc::new(Mutex::new(NetworkSimulator::new(a_tx, b_tx)));
        let network = NetworkController {
            inner: simulator.clone(),
        };

        let a = ChannelEndpoint {
            sender: NetworkHandle {
                inner: simulator.clone(),
                direction: Direction::AtoB,
            },
            receiver: a_rx,
            max_frame_bytes,
        };
        let b = ChannelEndpoint {
            sender: NetworkHandle {
                inner: simulator.clone(),
                direction: Direction::BtoA,
            },
            receiver: b_rx,
            max_frame_bytes,
        };

        Self { a, b, network }
    }

    pub fn with_limits(limits: &Limits) -> Self {
        Self::pair(limits.max_frame_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::identity;
    use uuid::Uuid;

    use beads_rs::core::ReplicaId;

    #[test]
    fn fixtures_repl_transport_roundtrip() {
        let limits = Limits::default();
        let transport = ChannelTransport::with_limits(&limits);
        let store = identity::store_identity_with_epoch(1, 1);
        let replica = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let hello = ReplMessage::Hello(repl_frames::hello(store, replica));

        transport.a.send_message(&hello);
        transport.network.flush();

        let received = transport.b.try_recv_message().expect("message");
        assert_eq!(received.message, hello);
    }

    #[test]
    fn fixtures_repl_transport_delay_and_drop() {
        let limits = Limits::default();
        let transport = ChannelTransport::with_limits(&limits);
        let store = identity::store_identity_with_epoch(3, 1);
        let replica = ReplicaId::new(Uuid::from_bytes([4u8; 16]));
        let hello = ReplMessage::Hello(repl_frames::hello(store, replica));

        transport.network.delay_next(Direction::AtoB, 5);
        transport.a.send_message(&hello);
        transport.network.flush();
        assert!(transport.b.try_recv_message().is_none());

        transport.network.advance(5);
        assert!(transport.b.try_recv_message().is_some());

        transport.network.drop_next(Direction::AtoB);
        transport.a.send_message(&hello);
        transport.network.flush();
        assert!(transport.b.try_recv_message().is_none());
    }
}
