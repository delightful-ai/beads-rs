//! An ordered reliable link (ORL) based loosely on the "perfect link" described in "[Introduction
//! to Reliable and Secure Distributed
//! Programming](https://link.springer.com/book/10.1007/978-3-642-15260-3)" by Cachin, Guerraoui,
//! and Rodrigues (with enhancements to provide ordering).
//!
//! Order is maintained for messages between a source/destination pair. Order is not maintained
//! between different destinations or different sources.
//!
//! The implementation assumes that actors can restart, by persisting sequencer
//! information to properly handle actor restarts.
//!
//! # See Also
//!
//! [`Network::new_ordered`] can be used to reduce the state space of models that will use this
//! abstraction.
//!
//! NOTE: This is a local fork of `stateright::actor::ordered_reliable_link` with a read-only
//! accessor for the wrapped state so we can run model invariants against actor state in
//! ordered-link mode.

use serde::{Deserialize, Serialize};

use stateright::actor::*;
use stateright::util::HashableHashMap;
use std::borrow::Cow;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Range;
use std::time::Duration;

/// Wraps an actor with logic to:
/// 1. Maintain message order.
/// 2. Resend lost messages.
/// 3. Avoid message redelivery.
#[derive(Clone)]
pub struct ActorWrapper<A: Actor> {
    pub resend_interval: Range<Duration>,
    pub wrapped_actor: A,
}

/// An envelope for ORL messages.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum MsgWrapper<Msg> {
    Deliver(Sequencer, Msg),
    Ack(Sequencer),
}

/// Message sequencer.
pub type Sequencer = u64;

/// Maintains state for the ORL.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StateWrapper<Msg, State, Storage> {
    // send side
    next_send_seq: Sequencer,
    msgs_pending_ack: HashableHashMap<Sequencer, (Id, Msg)>,

    // receive (ack'ing) side
    last_delivered_seqs: HashableHashMap<Id, Sequencer>,

    wrapped_state: State,
    wrapped_storage: Option<Storage>,
}

impl<Msg, State, Storage> StateWrapper<Msg, State, Storage> {
    pub fn wrapped_state(&self) -> &State {
        &self.wrapped_state
    }
}

/// Wrapper for timers.
#[derive(Eq, Ord, PartialEq, PartialOrd, Clone, Debug, Hash)]
pub enum TimerWrapper<Timer> {
    Network,
    User(Timer),
}

/// Maintains storage for the ORL.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StorageWrapper<Msg, Storage> {
    // send side
    next_send_seq: Sequencer,
    msgs_pending_ack: HashableHashMap<Sequencer, (Id, Msg)>,

    // receive (ack'ing) side
    last_delivered_seqs: HashableHashMap<Id, Sequencer>,

    wrapped_storage: Option<Storage>,
}

impl<A: Actor> ActorWrapper<A> {
    pub fn with_default_timeout(wrapped_actor: A) -> Self {
        Self {
            resend_interval: Duration::from_secs(1)..Duration::from_secs(2),
            wrapped_actor,
        }
    }
}

impl<A: Actor> Actor for ActorWrapper<A>
where
    A::Msg: Hash,
{
    type Msg = MsgWrapper<A::Msg>;
    type State = StateWrapper<A::Msg, A::State, A::Storage>;
    type Timer = TimerWrapper<A::Timer>;
    type Random = A::Random;
    type Storage = StorageWrapper<A::Msg, A::Storage>;

    fn on_start(&self, id: Id, storage: &Option<Self::Storage>, o: &mut Out<Self>) -> Self::State {
        o.set_timer(TimerWrapper::Network, self.resend_interval.clone());

        let mut wrapped_out = Out::new();
        let (next_send_seq, msgs_pending_ack, last_delivered_seqs, wrapped_storage) = match storage
        {
            Some(StorageWrapper {
                next_send_seq,
                msgs_pending_ack,
                last_delivered_seqs,
                wrapped_storage,
            }) => (
                *next_send_seq,
                msgs_pending_ack.clone(),
                last_delivered_seqs.clone(),
                wrapped_storage.clone(),
            ),
            None => (1, Default::default(), Default::default(), None),
        };
        let mut state = StateWrapper {
            next_send_seq,
            msgs_pending_ack,
            last_delivered_seqs,
            wrapped_state: self
                .wrapped_actor
                .on_start(id, &wrapped_storage, &mut wrapped_out),
            wrapped_storage: wrapped_storage.clone(),
        };
        process_output(&mut state, wrapped_out, o);
        state
    }

    fn on_msg(
        &self,
        id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        match msg {
            MsgWrapper::Deliver(seq, wrapped_msg) => {
                // Always ack the message to prevent re-sends, and early exit if already delivered.
                o.send(src, MsgWrapper::Ack(seq));
                if seq <= *state.last_delivered_seqs.get(&src).unwrap_or(&0) {
                    return;
                }

                // Process the message, and early exit if ignored.
                let mut wrapped_state = Cow::Borrowed(&state.wrapped_state);
                let mut wrapped_out = Out::new();
                self.wrapped_actor.on_msg(
                    id,
                    &mut wrapped_state,
                    src,
                    wrapped_msg,
                    &mut wrapped_out,
                );
                if is_no_op(&wrapped_state, &wrapped_out) {
                    return;
                }

                // Never delivered, and not ignored by actor, so update the sequencer and process the original output.
                if let Cow::Owned(wrapped_state) = wrapped_state {
                    // Avoid unnecessarily cloning wrapped_state by not calling to_mut() in this
                    // case.
                    *state = Cow::Owned(StateWrapper {
                        next_send_seq: state.next_send_seq,
                        msgs_pending_ack: state.msgs_pending_ack.clone(),
                        last_delivered_seqs: state.last_delivered_seqs.clone(),
                        wrapped_state,
                        wrapped_storage: state.wrapped_storage.clone(),
                    });
                }
                state.to_mut().last_delivered_seqs.insert(src, seq);
                process_output(state.to_mut(), wrapped_out, o);
            }
            MsgWrapper::Ack(seq) => {
                state.to_mut().msgs_pending_ack.remove(&seq);
            }
        }
        // Non-volatile fields in Storage are changed, persist them.
        o.save(StorageWrapper {
            next_send_seq: state.next_send_seq,
            msgs_pending_ack: state.msgs_pending_ack.clone(),
            last_delivered_seqs: state.last_delivered_seqs.clone(),
            wrapped_storage: state.wrapped_storage.clone(),
        })
    }

    fn on_timeout(
        &self,
        id: Id,
        state: &mut std::borrow::Cow<Self::State>,
        timer: &Self::Timer,
        o: &mut Out<Self>,
    ) {
        match timer {
            TimerWrapper::Network => {
                o.set_timer(TimerWrapper::Network, self.resend_interval.clone());
                for (seq, (dst, msg)) in &state.msgs_pending_ack {
                    o.send(*dst, MsgWrapper::Deliver(*seq, msg.clone()));
                }
            }
            TimerWrapper::User(timer) => {
                let mut wrapped_state = Cow::Borrowed(&state.wrapped_state);
                let mut wrapped_out = Out::new();
                self.wrapped_actor
                    .on_timeout(id, &mut wrapped_state, timer, &mut wrapped_out);
                if is_no_op(&wrapped_state, &wrapped_out) {
                    return;
                }
                process_output(state.to_mut(), wrapped_out, o);
            }
        }
    }

    fn name(&self) -> String {
        self.wrapped_actor.name()
    }
}

fn process_output<A: Actor>(
    state: &mut StateWrapper<A::Msg, A::State, A::Storage>,
    wrapped_out: Out<A>,
    o: &mut Out<ActorWrapper<A>>,
) where
    A::Msg: Hash,
{
    let mut should_save_storage = false;
    for command in wrapped_out {
        match command {
            Command::CancelTimer(timer) => {
                o.cancel_timer(TimerWrapper::User(timer));
            }
            Command::SetTimer(timer, duration) => {
                o.set_timer(TimerWrapper::User(timer), duration);
            }
            Command::Send(dst, inner_msg) => {
                o.send(
                    dst,
                    MsgWrapper::Deliver(state.next_send_seq, inner_msg.clone()),
                );
                state
                    .msgs_pending_ack
                    .insert(state.next_send_seq, (dst, inner_msg));
                state.next_send_seq += 1;
                should_save_storage = true;
            }
            Command::ChooseRandom(_, _) => {
                todo!("ChooseRandom is not supported at this time");
            }
            Command::Save(storage) => {
                should_save_storage = true;
                state.wrapped_storage = Some(storage);
            }
        }
    }
    if should_save_storage {
        // Non-volatile fields in Storage are changed, persist them.
        o.save(StorageWrapper {
            next_send_seq: state.next_send_seq,
            msgs_pending_ack: state.msgs_pending_ack.clone(),
            last_delivered_seqs: state.last_delivered_seqs.clone(),
            wrapped_storage: state.wrapped_storage.clone(),
        })
    }
}
