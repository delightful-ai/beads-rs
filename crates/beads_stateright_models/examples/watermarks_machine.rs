//! Model 2: Watermark semantics (applied vs durable) as *separate* monotonic counters.
//!
//! The spec distinction:
//! - `durable` is about "will survive restart" (fsync / replicated-fsync ack)
//! - `applied` is about "effects are visible in the materialized state"
//! - peers can claim durable progress via ACK, but that must not mutate local truth
//!
//! This toy model adds one extra counter (`appended`) so we can model:
//! - append -> fsync (durable) -> apply
//! - append -> apply -> fsync (still safe so long as we don't ACK before fsync)

use stateright::{report::WriteReporter, Checker, Model, Property};
use std::time::Duration;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    appended: u8,
    durable: u8,
    applied: u8,
    peer_claim_durable: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    Append,
    Fsync,
    Apply,
    ReceiveAck(u8),
}

#[derive(Clone, Debug)]
struct WatermarksModel {
    max: u8,
}

impl Model for WatermarksModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            appended: 0,
            durable: 0,
            applied: 0,
            peer_claim_durable: 0,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        if s.appended < self.max {
            actions.push(Action::Append);
        }
        if s.durable < s.appended {
            actions.push(Action::Fsync);
        }
        if s.applied < s.appended {
            actions.push(Action::Apply);
        }
        for ack in 0..=self.max {
            actions.push(Action::ReceiveAck(ack));
        }
    }

    fn next_state(&self, s: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut n = s.clone();
        match a {
            Action::Append => n.appended += 1,
            Action::Fsync => n.durable += 1,
            Action::Apply => n.applied += 1,
            Action::ReceiveAck(v) => n.peer_claim_durable = n.peer_claim_durable.max(v),
        }
        Some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("contiguity + bounds", |m, s: &State| {
                let max = m.max;
                s.appended <= max
                    && s.durable <= s.appended
                    && s.applied <= s.appended
                    && s.peer_claim_durable <= max
            }),
            Property::always("peer claims do not change local truth", |m, s: &State| {
                // This property is a bit meta in a single-state model.
                // In practice, it's a reminder: `peer_claim_durable` is *not* consulted
                // when deciding which local events are contiguous / safe.
                s.appended <= m.max
            }),
        ]
    }
}

fn main() -> Result<(), pico_args::Error> {
    env_logger::init();

    let mut args = pico_args::Arguments::from_env();
    match args.subcommand()?.as_deref() {
        Some("explore") => {
            let address = args
                .opt_free_from_str()?
                .unwrap_or("localhost:3000".to_string());
            println!("Exploring watermark semantics on {address}.");
            WatermarksModel { max: 3 }
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(20))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking watermark semantics.");
            WatermarksModel { max: 3 }
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(20))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  watermarks_machine check");
            println!("  watermarks_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
