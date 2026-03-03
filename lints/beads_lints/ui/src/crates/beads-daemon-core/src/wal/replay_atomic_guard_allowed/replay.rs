#![allow(dead_code)]

#[derive(Clone, Copy)]
enum ReplayMode {
    Rebuild,
    CatchUp,
}

struct Index;
struct Txn;

impl Index {
    fn begin_txn(&self) -> Txn {
        Txn
    }
}

impl Txn {
    fn write_rows(&mut self) {}
    fn apply_frontier_updates(&mut self) {}
    fn commit(&mut self) {}
}

fn replay_index(mode: ReplayMode, index: &Index) {
    match mode {
        ReplayMode::CatchUp => {
            let mut txn = index.begin_txn();
            txn.write_rows();
            txn.apply_frontier_updates();
            txn.commit();
        }
        ReplayMode::Rebuild => {
            let mut txn = index.begin_txn();
            txn.write_rows();
            txn.commit();
        }
    }
}

fn main() {
    replay_index(ReplayMode::CatchUp, &Index);
}
