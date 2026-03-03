#![allow(dead_code)]

// normalize-stderr-test: "\$DIR/replay.rs:[0-9]+:[0-9]+" -> "$$DIR/replay.rs:LL:CC"
// normalize-stderr-test: "(?m)^   = note: .*\n" -> ""
// normalize-stderr-test: "(?m)^warning: [0-9]+ warnings? emitted\n\n" -> ""

#[derive(Clone, Copy, PartialEq, Eq)]
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
    if mode != ReplayMode::CatchUp {
        let mut txn = index.begin_txn();
        txn.write_rows();
        txn.commit();
    } else {
        let mut rows_txn = index.begin_txn();
        rows_txn.write_rows();
        rows_txn.commit();

        let mut frontier_txn = index.begin_txn();
        frontier_txn.apply_frontier_updates();
        frontier_txn.commit();
    }
}

fn main() {
    replay_index(ReplayMode::CatchUp, &Index);
}
