use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Oracle {
    next_txn_ts: AtomicU64,
    discard_ts: AtomicU64,
}

impl Oracle {
    pub fn read_ts(&self) -> u64 {
        self.next_txn_ts.load(Ordering::SeqCst) - 1
    }

    pub fn next_ts(&self) -> u64 {
        self.next_txn_ts.load(Ordering::SeqCst)
    }

    pub fn increment_next_ts(&self) {
        self.next_txn_ts.fetch_add(1, Ordering::SeqCst);
    }

    pub fn set_discard_ts(&self, discard_ts: u64) {
        self.discard_ts.store(discard_ts, Ordering::SeqCst);
    }

    pub(crate) fn discard_at_or_below(&self) -> u64 {
        // TODO: Implement it.
        self.discard_ts.load(Ordering::SeqCst)
    }
}
