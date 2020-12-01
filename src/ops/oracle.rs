use super::transaction::Transaction;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use bytes::Bytes;

struct CommittedTxn {
    ts: u64,
    conflict_keys: HashSet<u64>,
}

struct CommitInfo {
    next_txn_ts: u64,
    discard_ts: u64,
    committed_txns: Vec<CommittedTxn>,
    last_cleanup_ts: u64,
    is_managed: bool,
}

impl CommitInfo {
    pub fn new(is_managed: bool) -> Self {
        Self {
            next_txn_ts: 0,
            discard_ts: 0,
            committed_txns: vec![],
            last_cleanup_ts: 0,
            is_managed,
        }
    }

    fn cleanup_committed_transactions(&mut self) {
        let max_read_ts;
        if self.is_managed {
            max_read_ts = self.discard_ts;
        } else {
            unimplemented!();
        }

        assert!(max_read_ts >= self.last_cleanup_ts);

        if max_read_ts == self.last_cleanup_ts {
            return;
        }

        self.last_cleanup_ts = max_read_ts;

        self.committed_txns
            .drain_filter(|txn| txn.ts <= max_read_ts);
    }

    fn has_conflict(&self, txn: &Transaction) -> bool {
        let reads = txn.reads.lock().unwrap();
        if reads.is_empty() {
            false
        } else {
            for committed_txn in &self.committed_txns {
                if committed_txn.ts <= txn.read_ts {
                    continue;
                }

                for read in reads.iter() {
                    if committed_txn.conflict_keys.contains(read) {
                        return true;
                    }
                }
            }
            false
        }
    }
}

pub struct Oracle {
    commit_info: Mutex<CommitInfo>,
    is_managed: bool,
    detect_conflicts: bool,
    pub(crate) write_ch_lock: Mutex<()>,
}

impl Oracle {
    pub fn new(is_managed: bool, detect_conflicts: bool) -> Self {
        Self {
            commit_info: Mutex::new(CommitInfo::new(is_managed)),
            is_managed,
            detect_conflicts,
            write_ch_lock: Mutex::new(()),
        }
    }
    fn read_ts(&self) -> u64 {
        if self.is_managed {
            panic!("read_ts should not be used in managed mode");
        } else {
            unimplemented!();
        }
    }

    fn next_ts(&self) -> u64 {
        let commit_info = self.commit_info.lock().unwrap();
        commit_info.next_txn_ts
    }

    fn increment_next_ts(&self) {
        let mut commit_info = self.commit_info.lock().unwrap();
        commit_info.next_txn_ts += 1;
    }

    fn set_discard_ts(&self, discard_ts: u64) {
        let mut commit_info = self.commit_info.lock().unwrap();
        commit_info.discard_ts = discard_ts;
        commit_info.cleanup_committed_transactions();
    }

    fn discard_at_or_below(&self) -> u64 {
        if self.is_managed {
            let commit_info = self.commit_info.lock().unwrap();
            commit_info.discard_ts
        } else {
            unimplemented!();
        }
    }

    pub(crate) fn new_commit_ts(&self, txn: &Transaction) -> u64 {
        let mut commit_info = self.commit_info.lock().unwrap();
        if commit_info.has_conflict(txn) {
            return 0;
        }

        let ts;

        if !self.is_managed {
            unimplemented!();
        } else {
            ts = txn.commit_ts;
        }

        assert!(ts >= commit_info.last_cleanup_ts);

        if self.detect_conflicts {
            commit_info.committed_txns.push(CommittedTxn {
                ts,
                conflict_keys: txn.conflict_keys.clone(),
            })
        }

        ts
    }

    fn done_read(&self, txn: &mut Transaction) {
        if !txn.done_read {
            txn.done_read = true;
            unimplemented!();
        }
    }

    fn cleanup_committed_transactions(&self) {
        if !self.detect_conflicts {
            return;
        } else {
            let mut commit_info = self.commit_info.lock().unwrap();
            commit_info.cleanup_committed_transactions();
        }
    }

    fn done_commit(&self, _commit_ts: u64) {
        if self.is_managed {
            return;
        } else {
            unimplemented!();
        }
    }
}
