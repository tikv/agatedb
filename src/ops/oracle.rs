use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use super::transaction::Transaction;
use crate::{closer::Closer, watermark::WaterMark, AgateOptions};

struct CommittedTxn {
    ts: u64,
    // Keeps track of the entries written at timestamp ts.
    conflict_keys: HashSet<u64>,
}

#[derive(Default)]
struct CommitInfo {
    next_txn_ts: u64,
    /// Used by managed DB.
    discard_ts: u64,
    /// Contains all committed writes (contains fingerprints of keys written
    /// and their latest commit counter).
    committed_txns: Vec<CommittedTxn>,
    last_cleanup_ts: u64,
}

impl CommitInfo {
    fn cleanup_committed_transactions(&mut self, is_managed: bool, read_mark: Arc<WaterMark>) {
        let max_read_ts = {
            if is_managed {
                self.discard_ts
            } else {
                read_mark.done_until()
            }
        };

        assert!(max_read_ts >= self.last_cleanup_ts);

        if max_read_ts == self.last_cleanup_ts {
            return;
        }
        self.last_cleanup_ts = max_read_ts;

        self.committed_txns.retain(|txn| txn.ts > max_read_ts);
    }

    fn has_conflict(&self, txn: &Transaction) -> bool {
        let reads = txn.reads.lock().unwrap();

        if reads.is_empty() {
            false
        } else {
            for committed_txn in &self.committed_txns {
                // If the committed_txn.ts is less than txn.read_ts that implies that the
                // committed_txn finished before the current transaction started.
                // We don't need to check for conflict in that case.
                // This change assumes linearizability. Lack of linearizability could
                // cause the read ts of a new txn to be lower than the commit ts of
                // a txn before it.
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
    is_managed: bool,
    /// Determines if the txns should be checked for conflicts.
    detect_conflicts: bool,

    commit_info: Mutex<CommitInfo>,

    /// Ensures that transactions go to the write channel in the same order
    /// as their commit timestamps.
    pub(crate) write_ch_lock: Mutex<()>,

    /// Used to block `new_transaction`, so all previous commits are visible to a new read.
    txn_mark: Arc<WaterMark>,

    /// Used by DB.
    read_mark: Arc<WaterMark>,

    /// Used to stop watermarks.
    closer: Closer,
}

impl Oracle {
    pub fn new(opts: &AgateOptions) -> Self {
        let oracle = Self {
            is_managed: opts.managed_txns,
            detect_conflicts: opts.detect_conflicts,

            commit_info: Mutex::new(CommitInfo::default()),

            write_ch_lock: Mutex::new(()),

            txn_mark: Arc::new(WaterMark::new("txn_ts".into())),
            read_mark: Arc::new(WaterMark::new("pending_reads".into())),

            closer: Closer::new(),
        };

        let txn_pool = std::thread::Builder::new().name("oracle_txn_watermark".into());
        let read_pool = std::thread::Builder::new().name("oracle_read_watermark".into());

        oracle.txn_mark.init(txn_pool, oracle.closer.clone());
        oracle.read_mark.init(read_pool, oracle.closer.clone());

        oracle
    }

    pub fn stop(&self) {
        todo!()
    }

    fn read_ts(&self) -> u64 {
        if self.is_managed {
            panic!("read_ts should not be used in managed mode");
        } else {
            let read_ts = {
                let commit_info = self.commit_info.lock().unwrap();
                let read_ts = commit_info.next_txn_ts - 1;
                self.read_mark.begin(read_ts);
                read_ts
            };

            self.txn_mark.wait_for_mark(read_ts);
            read_ts
        }
    }

    fn next_ts(&self) -> u64 {
        let commit_info = self.commit_info.lock().unwrap();
        commit_info.next_txn_ts
    }

    fn increment_next_ts(&self) {
        let mut commit_info = self.commit_info.lock().unwrap();
        commit_info.next_txn_ts += 1
    }

    /// Any deleted or invalid versions at or below ts would be discarded during
    /// compaction to reclaim disk space in LSM tree and thence value log.
    pub(crate) fn set_discard_ts(&self, discard_ts: u64) {
        let mut commit_info = self.commit_info.lock().unwrap();
        commit_info.discard_ts = discard_ts;
        commit_info.cleanup_committed_transactions(self.is_managed, self.read_mark.clone());
    }

    pub(crate) fn discard_at_or_below(&self) -> u64 {
        if self.is_managed {
            let commit_info = self.commit_info.lock().unwrap();
            commit_info.discard_ts
        } else {
            self.read_mark.done_until()
        }
    }

    pub(crate) fn new_commit_ts(&mut self, txn: &mut Transaction) -> (u64, bool) {
        let mut commit_info = self.commit_info.lock().unwrap();

        if commit_info.has_conflict(txn) {
            (0, true)
        } else {
            let ts = if !self.is_managed {
                self.done_read(txn);
                commit_info.cleanup_committed_transactions(self.is_managed, self.read_mark.clone());

                let txn_ts = commit_info.next_txn_ts;
                commit_info.next_txn_ts += 1;
                self.txn_mark.begin(txn_ts);
                txn_ts
            } else {
                txn.commit_ts
            };

            assert!(ts >= commit_info.last_cleanup_ts);

            if self.detect_conflicts {
                // We should ensure that txns are not added to committed_txns slice when
                // conflict detection is disabled otherwise this slice would keep growing.
                commit_info.committed_txns.push(CommittedTxn {
                    ts,
                    conflict_keys: txn.conflict_keys.clone(),
                })
            }

            (ts, false)
        }
    }

    fn done_read(&self, txn: &mut Transaction) {
        if !txn.done_read {
            txn.done_read = true;
            self.read_mark.done(txn.read_ts)
        }
    }

    fn cleanup_committed_transactions(&self) {
        if self.detect_conflicts {
            let mut commit_info = self.commit_info.lock().unwrap();
            commit_info.cleanup_committed_transactions(self.is_managed, self.read_mark.clone());
        }
    }

    pub(crate) fn done_commit(&self, commit_ts: u64) {
        if !self.is_managed {
            self.txn_mark.done(commit_ts);
        }
    }
}
