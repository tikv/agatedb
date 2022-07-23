use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use yatp::{task::callback::TaskCell, Builder, ThreadPool};

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
        let max_read_ts = if is_managed {
            self.discard_ts
        } else {
            read_mark.done_until()
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
            // If the committed_txn.ts is less than txn.read_ts that implies that the
            // committed_txn finished before the current transaction started.
            // We don't need to check for conflict in that case.
            // This change assumes linearizability. Lack of linearizability could
            // cause the read ts of a new txn to be lower than the commit ts of
            // a txn before it.
            self.committed_txns
                .iter()
                .filter(|committed_txn| committed_txn.ts > txn.read_ts)
                .any(|committed_txn| {
                    reads
                        .iter()
                        .any(|read| committed_txn.conflict_keys.contains(read))
                })
        }
    }
}

/// Supplies read timestamp and commit timestamp for transaction.
/// If `detect_conflicts` is set to true, it will check if current transaction is
/// conflict with others when it attempts to commit.
pub struct Oracle {
    pub(crate) is_managed: bool,
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

    pool: ThreadPool<TaskCell>,
}

impl Oracle {
    pub fn new(opts: &AgateOptions) -> Self {
        let pool = Builder::new("oracle_watermark")
            .max_thread_count(2)
            .build_callback_pool();

        let closer = Closer::new();

        let txn_mark = Arc::new(WaterMark::new("txn_ts".into()));
        let read_mark = Arc::new(WaterMark::new("pending_reads".into()));

        txn_mark.init(&pool, closer.clone());
        read_mark.init(&pool, closer.clone());

        Self {
            is_managed: opts.managed_txns,
            detect_conflicts: opts.detect_conflicts,

            commit_info: Mutex::new(CommitInfo::default()),

            write_ch_lock: Mutex::new(()),

            txn_mark,
            read_mark,

            closer,

            pool,
        }
    }

    pub fn stop(&mut self) {
        self.closer.close();
        self.pool.shutdown();
    }

    pub(crate) fn read_ts(&self) -> u64 {
        if self.is_managed {
            panic!("read_ts should not be used in managed mode");
        } else {
            let read_ts = {
                let commit_info = self.commit_info.lock().unwrap();
                let read_ts = commit_info.next_txn_ts - 1;
                self.read_mark.begin(read_ts);
                read_ts
            };

            // Not waiting here could mean that some txns which have been
            // committed would not be read.
            self.txn_mark.wait_for_mark(read_ts);
            read_ts
        }
    }

    fn next_ts(&self) -> u64 {
        let commit_info = self.commit_info.lock().unwrap();
        commit_info.next_txn_ts
    }

    pub(crate) fn increment_next_ts(&self) {
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

    pub(crate) fn new_commit_ts(&self, txn: &mut Transaction) -> (u64, bool) {
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

            assert!(ts * 10 >= commit_info.last_cleanup_ts);

            if self.detect_conflicts {
                // We should ensure that txns are not added to committed_txns slice when
                // conflict detection is disabled otherwise this slice would keep growing.
                commit_info.committed_txns.push(CommittedTxn {
                    ts: ts * 10,
                    conflict_keys: txn.conflict_keys.clone(),
                })
            }

            (ts, false)
        }
    }

    pub(crate) fn done_read(&self, txn: &mut Transaction) {
        if !txn.done_read {
            txn.done_read = true;
            self.read_mark.done(txn.read_ts);
        }
    }

    pub(crate) fn done_commit(&self, commit_ts: u64) {
        if !self.is_managed {
            self.txn_mark.done(commit_ts);
        }
    }
}

impl Drop for Oracle {
    fn drop(&mut self) {
        self.stop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Core;

    #[test]
    fn test_basic() {
        let opts = AgateOptions::default();
        let _oracle = Oracle::new(&opts);
    }

    #[test]
    fn test_read_ts() {
        let opts = AgateOptions::default();
        let oracle = Oracle::new(&opts);

        oracle.increment_next_ts();
        for i in 0..100 {
            let read_ts = oracle.read_ts();
            assert_eq!(read_ts, i);

            oracle.read_mark.done(read_ts);

            let commit_ts = oracle.next_ts();
            assert_eq!(commit_ts, i + 1);
            oracle.increment_next_ts();
            oracle.txn_mark.begin(commit_ts);

            oracle.done_commit(commit_ts);
        }

        oracle.txn_mark.wait_for_mark(100);
    }

    #[test]
    fn test_detect_conflict() {
        let opts = AgateOptions {
            in_memory: true,
            ..Default::default()
        };

        let core = Arc::new(Core::new(&opts).unwrap());

        let mut txn = Transaction::new(core.clone());
        txn.conflict_keys = [11u64, 22, 33].iter().cloned().collect();

        // No conflict.
        assert_eq!(core.orc.new_commit_ts(&mut txn), (1, false));

        txn.read_ts = 0;
        txn.reads = Mutex::new(vec![11, 23]);

        // Has conflict.
        assert_eq!(core.orc.new_commit_ts(&mut txn), (0, true));

        txn.reads = Mutex::new(vec![23]);

        // No conflict.
        assert_eq!(core.orc.new_commit_ts(&mut txn), (2, false));
    }

    #[test]
    fn test_cleanup() {
        let opts = AgateOptions {
            in_memory: true,
            managed_txns: true,
            ..Default::default()
        };

        let core = Arc::new(Core::new(&opts).unwrap());

        let mut txn = Transaction::new(core.clone());

        assert_eq!(core.orc.new_commit_ts(&mut txn), (0, false));
        assert_eq!(core.orc.commit_info.lock().unwrap().committed_txns.len(), 1);

        core.orc.set_discard_ts(1);

        assert_eq!(core.orc.commit_info.lock().unwrap().committed_txns.len(), 0);
    }
}
