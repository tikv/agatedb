use std::sync::Arc;

use crate::{db::Agate, ops::transaction::Transaction, Result};

impl crate::db::Core {
    /// Follows the same logic as `new_transaction`, but uses the provided read timestamp.
    pub fn new_transaction_at(self: &Arc<Self>, read_ts: u64, update: bool) -> Transaction {
        if !self.opts.managed_txns {
            panic!(
                "Cannot use new_transaction_at when managed_txns is false. Use new_transaction instead."
            );
        }

        let mut txn = self.new_transaction(update, true);
        txn.read_ts = read_ts;
        txn
    }

    /// Sets a timestamp at or below which, any invalid or deleted versions can be discarded
    /// from the LSM tree, and thence from the value log to reclaim disk space.
    ///
    /// Can only be used with managed transactions.
    pub fn set_discard_ts(&self, ts: u64) {
        if !self.opts.managed_txns {
            panic!("Cannot use set_discard_ts when managed_txns is false.");
        }

        self.orc.set_discard_ts(ts);
    }
}

impl Agate {
    pub fn new_transaction_at(&self, read_ts: u64, update: bool) -> Transaction {
        self.core.new_transaction_at(read_ts, update)
    }

    pub fn set_discard_ts(&self, ts: u64) {
        self.core.set_discard_ts(ts)
    }
}

impl Transaction {
    /// Commits the transaction, following the same logic as normal commit, but at the given
    /// commit timestamp.
    ///
    /// This will panic if not used with managed transactions.
    pub fn commit_at(mut self, commit_ts: u64) -> Result<()> {
        if !self.core.opts.managed_txns {
            panic!("Cannot use commit_at when managed_txns is false. Use commit_at instead.");
        }
        self.commit_ts = commit_ts;
        self.commit()?;
        Ok(())
    }
}
