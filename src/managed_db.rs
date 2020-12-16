use crate::db::Agate;
use crate::ops::transaction::Transaction;
use crate::Result;

use std::sync::Arc;

impl crate::db::Core {
    pub fn new_transaction_at(self: &Arc<Self>, read_ts: u64, update: bool) -> Transaction {
        if !self.opts.managed_txns {
            panic!("cannot use with managed db = false");
        }
        let mut txn = self.new_transaction(update);
        txn.read_ts = read_ts;
        txn
    }

    pub fn set_discard_ts(&self, ts: u64) {
        if !self.opts.managed_txns {
            panic!("cannot use with managed db = false");
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
    pub fn commit_at(mut self, commit_ts: u64) -> Result<()> {
        if !self.agate.opts.managed_txns {
            panic!("cannot use with managed db = false");
        }
        self.commit_ts = commit_ts;
        self.commit()?;
        Ok(())
    }
}
