use crate::db::Agate;
use crate::ops::transaction::Transaction;
use crate::Result;

impl Agate {
    pub fn new_transaction_at(&self, read_ts: u64, update: bool) -> Transaction {
        if !self.core.opts.managed_txns {
            panic!("cannot use with managed db = false");
        }
        let mut txn = self.new_transaction(update);
        txn.read_ts = read_ts;
        txn
    }

    pub fn set_discard_ts(&self, ts: u64) {
        if !self.core.opts.managed_txns {
            panic!("cannot use with managed db = false");
        }

        self.core.orc.set_discard_ts(ts);
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
