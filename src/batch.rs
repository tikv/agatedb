use crate::Agate;
use crate::{entry::Entry, ops::transaction::Transaction};
use crate::{Error, Result};

use bytes::Bytes;
use std::sync::Arc;

/// WriteBatch helps write multiple entries to database
pub struct WriteBatch {
    txn: Transaction,
    is_managed: bool,
    commit_ts: u64,
    err: Option<Error>,
    core: Arc<crate::db::Core>,
}

impl Agate {
    pub fn new_write_batch(&self) -> WriteBatch {
        if self.core.opts.managed_txns {
            panic!("cannot use new_write_batch in managed mode");
        }
        WriteBatch {
            txn: self.new_transaction(true),
            is_managed: false,
            commit_ts: 0,
            err: None,
            core: self.core.clone(),
        }
    }

    pub fn new_write_batch_at(&self, commit_ts: u64) -> WriteBatch {
        if !self.core.opts.managed_txns {
            panic!("cannot use new_write_batch_at in non-managed mode");
        }
        let mut txn = self.new_transaction(true);
        txn.commit_ts = commit_ts;
        WriteBatch {
            txn,
            commit_ts,
            is_managed: true,
            err: None,
            core: self.core.clone(),
        }
    }
}

impl WriteBatch {
    pub fn set(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        let entry = Entry::new(key, value);
        self.set_entry(entry)
    }

    pub fn set_entry(&mut self, entry: Entry) -> Result<()> {
        self.handle_entry(entry)
    }

    fn handle_entry(&mut self, entry: Entry) -> Result<()> {
        if let Err(err) = self.txn.set_entry(entry.clone()) {
            if !matches!(err, crate::Error::TxnTooBig(_)) {
                return Err(err);
            }
        } else {
            return Ok(());
        }

        self.commit()?;

        if let Err(err) = self.txn.set_entry(entry) {
            self.err = Some(err.clone());
            return Err(err);
        }

        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        if let Some(err) = &self.err {
            return Err(err.clone());
        }
        let mut new_txn = self.core.new_transaction(true);
        new_txn.commit_ts = self.commit_ts;
        let txn = std::mem::replace(&mut self.txn, new_txn);
        txn.commit()?;

        Ok(())
    }
}
