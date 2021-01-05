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
        let mut txn = self.core.new_transaction(true, true);
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
            if !matches!(err, crate::Error::TxnTooBig) {
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
        let mut new_txn = self.core.new_transaction(true, true);
        new_txn.commit_ts = self.commit_ts;
        let txn = std::mem::replace(&mut self.txn, new_txn);
        txn.commit()?;

        Ok(())
    }

    fn delete(&mut self, key: Bytes) -> Result<()> {
        if let Err(err) = self.txn.delete(key.clone()) {
            if !matches!(err, Error::TxnTooBig) {
                return Err(err);
            }
        }
        self.commit()?;
        if let Err(err) = self.txn.delete(key) {
            self.err = Some(err.clone());
            return Err(err);
        }

        Ok(())
    }

    fn flush(mut self) -> Result<()> {
        self.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::tests::{generate_test_agate_options, run_agate_test},
        AgateOptions,
    };
    use bytes::Bytes;

    fn test_with_options(opts: AgateOptions) {
        let key = |i| Bytes::from(format!("{:10}", i));
        let value = |i| Bytes::from(format!("{:128}", i));

        run_agate_test(Some(opts), move |agate| {
            let mut wb = agate.new_write_batch_at(1);
            const N: usize = 1000;
            const M: usize = 500;
            for i in 0..N {
                wb.set(key(i), value(i)).unwrap();
            }
            for i in 0..M {
                wb.delete(key(i)).unwrap();
            }
            wb.flush().unwrap();
        })
    }

    #[test]
    fn test_on_disk() {
        let mut opts = generate_test_agate_options();
        opts.managed_txns = true;
        opts.value_threshold = 32;
        test_with_options(opts);
    }

    #[test]
    fn test_in_memory() {
        let mut opts = generate_test_agate_options();
        opts.managed_txns = true;
        opts.in_memory = true;
        test_with_options(opts);
    }

    #[test]
    fn test_empty_write_batch() {
        let opts = AgateOptions {
            managed_txns: true,
            ..Default::default()
        };

        run_agate_test(Some(opts), |agate| {
            let wb = agate.new_write_batch_at(2);
            wb.flush().unwrap();
            let wb = agate.new_write_batch_at(208);
            wb.flush().unwrap();
            let wb = agate.new_write_batch_at(31);
            wb.flush().unwrap();
        })
    }
}
