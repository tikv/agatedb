use std::sync::Arc;

use bytes::Bytes;

use crate::{entry::Entry, ops::transaction::Transaction, Agate, Error, Result};

/// WriteBatch helps write multiple entries to database
pub struct WriteBatch {
    txn: Transaction,
    core: Arc<crate::db::Core>,
    err: Option<Error>,

    is_managed: bool,
    commit_ts: u64,
    // TODO: Add finished and support concurrency.
}

impl Agate {
    /// Creates a new [`WriteBatch`]. This provides a way to conveniently do a lot of writes,
    /// batching them up as tightly as possible in a single transaction, thus achieving good
    /// performance. This API hides away the logic of creating and committing transactions.
    /// Due to the nature of SSI guaratees provided by Agate, blind writes can never encounter
    /// transaction conflicts.
    pub fn new_write_batch(&self) -> WriteBatch {
        if self.core.opts.managed_txns {
            panic!("Cannot use new_write_batch in managed mode. Use new_write_batch_at instead");
        }

        WriteBatch {
            txn: self.core.new_transaction(true, true),
            core: self.core.clone(),
            err: None,
            is_managed: false,
            commit_ts: 0,
        }
    }

    /// Similar to `new_write_batch` but it allows user to set the commit timestamp.
    pub fn new_write_batch_at(&self, commit_ts: u64) -> WriteBatch {
        if !self.core.opts.managed_txns {
            panic!(
                "Cannot use new_write_batch_at in non-managed mode. Use new_write_batch instead."
            );
        }

        let mut txn = self.core.new_transaction(true, true);
        txn.commit_ts = commit_ts;
        WriteBatch {
            txn,
            core: self.core.clone(),
            err: None,
            is_managed: true,
            commit_ts,
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
                self.err = Some(err.clone());
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

        let mut new_txn = self.core.new_transaction(true, self.is_managed);
        new_txn.commit_ts = self.commit_ts;
        let txn = std::mem::replace(&mut self.txn, new_txn);
        txn.commit()?;

        Ok(())
    }

    fn delete(&mut self, key: Bytes) -> Result<()> {
        if let Err(err) = self.txn.delete(key.clone()) {
            if !matches!(err, Error::TxnTooBig) {
                self.err = Some(err.clone());
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

    /// Must be called at the end to ensure that any pending writes get committed to Agate.
    /// Returns any error stored by [`WriteBatch`].
    fn flush(mut self) -> Result<()> {
        self.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{
        db::tests::{generate_test_agate_options, run_agate_test},
        AgateOptions,
    };

    fn test_with_options(opts: AgateOptions) {
        let key = |i| Bytes::from(format!("{:10}", i));
        let value = |i| Bytes::from(format!("{:128}", i));

        run_agate_test(Some(opts.clone()), move |agate| {
            let mut wb = if !opts.managed_txns {
                agate.new_write_batch()
            } else {
                agate.new_write_batch_at(1)
            };

            // Do not set too large to avoid out of memory.
            const N: usize = 100;
            const M: usize = 100;
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
        opts.value_threshold = 32;

        test_with_options(opts.clone());

        opts.managed_txns = true;
        test_with_options(opts);
    }

    #[test]
    fn test_in_memory() {
        let mut opts = generate_test_agate_options();
        opts.in_memory = true;

        test_with_options(opts.clone());

        opts.managed_txns = true;
        test_with_options(opts);
    }

    #[test]
    fn test_empty_write_batch() {
        run_agate_test(None, |agate| {
            let wb = agate.new_write_batch();
            wb.flush().unwrap();
        });

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
