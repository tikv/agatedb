use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use bytes::{Bytes, BytesMut};
use skiplist::KeyComparator;

use super::oracle::Oracle;
use crate::{
    entry::Entry,
    format::{append_ts, user_key},
    key_with_ts,
    util::{default_hash, COMPARATOR},
    AgateIterator, AgateOptions, Error, Result, Value,
};

const MAX_KEY_LENGTH: usize = 65000;

pub const AGATE_PREFIX: &[u8] = b"!agate!";
pub const TXN_KEY: &[u8] = b"!agate!txn";

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) commit_ts: u64,
    pub(crate) size: usize,
    pub(crate) count: usize,

    /// Contains fingerprints of keys read.
    pub(crate) reads: Mutex<Vec<u64>>,
    /// Contains fingerprints of keys written. This is used for conflict detection.
    pub(crate) conflict_keys: HashSet<u64>,

    /// Cache stores any writes done by txn.
    pub(crate) pending_writes: HashMap<Bytes, Entry>,
    /// Used in managed mode to store duplicate entries.
    pub(crate) duplicate_writes: Vec<Entry>,

    pub(crate) num_iterators: AtomicUsize,
    pub(crate) discarded: bool,
    pub(crate) done_read: bool,
    // Update is used to conditionally keep track of reads.
    pub(crate) update: bool,

    // TODO: Use Agate rather than AgateOptions and Oracle.
    pub(crate) opts: AgateOptions,
    pub(crate) orc: Arc<Oracle>,
}

pub struct PendingWritesIterator {
    entries: Vec<Entry>,
    next_idx: usize,
    read_ts: u64,
    reversed: bool,
    key: BytesMut,
}

impl Transaction {
    pub(crate) fn new(opts: &AgateOptions) -> Transaction {
        Transaction {
            read_ts: 0,
            commit_ts: 0,
            size: 0,
            count: 0,
            reads: Mutex::new(vec![]),
            conflict_keys: HashSet::new(),
            pending_writes: HashMap::new(),
            duplicate_writes: vec![],
            num_iterators: AtomicUsize::new(0),
            discarded: false,
            done_read: false,
            update: false,
            opts: opts.clone(),
            orc: Arc::new(Oracle::new(opts)),
        }
    }

    pub(crate) fn new_pending_writes_iterator(
        &self,
        reversed: bool,
    ) -> Option<PendingWritesIterator> {
        if !self.update || self.pending_writes.is_empty() {
            return None;
        }

        // As each entry saves key / value as Bytes, there will only be overhead of pointer clone.
        let mut entries: Vec<_> = self.pending_writes.values().cloned().collect();
        entries.sort_by(|x, y| {
            let cmp = COMPARATOR.compare_key(&x.key, &y.key);
            if reversed { cmp.reverse() } else { cmp }
        });

        Some(PendingWritesIterator::new(self.read_ts, reversed, entries))
    }

    fn check_size(&mut self, entry: &Entry) -> Result<()> {
        let count = self.count + 1;
        let opt = &self.opts;
        let size = self.size + entry.estimate_size(opt.value_threshold) as usize + 10;

        if count >= opt.max_batch_count as usize || size >= opt.max_batch_size as usize {
            return Err(Error::TxnTooBig);
        }

        self.count = count;
        self.size = size;

        Ok(())
    }

    fn modify(&mut self, e: Entry) -> Result<()> {
        if !self.update {
            return Err(Error::CustomError(
                "No sets or deletes are allowed in a read-only transaction.".to_string(),
            ));
        }
        if self.discarded {
            return Err(Error::CustomError(
                "This transaction has been discarded. Create a new one.".to_string(),
            ));
        }
        if e.key.is_empty() {
            return Err(Error::EmptyKey);
        }
        if e.key.starts_with(AGATE_PREFIX) {
            return Err(Error::CustomError(
                "Key is using a reserved !agate! prefix".to_string(),
            ));
        }
        if e.key.len() > MAX_KEY_LENGTH {
            return Err(Error::TooLong(format!(
                "key's length > {}: {:?}..",
                MAX_KEY_LENGTH,
                &e.key[..MAX_KEY_LENGTH]
            )));
        }
        let value_log_file_size = self.opts.value_log_file_size as usize;
        if e.value.len() > value_log_file_size {
            return Err(Error::TooLong(format!(
                "value's length > {}: {:?}..",
                value_log_file_size,
                &e.value[..value_log_file_size]
            )));
        }
        if self.opts.in_memory && e.value.len() > self.opts.value_threshold {
            return Err(Error::TooLong(format!(
                "value's length > {}: {:?}..",
                self.opts.value_threshold,
                &e.value[..self.opts.value_threshold]
            )));
        }

        self.check_size(&e)?;

        if self.opts.detect_conflicts {
            self.conflict_keys.insert(default_hash(&e.key));
        }

        // Add the entry to duplicate_writes only if both the entries have different versions.
        // For same versions, we will overwrite the existing entry.
        let version = e.version;
        if let Some(old_entry) = self.pending_writes.insert(e.key.clone(), e) {
            if old_entry.version != version {
                self.duplicate_writes.push(old_entry);
            }
        }
        Ok(())
    }

    /// Adds a key-value pair to the database.
    pub fn set(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        self.set_entry(Entry::new(key, value))?;
        Ok(())
    }

    /// Takes an [`Entry`] struct and adds the key-value pair in the struct,
    /// along with other metadata to the database.
    pub fn set_entry(&mut self, e: Entry) -> Result<()> {
        self.modify(e)?;
        Ok(())
    }

    /// Delete deletes a key.
    ///
    /// This is done by adding a delete marker for the key at commit timestamp.  Any
    /// reads happening before this timestamp would be unaffected. Any reads after
    /// this commit would see the deletion.
    pub fn delete(&mut self, key: Bytes) -> Result<()> {
        let mut e = Entry::new(key, Bytes::new());
        e.mark_delete();
        self.modify(e)?;
        Ok(())
    }

    /// Looks for key and returns corresponding Item.
    pub(crate) fn get(&self, _key: &Bytes) -> Result<Value> {
        unimplemented!()
    }

    pub(crate) fn add_read_key(&self, key: &Bytes) {
        if self.update {
            self.reads.lock().unwrap().push(default_hash(key));
        }
    }

    /// Discards a created transaction.
    ///
    /// This method is very important and must be called. `commit` method calls this
    /// internally, and calling this multiple times doesn't cause any issues.
    pub fn discard(&mut self) {
        if self.discarded {
            // Avoid a re-run.
            return;
        }

        if self.num_iterators.load(Ordering::SeqCst) > 0 {
            panic!("Unclosed iterator at time of Transaction::discard.")
        }

        self.discarded = true;
        if !self.orc.is_managed {
            self.orc.clone().done_read(self);
        }
    }

    fn commit_and_send(&mut self) -> Result<()> {
        let orc = self.orc.clone();
        // Ensure that the order in which we get the commit timestamp is the same as
        // the order in which we push these updates to the write channel. So, we
        // acquire a write_ch_lock before getting a commit timestamp, and only release
        // it after pushing the entries to it.
        let _write_ch_lock = orc.write_ch_lock.lock().unwrap();

        let (commit_ts, conflict) = orc.new_commit_ts(self);
        if conflict {
            return Err(Error::CustomError(
                "Transaction Conflict. Please retry.".to_string(),
            ));
        }

        let mut keep_together = true;
        let set_version = |keep_together: &mut bool, e: &mut Entry| {
            if e.version == 0 {
                e.version = commit_ts;
            } else {
                *keep_together = false;
            }
        };

        self.pending_writes.iter_mut().for_each(|(_, e)| {
            set_version(&mut keep_together, e);
        });

        // The duplicate_writes slice will be non-empty only if there are duplicate
        // entries with different versions.
        self.duplicate_writes.iter_mut().for_each(|e| {
            set_version(&mut keep_together, e);
        });

        let mut entries =
            Vec::with_capacity(self.pending_writes.len() + self.duplicate_writes.len() + 1);

        let process_entry = |entries: &mut Vec<Entry>, mut e: Entry| {
            let mut key = BytesMut::new();
            key.extend_from_slice(&e.key);
            e.key = key_with_ts(key, e.version);
            if keep_together {
                e.meta |= crate::value::VALUE_TXN;
            }
            entries.push(e);
        };

        for (_, e) in self.pending_writes.drain() {
            process_entry(&mut entries, e);
        }

        for e in self.duplicate_writes.drain(..) {
            process_entry(&mut entries, e);
        }

        if keep_together {
            // commit_ts should not be zero if we're inserting transaction markers.
            assert!(commit_ts != 0);
            let mut e = Entry::new(
                key_with_ts(BytesMut::from(TXN_KEY), commit_ts),
                Bytes::from(commit_ts.to_string()),
            );
            e.meta = crate::value::VALUE_FIN_TXN;
            entries.push(e);
        }

        // TODO: Send to write channel.

        orc.done_commit(commit_ts);

        Ok(())
    }

    fn commit_precheck(&self) -> Result<()> {
        if self.discarded {
            return Err(Error::CustomError(
                "Trying to commit a discarded txn.".to_string(),
            ));
        }

        let keep_together = self.pending_writes.iter().all(|(_, e)| e.version == 0);

        // If keep_together is true, it implies transaction markers will be added.
        // In that case, commit_ts should never be zero. This might happen if someone
        // uses Transaction::commit instead of Transaction::commit_at in managed mode.
        // This should happen only in managed mode. In normal mode, keep_together will
        // always be true.
        if keep_together && self.opts.managed_txns && self.commit_ts == 0 {
            return Err(Error::CustomError("commit_ts cannot be zero.".to_string()));
        }

        Ok(())
    }

    /// Commits the transaction, following these steps:
    ///
    /// 1. If there are no writes, return immediately.
    ///
    /// 2. Check if read rows were updated since txn started. If so, return conflict error.
    ///
    /// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
    ///
    /// 4. Batch up all writes, write them to value log and LSM tree.
    ///
    /// 5. If callback is provided, return immediately after checking for conflicts.
    /// Writes to the database will happen in the background. If there is a conflict,
    /// an error will be returned and the callback will not run. If there are no conflicts,
    /// the callback will be called in the background upon successful completion of writes
    /// or any error during write.
    ///
    /// If error is nil, the transaction is successfully committed. In case of a non-nil
    /// error, the LSM tree won't be updated, so there's no need for any rollback.
    pub(crate) fn commit(mut self) -> Result<()> {
        if self.pending_writes.is_empty() {
            return Ok(());
        }

        self.commit_precheck()?;

        self.commit_and_send()?;
        // TODO: Callback.

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.discard();
    }
}

impl PendingWritesIterator {
    fn new(read_ts: u64, reversed: bool, entries: Vec<Entry>) -> Self {
        Self {
            entries,
            next_idx: 0,
            read_ts,
            reversed,
            key: BytesMut::new(),
        }
    }

    fn update_key(&mut self) {
        if self.valid() {
            let entry = &self.entries[self.next_idx];
            self.key.clear();
            self.key.extend_from_slice(&entry.key);
            append_ts(&mut self.key, self.read_ts);
        }
    }
}

impl AgateIterator for PendingWritesIterator {
    fn next(&mut self) {
        self.next_idx += 1;
        self.update_key();
    }

    fn rewind(&mut self) {
        self.next_idx = 0;
        self.update_key();
    }

    fn seek(&mut self, key: &Bytes) {
        use std::cmp::Ordering::*;

        let key = user_key(key);
        self.next_idx = crate::util::search(self.entries.len(), |idx| {
            let cmp = COMPARATOR.compare_key(&self.entries[idx].key, key);
            if !self.reversed {
                cmp != Less
            } else {
                cmp != Greater
            }
        });

        self.update_key();
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        &self.key
    }

    fn value(&self) -> Value {
        assert!(self.valid());
        let entry = &self.entries[self.next_idx];
        Value {
            meta: entry.meta,
            user_meta: entry.user_meta,
            expires_at: entry.expires_at,
            value: entry.value.clone(),
            version: self.read_ts,
        }
    }

    fn valid(&self) -> bool {
        self.next_idx < self.entries.len()
    }
}
