use crate::{
    db::Agate,
    format::{append_ts, user_key},
    iterator::Item,
    iterator::{is_deleted_or_expired, PrefetchStatus},
    key_with_ts,
    util::{KeyComparator, COMPARATOR},
    AgateIterator, Value,
};
use crate::{entry::Entry, util::default_hash};
use crate::{Error, Result};
use bytes::{Bytes, BytesMut};
use std::collections::{HashMap, HashSet};
use std::sync::{atomic::AtomicUsize, Arc, Mutex};

const MAX_KEY_LENGTH: usize = 65000;

pub const AGATE_PREFIX: &[u8] = b"!agate!";
pub const TXN_KEY: &[u8] = b"!agate!txn";

// TODO: a discarded transaction must have been dropped.
// we don't need to handle related logic inside Transaction.
pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) commit_ts: u64,
    pub(crate) size: usize,
    pub(crate) count: usize,
    pub(crate) reads: Mutex<Vec<u64>>,

    // Chances are that keys may collide in HashSet. We should come
    // up with a better design.
    pub(crate) conflict_keys: HashSet<u64>,

    pub(crate) pending_writes: HashMap<Bytes, Entry>,
    pub(crate) duplicate_writes: Vec<Entry>,

    pub(crate) num_iterators: AtomicUsize,
    pub(crate) discarded: bool,
    pub(crate) done_read: bool,
    pub(crate) update: bool,

    pub(crate) agate: Arc<crate::db::Core>,
}

impl Agate {
    pub fn new_transaction(&self, update: bool) -> Transaction {
        // TODO: read-only database
        let mut txn = Transaction::new(self.core.clone());
        txn.update = update;
        txn.count = 1;
        txn.size = TXN_KEY.len() + 10;
        // TODO: allocate transaction conflict_keys and pending_writes on demand
        // TODO: not is_managed
        txn
    }

    pub fn update(&self, f: impl FnOnce(&mut Transaction) -> Result<()>) -> Result<()> {
        if self.core.opts.managed_txns {
            panic!("update can obly be used with managed_db=false");
        }
        let mut txn = self.new_transaction(true);
        f(&mut txn)?;
        txn.commit()?;
        Ok(())
    }

    pub fn view(&self, f: impl FnOnce(&mut Transaction) -> Result<()>) -> Result<()> {
        // TODO: check closed
        let mut txn;
        if self.core.opts.managed_txns {
            txn = self.new_transaction_at(std::u64::MAX, false);
        } else {
            txn = self.new_transaction(false);
        }
        f(&mut txn)?;
        Ok(())
    }
}

impl Transaction {
    fn new(core: Arc<crate::db::Core>) -> Transaction {
        Transaction {
            read_ts: 0,
            commit_ts: 0,
            size: 0,
            count: 0,
            reads: Mutex::new(vec![]),
            conflict_keys: HashSet::new(),
            pending_writes: HashMap::new(),
            duplicate_writes: vec![],
            discarded: false,
            done_read: false,
            update: false,
            num_iterators: AtomicUsize::new(0),
            agate: core,
        }
    }

    pub fn set(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        self.set_entry(Entry::new(key, value))?;
        Ok(())
    }

    pub fn delete(&mut self, key: Bytes) -> Result<()> {
        let mut e = Entry::new(key, Bytes::new());
        e.mark_delete();
        self.modify(e)?;
        Ok(())
    }

    fn modify(&mut self, e: Entry) -> Result<()> {
        if !self.update {
            return Err(Error::CustomError("read only txn".to_string()));
        }
        if self.discarded {
            return Err(Error::CustomError("txn discarded".to_string()));
        }
        if e.key.is_empty() {
            return Err(Error::EmptyKey);
        }
        if e.key.starts_with(AGATE_PREFIX) {
            return Err(Error::CustomError("invalid key".to_string()));
        }
        if e.key.len() > MAX_KEY_LENGTH {
            return Err(Error::TooLong(format!(
                "key's length > {}: {:?}..",
                MAX_KEY_LENGTH,
                &e.key[..MAX_KEY_LENGTH]
            )));
        }

        let value_log_file_size = self.agate.opts.value_log_file_size as usize;
        if e.value.len() > value_log_file_size {
            return Err(Error::TooLong(format!(
                "value's length > {}: {:?}..",
                value_log_file_size,
                &e.value[..value_log_file_size]
            )));
        }
        if self.agate.opts.in_memory && e.value.len() > self.agate.opts.value_threshold {
            return Err(Error::TooLong(format!(
                "value's length > {}: {:?}..",
                self.agate.opts.value_threshold,
                &e.value[..self.agate.opts.value_threshold]
            )));
        }
        self.check_size(&e)?;
        if self.agate.opts.detect_conflicts {
            self.conflict_keys.insert(default_hash(&e.key));
        }
        if let Some(old_entry) = self.pending_writes.get(&e.key) {
            if old_entry.version != e.version {
                self.duplicate_writes.push(old_entry.clone());
            }
        }
        self.pending_writes.insert(e.key.clone(), e);
        Ok(())
    }

    pub(crate) fn new_pending_write_iterator(
        &self,
        reversed: bool,
    ) -> Option<PendingWritesIterator> {
        if !self.update || self.pending_writes.is_empty() {
            return None;
        }

        // As each entry saves key / value as Bytes, there will only be overhead of pointer clone
        let mut entries: Vec<_> = self.pending_writes.values().cloned().collect();
        entries.sort_by(|x, y| {
            let cmp = COMPARATOR.compare_key(&x.key, &y.key);
            if reversed {
                cmp.reverse()
            } else {
                cmp
            }
        });

        Some(PendingWritesIterator::new(self.read_ts, reversed, entries))
    }

    fn check_size(&mut self, entry: &Entry) -> Result<()> {
        let count = self.count + 1;
        let opt = &self.agate.opts;
        let size = self.size + entry.estimate_size(opt.value_threshold) as usize + 10;

        if count >= opt.max_batch_count as usize || size >= opt.max_batch_size as usize {
            return Err(Error::TxnTooBig("".to_string()));
        }

        self.count = count;
        self.size = size;

        Ok(())
    }

    pub fn set_entry(&mut self, e: Entry) -> Result<()> {
        self.modify(e)?;
        Ok(())
    }

    pub fn get(&self, key: &Bytes) -> Result<Item> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        } else if self.discarded {
            return Err(Error::CustomError("txn discarded".to_string()));
        }

        if self.update {
            if let Some(entry) = self.pending_writes.get(key) {
                if key == &entry.key {
                    if is_deleted_or_expired(entry.meta, entry.expires_at) {
                        return Err(Error::KeyNotFound(()));
                    }
                    return Ok(Item {
                        meta: entry.meta,
                        value: entry.value.clone(),
                        user_meta: entry.user_meta,
                        key: entry.key.clone(),
                        status: PrefetchStatus::Prefetched,
                        version: self.read_ts,
                        expires_at: entry.expires_at,
                        vptr: Bytes::new(),
                    });
                }
            }

            self.add_read_key(key);
        }

        let mut key_with_ts = BytesMut::new();
        key_with_ts.extend_from_slice(key);
        append_ts(&mut key_with_ts, self.read_ts);

        let seek = key_with_ts.freeze();

        let vs = self.agate.get(&seek)?;

        if vs.value.is_empty() && vs.meta == 0 {
            return Err(Error::KeyNotFound(()));
        }

        if is_deleted_or_expired(vs.meta, vs.expires_at) {
            return Err(Error::KeyNotFound(()));
        }

        Ok(Item {
            key: key.clone(),
            version: vs.version,
            meta: vs.meta,
            user_meta: vs.user_meta,
            vptr: vs.value,
            expires_at: vs.expires_at,
            value: Bytes::new(),
            status: PrefetchStatus::No,
        })
    }

    pub(crate) fn add_read_key(&self, key: &Bytes) {
        if self.update {
            self.reads.lock().unwrap().push(default_hash(key));
        }
    }

    fn commit_precheck(&self) -> Result<()> {
        if self.discarded {
            return Err(Error::CustomError("commit a discarded txn".to_string()));
        }
        let mut keep_together = true;
        for (_, entry) in &self.pending_writes {
            if entry.version != 0 {
                keep_together = false;
            }
        }

        if keep_together && self.agate.opts.managed_txns && self.commit_ts == 0 {
            return Err(Error::CustomError("commit ts cannot be zero".to_string()));
        }

        Ok(())
    }

    fn commit_and_send(&mut self) -> Result<()> {
        let orc = self.agate.orc.clone();
        let write_ch_lock = orc.write_ch_lock.lock().unwrap();
        let commit_ts = orc.new_commit_ts(&self);
        if commit_ts == 0 && !self.agate.opts.managed_txns {
            return Err(Error::CustomError("conflict".to_string()));
        }

        let mut keep_together = true;
        let set_version = |keep_together: &mut bool, e: &mut Entry| {
            if e.version == 0 {
                e.version = commit_ts;
            } else {
                *keep_together = false;
            }
        };

        for (_, e) in &mut self.pending_writes {
            set_version(&mut keep_together, e);
        }

        for e in &mut self.duplicate_writes {
            set_version(&mut keep_together, e);
        }

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
            assert!(commit_ts != 0);
            let mut e = Entry::new(
                key_with_ts(BytesMut::from(TXN_KEY), commit_ts),
                Bytes::from(commit_ts.to_string()),
            );
            e.meta = crate::value::VALUE_FIN_TXN;
            entries.push(e);
        }

        let done = self.agate.send_to_write_channel(entries);

        println!("--- sent to channel ---");

        if let Err(err) = done {
            orc.done_commit(commit_ts);
            return Err(err);
        }

        drop(write_ch_lock);

        let done = done.unwrap();

        done.recv().unwrap()
    }

    pub(crate) fn commit(mut self) -> Result<()> {
        if self.pending_writes.is_empty() {
            return Ok(());
        }

        self.commit_precheck()?;

        self.commit_and_send()?;
        // TODO: callback

        Ok(())
    }

    fn discard_inner(&mut self) {}

    pub fn discard(mut self) {
        self.discard_inner()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.discard_inner();
    }
}

pub struct PendingWritesIterator {
    entries: Vec<Entry>,
    next_idx: usize,
    read_ts: u64,
    reversed: bool,
    key: BytesMut,
}

impl PendingWritesIterator {
    fn new(read_ts: u64, reversed: bool, entries: Vec<Entry>) -> Self {
        Self {
            entries,
            reversed,
            read_ts,
            key: BytesMut::new(),
            next_idx: 0,
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
            value: entry.value.clone(),
            meta: entry.meta,
            user_meta: entry.user_meta,
            expires_at: entry.expires_at,
            version: self.read_ts,
        }
    }

    fn valid(&self) -> bool {
        self.next_idx < self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::db::tests::with_agate_test;
    use bytes::Bytes;

    #[test]
    fn test_basic_transaction() {
        with_agate_test(|agate| {
            let key = Bytes::from("233333".to_string());
            let val = Bytes::from("23333333333".to_string());
            let mut txn = agate.new_transaction_at(2333333, true);
            println!("---started---");
            txn.set(key.clone(), val.clone()).unwrap();
            println!("---set---");
            txn.commit_at(2333334).unwrap();
            println!("---commit---");
            let txn = agate.new_transaction_at(2333335, false);
            println!("---started---");
            assert_eq!(txn.get(&key).unwrap().vptr, val);
            println!("---get---");
            txn.commit_at(2333336).unwrap();
            println!("---commit---");
            let txn = agate.new_transaction_at(2333332, false);
            println!("---started---");
            assert!(txn.get(&key).is_err());
            println!("---get---");
            txn.commit_at(2333337).unwrap();
            println!("---commit---");
        });
    }
}
