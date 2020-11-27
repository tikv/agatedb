use crate::{
    db::Agate,
    format::{append_ts, user_key},
    key_with_ts,
    util::{KeyComparator, COMPARATOR},
    AgateIterator, Value,
};
use crate::{entry::Entry, util::default_hash};
use crate::{Error, Result};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
};

const MAX_KEY_LENGTH: usize = 65000;

const AGATE_PREFIX: &[u8] = b"!agate!";

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) commit_ts: u64,
    pub(crate) size: usize,
    pub(crate) count: usize,
    pub(crate) reads: Vec<u64>,

    pub(crate) conflict_keys: HashSet<u64>,

    pub(crate) pending_writes: HashMap<Bytes, Entry>,
    pub(crate) duplicate_writes: Vec<Entry>,

    pub(crate) num_iterators: usize,
    pub(crate) discarded: bool,
    pub(crate) done_read: bool,
    pub(crate) update: bool,

    agate: Arc<crate::db::Core>,
}

impl Agate {
    pub fn new_transaction(&self, update: bool) -> Transaction {
        Transaction {
            read_ts: 0,
            commit_ts: 0,
            size: 0,
            count: 0,
            reads: vec![],
            conflict_keys: HashSet::new(),
            pending_writes: HashMap::new(),
            duplicate_writes: vec![],
            discarded: false,
            done_read: false,
            update,
            num_iterators: 0,
            agate: self.core.clone(),
        }
    }
}

impl Transaction {
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

    fn new_pending_write_iterator(&self, reversed: bool) -> Option<PendingWritesIterator> {
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
        Ok(Item {})
    }
}

struct PendingWritesIterator {
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
            self.key.clone_from_slice(&entry.key);
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

pub struct Item {}
