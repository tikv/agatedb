use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use bytes::Bytes;

use crate::{db::Agate, entry::Entry, Error, Result};

const MAX_KEY_LENGTH: usize = 65000;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) commit_ts: u64,

    update: bool,
    pending_writes: HashMap<Bytes, Entry>,
    agate: Agate,

    pub(crate) reads: Mutex<Vec<u64>>,
    pub(crate) conflict_keys: HashSet<u64>,
    pub(crate) done_read: bool,
}

impl Agate {
    pub fn new_transaction(&self, update: bool) -> Transaction {
        Transaction {
            read_ts: 0,
            commit_ts: 0,
            update,
            pending_writes: HashMap::default(),
            agate: self.clone(),

            reads: Mutex::new(Vec::new()),
            conflict_keys: HashSet::new(),
            done_read: false,
        }
    }
}

impl Transaction {
    pub fn set(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        self.modify(Entry::new(key, value))
    }

    pub fn delete(&mut self, key: Bytes) -> Result<()> {
        let mut e = Entry::new(key, Bytes::new());
        e.mark_delete();
        self.modify(e)
    }

    fn modify(&mut self, e: Entry) -> Result<()> {
        if e.key.is_empty() {
            return Err(Error::EmptyKey);
        }
        if e.key.len() > MAX_KEY_LENGTH {
            return Err(Error::TooLong(format!(
                "key's length > {}: {:?}..",
                MAX_KEY_LENGTH,
                &e.key[..MAX_KEY_LENGTH]
            )));
        }
        self.pending_writes.insert(e.key.clone(), e);
        Ok(())
    }
}
