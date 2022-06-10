use std::{cell::RefCell, sync::Arc};

use bytes::{Bytes, BytesMut};
use skiplist::{IterRef, KeyComparator, Skiplist};

use crate::{
    format::user_key,
    get_ts, key_with_ts,
    ops::transaction::{Transaction, AGATE_PREFIX},
    table::{MergeIterator, TableIterators},
    value::{ValuePointer, VALUE_POINTER},
    wal::Wal,
    AgateIterator, Table, Value,
};

pub enum PrefetchStatus {
    No,
    Prefetched,
}

impl Default for PrefetchStatus {
    fn default() -> Self {
        Self::No
    }
}

pub struct Item {
    pub(crate) key: Bytes,
    pub(crate) vptr: Bytes,
    value: RefCell<Bytes>,
    pub(crate) version: u64,
    pub(crate) expires_at: u64,

    pub(crate) status: PrefetchStatus,
    pub(crate) meta: u8,
    pub(crate) user_meta: u8,

    agate: Arc<crate::db::Core>,
}

impl Item {
    pub(crate) fn new(agate: Arc<crate::db::Core>) -> Self {
        Self {
            key: Bytes::new(),
            vptr: Bytes::new(),
            value: RefCell::new(Bytes::new()),
            version: 0,
            expires_at: 0,
            status: PrefetchStatus::No,
            meta: 0,
            user_meta: 0,
            agate,
        }
    }

    pub fn value(&self) -> Bytes {
        if matches!(self.status, PrefetchStatus::No) {
            self.yield_item_value()
        }
        return self.value.borrow().clone();
    }

    fn has_value(&self) -> bool {
        !(self.meta == 0 && self.vptr.is_empty())
    }

    fn yield_item_value(&self) {
        let mut value = self.value.borrow_mut();

        if !self.has_value() {
            *value = Bytes::new();
            return;
        }

        if self.meta & VALUE_POINTER == 0 {
            *value = self.vptr.clone();
            return;
        }

        let mut vptr = ValuePointer::default();
        vptr.decode(&self.vptr);
        let vlog = (*self.agate.vlog).as_ref().unwrap();
        let raw_buffer = vlog.read(vptr);
        if let Ok(mut raw_buffer) = raw_buffer {
            let entry = Wal::decode_entry(&mut raw_buffer).unwrap();
            *value = entry.value;
        } else {
            panic!("Unable to read.");
        }
    }

    pub(crate) fn set_value(&mut self, value: Bytes) {
        self.status = PrefetchStatus::Prefetched;
        *self.value.borrow_mut() = value;
    }
}

pub fn is_deleted_or_expired(meta: u8, expires_at: u64) -> bool {
    if meta & crate::value::VALUE_DELETE != 0 {
        return true;
    }
    if expires_at == 0 {
        return false;
    }
    expires_at <= crate::util::unix_time()
}

/// Used to set options when iterating over key-value stores.
#[derive(Default, Clone)]
pub struct IteratorOptions {
    /// The number of key-value pairs to prefetch while iterating. Valid only if `prefetch_values` is true.
    pub prefetch_size: usize,
    // Indicates whether we should prefetch values during iteration and store them.
    pub prefetch_values: bool,
    /// Direction of iteration. False is forward, true is backward.
    pub reverse: bool,
    /// Fetch all valid versions of the same key.
    pub all_versions: bool,
    /// Used to allow internal access to keys.
    pub internal_access: bool,

    /// If set, use the prefix for bloom filter lookup.
    prefix_is_key: bool,
    /// Only iterate over this given prefix.
    pub prefix: Bytes,
}

impl IteratorOptions {
    pub fn pick_table(&self, _table: &Table) -> bool {
        true
        // TODO: Implement table selection logic.
    }

    /// Picks the necessary table for the iterator. This function also assumes
    /// that the tables are sorted in the right order.
    pub fn pick_tables(&self, _tables: &mut [Table]) {
        // TODO: Implement table selection logic.
    }
}

pub struct SkiplistIterator<C: KeyComparator> {
    skl_iter: IterRef<Skiplist<C>, C>,
}

impl<C: KeyComparator> SkiplistIterator<C> {
    pub fn new(skl_iter: IterRef<Skiplist<C>, C>) -> Self {
        Self { skl_iter }
    }
}

impl<C: KeyComparator> AgateIterator for SkiplistIterator<C> {
    fn next(&mut self) {
        self.skl_iter.next();
    }

    fn rewind(&mut self) {
        self.skl_iter.seek_to_first();
    }

    fn seek(&mut self, key: &Bytes) {
        self.skl_iter.seek(key);
    }

    fn key(&self) -> &[u8] {
        self.skl_iter.key()
    }

    fn value(&self) -> Value {
        let mut value = Value::default();
        value.decode(self.skl_iter.value().clone());
        value
    }

    fn valid(&self) -> bool {
        self.skl_iter.valid()
    }
}

pub struct Iterator {
    pub(crate) table_iter: Box<TableIterators>,
    txn: Arc<Transaction>,
    read_ts: u64,

    opt: IteratorOptions,
    item: Option<Item>,

    /// Used to skip over multiple versions of the same key.
    last_key: BytesMut,
}

impl Transaction {
    /// Returns a new iterator. Depending upon the options, either only keys, or both
    /// key-value pairs would be fetched. The keys are returned in lexicographically sorted order.
    ///
    /// Multiple Iterators:
    /// For a read-only txn, multiple iterators can be running simultaneously. However, for
    /// a read-write txn, iterators have the nuance of being a snapshot of the writes for
    /// the transaction at the time iterator was created. If writes are performed after an
    /// iterator is created, then that iterator will not be able to see those writes. Only
    /// writes performed before an iterator was created can be viewed.
    pub fn new_iterator(self: &Arc<Self>, opt: &IteratorOptions) -> Iterator {
        if self.discarded {
            panic!("Transaction has already been discarded.")
        }

        // TODO: Check DB closed.

        self.num_iterators
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let tables = self.agate.get_mem_tables();
        // We need to get a reference to vlog, to avoid vlog GC (not implemented).
        let _vlog = self.agate.vlog.clone();

        let mut iters: Vec<TableIterators> = vec![];
        if let Some(itr) = self.new_pending_writes_iterator(opt.reverse) {
            iters.push(TableIterators::from(itr));
        }
        for table in tables {
            iters.push(TableIterators::from(SkiplistIterator::new(
                table.skl.iter(),
            )));
        }

        self.agate.lvctl.append_iterators(&mut iters, opt);

        Iterator {
            table_iter: MergeIterator::from_iterators(iters, false),
            txn: self.clone(),
            read_ts: self.read_ts,
            opt: opt.clone(),
            item: None,
            last_key: BytesMut::new(),
        }
    }
}

impl Iterator {
    /// Returns pointer to the current key-value pair.
    /// This item is only valid until `next` gets called.
    pub fn item(&self) -> &Item {
        self.txn.add_read_key(&self.item.as_ref().unwrap().key);
        self.item.as_ref().unwrap()
    }

    /// Returns false when iteration is done.
    pub fn valid(&self) -> bool {
        self.item.is_some()

        // TODO: Check prefix.
    }

    /// Returns false when iteration is done or when the current key is not prefixed by the specified prefix.
    pub fn valid_for_prefix(&self, prefix: &Bytes) -> bool {
        self.valid() && self.item().key.starts_with(&prefix[..])
    }

    // Advances the iterator by one.
    pub fn next(&mut self) {
        while self.table_iter.valid() {
            if self.parse_item() {
                return;
            }
        }

        self.item = None;
    }

    /// Handles both forward and reverse iteration implementation. We store keys such that
    /// their versions are sorted in descending order. This makes forward iteration
    /// efficient, but revese iteration complicated. This tradeoff is better because
    /// forward iteration is more common than reverse.
    ///
    /// This function advances the iterator.
    fn parse_item(&mut self) -> bool {
        let key: &[u8] = self.table_iter.key();

        // Skip Agate keys.
        if !self.opt.internal_access && key.starts_with(AGATE_PREFIX) {
            self.table_iter.next();
            return false;
        }

        // Skip any versions which are beyond the read_ts.
        let version = get_ts(key);
        if version > self.read_ts {
            self.table_iter.next();
            return false;
        }

        if self.opt.all_versions {
            // Return deleted or expired values also, otherwise user can't figure out
            // whether the key was deleted.
            let mut item = Item::new(self.txn.agate.clone());
            Self::fill_item(&mut item, key, &self.table_iter.value(), &self.opt);
            self.item = Some(item);
            self.table_iter.next();
            return true;
        }

        // If iterating in forward direction, then just checking the last key against
        // current key would be sufficient.
        if !self.opt.reverse {
            if crate::util::same_key(&self.last_key, key) {
                self.table_iter.next();
                return false;
            }
            // Only track in forward direction.
            // We should update last_key as soon as we find a different key in our snapshot.
            // Consider keys: a 5, b 7 (del), b 5. When iterating, last_key = a.
            // Then we see b 7, which is deleted. If we don't store last_key = b, we'll then
            // return b 5, which is wrong. Therefore, update last_key here.
            self.last_key.clear();
            self.last_key.extend_from_slice(key);
        }

        let vs = self.table_iter.value();
        if is_deleted_or_expired(vs.meta, vs.expires_at) {
            self.table_iter.next();
            return false;
        }

        let mut item = Item::new(self.txn.agate.clone());
        Self::fill_item(&mut item, key, &self.table_iter.value(), &self.opt);

        self.table_iter.next();
        if !self.opt.reverse || !self.table_iter.valid() {
            self.item = Some(item);
            return true;
        }

        // TODO: Reverse direction.
        unimplemented!();
    }

    fn fill_item(item: &mut Item, key: &[u8], vs: &Value, opts: &IteratorOptions) {
        item.meta = vs.meta;
        item.user_meta = vs.user_meta;
        item.expires_at = vs.expires_at;

        item.version = get_ts(key);
        item.key = Bytes::copy_from_slice(user_key(key));

        item.vptr = vs.value.clone();
        item.value = RefCell::new(Bytes::new());
        item.status = PrefetchStatus::No;

        if opts.prefetch_values {
            unimplemented!();
        }
    }

    /// Seeks to the provided key if present. If absent, it would seek to the next
    /// smallest key greater than the provided key if iterating in the forward direction.
    /// Behavior would be reversed if iterating backwards.
    pub fn seek(&mut self, key: &Bytes) {
        if !key.is_empty() {
            self.txn.add_read_key(key);
        }

        self.last_key.clear();

        // TODO: Prefix.

        if key.is_empty() {
            self.table_iter.rewind();
            self.next();
            return;
        }

        let key = if !self.opt.reverse {
            key_with_ts(BytesMut::from(&key[..]), self.txn.read_ts)
        } else {
            key_with_ts(BytesMut::from(&key[..]), 0)
        };

        self.table_iter.seek(&key);
        self.next();
    }

    /// Rewinds the iterator cursor all the way to zero-th position, which would be the
    /// smallest key if iterating forward, and largest if iterating backward. It does not
    /// keep track of whether the cursor started with a `seek`.
    pub fn rewind(&mut self) {
        self.seek(&Bytes::new());
    }
}
