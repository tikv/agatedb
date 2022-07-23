use std::{
    cell::RefCell,
    sync::{atomic::Ordering, Arc},
};

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

    core: Arc<crate::db::Core>,
}

impl Item {
    pub(crate) fn new(core: Arc<crate::db::Core>) -> Self {
        Self {
            key: Bytes::new(),
            vptr: Bytes::new(),
            value: RefCell::new(Bytes::new()),
            version: 0,
            expires_at: 0,
            status: PrefetchStatus::No,
            meta: 0,
            user_meta: 0,
            core,
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
        let vlog = (*self.core.vlog).as_ref().unwrap();
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
    pub prefix_is_key: bool,
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
    reversed: bool,
}

impl<C: KeyComparator> SkiplistIterator<C> {
    pub fn new(skl_iter: IterRef<Skiplist<C>, C>, reversed: bool) -> Self {
        Self { skl_iter, reversed }
    }
}

impl<C: KeyComparator> AgateIterator for SkiplistIterator<C> {
    fn next(&mut self) {
        if !self.reversed {
            self.skl_iter.next();
        } else {
            self.skl_iter.prev();
        }
    }

    fn rewind(&mut self) {
        if !self.reversed {
            self.skl_iter.seek_to_first();
        } else {
            self.skl_iter.seek_to_last();
        }
    }

    fn seek(&mut self, key: &Bytes) {
        if !self.reversed {
            self.skl_iter.seek(key);
        } else {
            self.skl_iter.seek_for_prev(key);
        }
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

    fn prev(&mut self) {
        if !self.reversed {
            self.skl_iter.prev();
        } else {
            self.skl_iter.next();
        }
    }

    fn to_last(&mut self) {
        if !self.reversed {
            self.skl_iter.seek_to_last();
        } else {
            self.skl_iter.seek_to_first();
        }
    }
}

pub struct Iterator<'a> {
    pub(crate) table_iter: Box<TableIterators>,
    txn: &'a Transaction,
    read_ts: u64,

    pub(crate) opt: IteratorOptions,
    item: Option<Item>,
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
    pub fn new_iterator(&self, opt: &IteratorOptions) -> Iterator {
        if self.discarded {
            panic!("Transaction has already been discarded.")
        }

        // TODO: Check DB closed.

        self.num_iterators
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let tables = self.core.get_mem_tables();
        // We need to get a reference to vlog, to avoid vlog GC (not implemented).
        let _vlog = self.core.vlog.clone();

        let mut iters: Vec<TableIterators> = vec![];
        if let Some(itr) = self.new_pending_writes_iterator(opt.reverse) {
            iters.push(TableIterators::from(itr));
        }
        for table in tables {
            iters.push(TableIterators::from(SkiplistIterator::new(
                table.skl.iter(),
                opt.reverse,
            )));
        }

        self.core.lvctl.append_iterators(&mut iters, opt);

        Iterator {
            table_iter: MergeIterator::from_iterators(iters, opt.reverse),
            txn: self,
            read_ts: self.read_ts,
            opt: opt.clone(),
            item: None,
        }
    }
}

impl<'a> Iterator<'a> {
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
        let key = BytesMut::from(&self.item().key.clone()[..]);
        let key_with_ts = key_with_ts(key, self.item().version);

        self.table_iter.seek(&key_with_ts);
        self.table_iter.next();

        while self.table_iter.valid() {
            if self.parse_item() {
                return;
            }
        }

        self.item = None;
    }

    pub fn prev(&mut self) {
        let key = BytesMut::from(&self.item().key.clone()[..]);
        let key_with_ts = key_with_ts(key, self.item().version);

        self.table_iter.seek(&key_with_ts);
        self.table_iter.prev();

        while self.table_iter.valid() {
            if self.parse_item_for_prev() {
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
        #[allow(clippy::unnecessary_to_owned)]
        let key: &[u8] = &self.table_iter.key().to_owned();

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
            let mut item = Item::new(self.txn.core.clone());
            Self::fill_item(&mut item, key, &self.table_iter.value(), &self.opt);
            self.item = Some(item);
            self.table_iter.next();
            return true;
        }

        // If iterating in forward direction, then just checking the last key against
        // current key would be sufficient.
        if !self.opt.reverse {
            self.table_iter.prev();

            // Only track in forward direction.
            // We should update last_key as soon as we find a different key in our snapshot.
            // Consider keys: a 5, b 7 (del), b 5. When iterating, last_key = a.
            // Then we see b 7, which is deleted. If we don't store last_key = b, we'll then
            // return b 5, which is wrong. Therefore, update last_key here.
            if self.table_iter.valid()
                && crate::util::same_key(self.table_iter.key(), key)
                && get_ts(self.table_iter.key()) <= self.read_ts
            {
                self.table_iter.next();
                self.table_iter.next();
                return false;
            }

            self.table_iter.next();
        }

        loop {
            let vs = self.table_iter.value();
            if is_deleted_or_expired(vs.meta, vs.expires_at) {
                self.table_iter.next();
                return false;
            }

            let mut item = Item::new(self.txn.core.clone());
            Self::fill_item(
                &mut item,
                self.table_iter.key(),
                &self.table_iter.value(),
                &self.opt,
            );

            self.table_iter.next();
            if !self.opt.reverse || !self.table_iter.valid() {
                self.item = Some(item);
                return true;
            }

            // Reverse direction.
            let next_ts = get_ts(self.table_iter.key());
            if next_ts <= self.read_ts && crate::util::same_key(self.table_iter.key(), key) {
                // This is a valid potential candidate.
                continue;
            }

            // Ignore the next candidate. Return the current one.
            self.item = Some(item);
            return true;
        }
    }

    /// Just like `parse_item`, but used for `prev`.
    ///
    /// This function advances the iterator.
    fn parse_item_for_prev(&mut self) -> bool {
        #[allow(clippy::unnecessary_to_owned)]
        let key: &[u8] = &self.table_iter.key().to_owned();

        // Skip Agate keys.
        if !self.opt.internal_access && key.starts_with(AGATE_PREFIX) {
            self.table_iter.prev();
            return false;
        }

        // Skip any versions which are beyond the read_ts.
        let version = get_ts(key);
        if version > self.read_ts {
            self.table_iter.prev();
            return false;
        }

        if self.opt.all_versions {
            // Return deleted or expired values also, otherwise user can't figure out
            // whether the key was deleted.
            let mut item = Item::new(self.txn.core.clone());
            Self::fill_item(&mut item, key, &self.table_iter.value(), &self.opt);
            self.item = Some(item);
            self.table_iter.prev();
            return true;
        }

        // If iterating in *reverse* direction, then just checking the last key against
        // current key would be sufficient.
        if self.opt.reverse {
            self.table_iter.next();

            if self.table_iter.valid()
                && crate::util::same_key(self.table_iter.key(), key)
                && get_ts(self.table_iter.key()) <= self.read_ts
            {
                self.table_iter.prev();
                self.table_iter.prev();
                return false;
            }

            self.table_iter.prev();
        }

        loop {
            let vs = self.table_iter.value();
            if is_deleted_or_expired(vs.meta, vs.expires_at) {
                self.table_iter.prev();
                return false;
            }

            let mut item = Item::new(self.txn.core.clone());
            Self::fill_item(
                &mut item,
                self.table_iter.key(),
                &self.table_iter.value(),
                &self.opt,
            );

            self.table_iter.prev();
            if self.opt.reverse || !self.table_iter.valid() {
                self.item = Some(item);
                return true;
            }

            // Forward direction.
            let next_ts = get_ts(self.table_iter.key());
            if next_ts <= self.read_ts && crate::util::same_key(self.table_iter.key(), key) {
                // This is a valid potential candidate.
                continue;
            }

            // Ignore the next candidate. Return the current one.
            self.item = Some(item);
            return true;
        }
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

        // TODO: Prefix.

        if key.is_empty() {
            self.table_iter.rewind();
        } else {
            let key = if !self.opt.reverse {
                key_with_ts(BytesMut::from(&key[..]), self.txn.read_ts)
            } else {
                key_with_ts(BytesMut::from(&key[..]), 0)
            };

            self.table_iter.seek(&key);
        }

        while self.table_iter.valid() {
            if self.parse_item() {
                return;
            }
        }

        self.item = None;
    }

    /// Rewinds the iterator cursor all the way to zero-th position, which would be the
    /// smallest key if iterating forward, and largest if iterating backward. It does not
    /// keep track of whether the cursor started with a `seek`.
    pub fn rewind(&mut self) {
        self.seek(&Bytes::new());
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_last(&mut self) {
        // TODO: Re-examine this.

        self.table_iter.to_last();

        while self.table_iter.valid() {
            if self.parse_item_for_prev() {
                return;
            }
        }
    }
}

impl<'a> Drop for Iterator<'a> {
    fn drop(&mut self) {
        self.txn.num_iterators.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::tests::*,
        entry::Entry,
        util::{make_comparator, test::check_iterator_out_of_bound},
    };

    #[test]
    fn test_skl_iterator_out_of_bound() {
        let n = 100;
        let skl = Skiplist::with_capacity(make_comparator(), 4 * 1024 * 1024, true);

        for i in 0..n {
            let key = key_with_ts(format!("{:012x}", i).as_str(), 0);
            skl.put(key, Bytes::new());
        }

        let iter = TableIterators::from(SkiplistIterator::new(skl.iter(), false));
        check_iterator_out_of_bound(iter, n, false);

        let iter = TableIterators::from(SkiplistIterator::new(skl.iter(), true));
        check_iterator_out_of_bound(iter, n, true);
    }

    #[test]
    fn test_iterator() {
        run_agate_test(None, |agate| {
            let n = 100;

            let key = |i| BytesMut::from(format!("key-{:012x}", i).as_bytes());
            let value = |i| Bytes::from(format!("value-{:012x}", i));

            let mut txn = agate.new_transaction(true);

            for i in 0..n {
                txn.set_entry(Entry::new(key(i).freeze(), value(i)))
                    .unwrap();
            }

            let check = |txn: &Transaction, reversed: bool| {
                let mut iter = txn.new_iterator(&IteratorOptions {
                    reverse: reversed,
                    ..Default::default()
                });

                iter.rewind();

                // test iterate
                for i in 0..n {
                    assert!(iter.valid());
                    if !reversed {
                        assert_eq!(iter.item().key, key(i));
                    } else {
                        assert_eq!(iter.item().key, key(n - i - 1));
                    }
                    iter.next();
                }
                assert!(!iter.valid());

                // test seek
                for i in 10..n - 10 {
                    iter.seek(&key(i).freeze());

                    for j in 0..10 {
                        if !reversed {
                            assert_eq!(iter.item().key, key(i + j));
                        } else {
                            assert_eq!(iter.item().key, key(i - j));
                        }
                        iter.next();
                    }
                }

                // test prev
                for i in 10..n - 10 {
                    iter.seek(&key(i).freeze());

                    for j in 0..10 {
                        if !reversed {
                            assert_eq!(iter.item().key, key(i - j));
                        } else {
                            assert_eq!(iter.item().key, key(i + j));
                        }
                        iter.prev();
                    }
                }

                // test to_last
                iter.to_last();
                if !reversed {
                    assert_eq!(iter.item().key, key(n - 1));
                } else {
                    assert_eq!(iter.item().key, key(0));
                }
            };

            check(&txn, false);

            check(&txn, true);
        });
    }
}
