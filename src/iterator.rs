use bytes::{Bytes, BytesMut};

use crate::wal::Wal;
use crate::{
    format::user_key,
    get_ts, key_with_ts,
    table::{MergeIterator, TableIterators},
    value::{ValuePointer, VALUE_POINTER},
    value_log::ValueLog,
    Table,
};
use crate::{ops::transaction::Transaction, AgateIterator, Value};

use skiplist::{KeyComparator, Skiplist};
use std::cell::RefCell;
use std::sync::Arc;

use crate::ops::transaction::AGATE_PREFIX;

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

    fn has_value(&self) -> bool {
        if self.meta == 0 && self.vptr.is_empty() {
            false
        } else {
            true
        }
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
            panic!("read value log of older version is not implemented");
        }
    }

    pub fn value(&self) -> Bytes {
        if matches!(self.status, PrefetchStatus::No) {
            self.yield_item_value()
        }
        return self.value.borrow().clone();
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

#[derive(Default, Clone)]
pub struct IteratorOptions {
    pub prefetch_size: usize,
    pub prefetch_values: bool,
    pub reverse: bool,
    pub all_versions: bool,
    pub internal_access: bool,
    prefix_is_key: bool,
    pub prefix: Bytes,
}

impl IteratorOptions {
    pub fn pick_table(&self, _table: &Table) -> bool {
        true
        // TODO: implement table selection logic
    }

    pub fn pick_tables(&self, _tables: &mut Vec<Table>) {
        return;
        // TODO: implement table selection logic
    }
}

pub struct SkiplistIterator<C: KeyComparator> {
    skl_iter: skiplist::IterRef<Skiplist<C>, C>,
}

impl<C: KeyComparator> SkiplistIterator<C> {
    pub fn new(skl_iter: skiplist::IterRef<Skiplist<C>, C>) -> Self {
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
        // TODO: remove unnecesary decode
        let mut value = Value::default();
        value.decode(self.skl_iter.value());
        value
    }

    fn valid(&self) -> bool {
        self.skl_iter.valid()
    }
}

pub struct Iterator<'a> {
    pub(crate) table_iter: Box<TableIterators>,
    read_ts: u64,
    opt: IteratorOptions,
    _vlog: Arc<Option<ValueLog>>,
    item: Option<Item>,
    txn: &'a Transaction,
    last_key: BytesMut,
}

impl Transaction {
    // TODO: add IteratorOptions support. Currently, we could only create
    // an iterator that iterates all items.
    pub fn new_iterator(&self, opt: &IteratorOptions) -> Iterator<'_> {
        if self.discarded {
            panic!("transaction has been discarded")
        }
        // TODO: check DB closed
        self.num_iterators
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tables = self.agate.get_mem_tables();
        // We need to get a reference to vlog, to avoid vlog GC (not implemented)
        let vlog = self.agate.vlog.clone();
        let mut iters: Vec<TableIterators> = vec![];
        if let Some(itr) = self.new_pending_write_iterator(false) {
            iters.push(TableIterators::from(itr));
        }
        for table in tables {
            iters.push(TableIterators::from(SkiplistIterator::new(
                table.skl.iter(),
            )));
        }

        self.agate.lvctl.append_iterators(&mut iters, &opt);
        Iterator {
            table_iter: MergeIterator::from_iterators(
                iters.into_iter().map(Box::new).collect(),
                false,
            ),
            opt: opt.clone(),
            read_ts: self.read_ts,
            _vlog: vlog,
            item: None,
            txn: self,
            last_key: BytesMut::new(),
        }
    }
}

impl Iterator<'_> {
    pub fn item(&self) -> &Item {
        self.txn.add_read_key(&self.item.as_ref().unwrap().key);
        self.item.as_ref().unwrap()
    }

    pub fn valid(&self) -> bool {
        self.item.is_some()
        // TODO: check prefix
    }

    pub fn next(&mut self) {
        while self.table_iter.valid() {
            if self.parse_item() {
                return;
            }
        }
        self.item = None;
    }

    fn parse_item(&mut self) -> bool {
        let key: &[u8] = self.table_iter.key();

        if !self.opt.internal_access && key.starts_with(AGATE_PREFIX) {
            self.table_iter.next();
            return false;
        }

        let version = get_ts(key);
        if version > self.read_ts {
            self.table_iter.next();
            return false;
        }

        if self.opt.all_versions {
            let mut item = Item::new(self.txn.agate.clone());
            Self::fill_item(&mut item, key, &self.table_iter.value(), &self.opt);
            self.item = Some(item);
            self.table_iter.next();
            return true;
        }

        if !self.opt.reverse {
            if crate::util::same_key(&self.last_key, key) {
                self.table_iter.next();
                return false;
            }
            self.last_key.clear();
            self.last_key.extend_from_slice(key);
        }

        let vs = self.table_iter.value();
        if is_deleted_or_expired(vs.meta, vs.expires_at) {
            self.table_iter.next();
            return false;
        }

        // TODO: prefetch

        let mut item = Item::new(self.txn.agate.clone());
        Self::fill_item(&mut item, key, &self.table_iter.value(), &self.opt);
        self.item = Some(item);

        self.table_iter.next();

        if !self.opt.reverse || !self.valid() {
            // TODO: implement data vector
            return true;
        }

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

    pub fn seek(&mut self, key: &Bytes) {
        if !key.is_empty() {
            self.txn.add_read_key(key);
        }

        self.last_key.clear();

        // TODO: prefix

        if key.is_empty() {
            self.table_iter.rewind();
            self.next();
            return;
        }

        // TODO: reverse
        let key = key_with_ts(BytesMut::from(&key[..]), self.txn.read_ts);

        self.table_iter.seek(&key);
        self.next();
    }

    pub fn rewind(&mut self) {
        self.seek(&Bytes::new());
    }

    pub fn valid_for_prefix(&self, prefix: &Bytes) -> bool {
        self.valid() && self.item().key.starts_with(&prefix[..])
    }
}
