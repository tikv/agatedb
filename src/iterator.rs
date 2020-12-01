use bytes::{Bytes, BytesMut};

use crate::{
    format::user_key,
    get_ts, key_with_ts,
    table::{MergeIterator, TableIterators},
    value_log::ValueLog,
    Table,
};
use crate::{ops::transaction::Transaction, AgateIterator, Value};
use skiplist::{KeyComparator, Skiplist};
use std::sync::Arc;

use crate::ops::transaction::AGATE_PREFIX;
use crate::value::VALUE_DELETE;

pub enum PrefetchStatus {
    No,
    Prefetched,
}

impl Default for PrefetchStatus {
    fn default() -> Self {
        Self::No
    }
}

#[derive(Default)]
pub struct Item {
    pub(crate) key: Bytes,
    pub(crate) vptr: Bytes,
    pub(crate) value: Bytes,
    pub(crate) version: u64,
    pub(crate) expires_at: u64,

    pub(crate) status: PrefetchStatus,

    pub(crate) meta: u8,
    pub(crate) user_meta: u8,
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

#[derive(Default)]
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
    pub fn pick_table(&self, table: &Table) -> bool {
        true
        // TODO: implement table selection logic
    }

    pub fn pick_tables(&self, tables: &mut Vec<Table>) {
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
    table_iter: Box<TableIterators>,
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
    fn new_iterator(&self) -> Iterator<'_> {
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
        let opt = IteratorOptions::default();
        self.agate.lvctl.append_iterators(&mut iters, &opt);

        Iterator {
            table_iter: MergeIterator::from_iterators(
                iters.into_iter().map(Box::new).collect(),
                false,
            ),
            opt,
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
                break;
            }
        }
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

        // TODO: all versions
        if self.opt.all_versions {
            unimplemented!()
        }

        if !self.opt.reverse {
            if crate::util::same_key(&self.last_key, key) {
                self.table_iter.next();
                return false;
            }
            self.last_key.clone_from_slice(key);
        }

        let vs = self.table_iter.value();
        if is_deleted_or_expired(vs.meta, vs.expires_at) {
            self.table_iter.next();
            return false;
        }

        // TODO: prefetch

        let mut item = Item::default();
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
        item.value = Bytes::new();

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

        if key.len() == 0 {
            self.table_iter.rewind();
        }

        // TODO: reverse
        let mut key_ts = BytesMut::new();
        key_ts.clone_from_slice(key);
        let key = key_with_ts(key_ts, self.txn.read_ts);

        self.table_iter.seek(&key);
    }

    pub fn rewind(&mut self) {
        self.seek(&Bytes::new());
    }
}
