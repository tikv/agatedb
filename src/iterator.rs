use bytes::Bytes;

use crate::ops::transaction::Transaction;
use crate::table::TableIterators;

use crate::value::VALUE_DELETE;

pub enum PrefetchStatus {
    No,
    Prefetched,
}

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

pub struct IteratorOptions {
    pub prefetch_size: usize,
    pub prefetch_values: bool,
    pub reverse: bool,
    pub all_versions: bool,
    pub internal_access: bool,
    prefix_is_key: bool,
    pub prefix : Bytes
}

pub struct Iterator {

}

impl Transaction {
    // TODO: add IteratorOptions support. Currently, we could only create
    // an iterator that iterates all items.
    fn new_iterator(&self) -> Iterator {
        if self.discarded {
            panic!("transaction has been discarded")
        }
        // TODO: check DB closed
        self.num_iterators.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tables = self.agate.get_mem_tables();
        // We need to get a reference to vlog, to avoid vlog GC (not implemented)
        let vlog = self.agate.vlog.clone();
        let mut iters: Vec<TableIterators> = vec![];
        if let Some(itr) = self.new_pending_write_iterator(false) {
            iters.push(TableIterators::from(itr));
        }
        for table in tables {
            
        }
        Iterator {}
    }
}
