use crate::util::Comparator;
use crate::value::Value;
use crate::wal::Wal;
use crate::AgateOptions;
use crate::Result;
use bytes::Bytes;
use skiplist::Skiplist;
use std::collections::VecDeque;
use std::mem::{self, ManuallyDrop, MaybeUninit};

use std::ptr;
use std::sync::Mutex;

const MEMTABLE_VIEW_MAX: usize = 20;

/// MemTableCore guards WAL and max_version.
/// These data will only be modified on memtable put.
/// Therefore, separating wal and max_version enables
/// concurrent read/write of MemTable.
struct MemTableCore {
    wal: Option<Wal>,
    max_version: u64,
}

pub struct MemTable {
    pub(crate) skl: Skiplist<Comparator>,
    opt: AgateOptions,
    core: Mutex<MemTableCore>,
}

impl MemTable {
    pub fn new(skl: Skiplist<Comparator>, wal: Option<Wal>, opt: AgateOptions) -> Self {
        Self {
            skl,
            opt,
            core: Mutex::new(MemTableCore {
                wal,
                max_version: 0,
            }),
        }
    }

    pub fn update_skip_list(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn put(&self, _key: Bytes, _value: Value) -> Result<()> {
        unimplemented!()
    }

    pub fn sync_wal(&self) -> Result<()> {
        unimplemented!()
    }
}

pub struct MemTablesView {
    tables: ManuallyDrop<[Skiplist<Comparator>; MEMTABLE_VIEW_MAX]>,
    len: usize,
}

impl MemTablesView {
    pub fn tables(&self) -> &[Skiplist<Comparator>] {
        &self.tables[0..self.len]
    }
}

impl Drop for MemTablesView {
    fn drop(&mut self) {
        for i in 0..self.len {
            unsafe {
                ptr::drop_in_place(&mut self.tables[i]);
            }
        }
    }
}

pub struct MemTables {
    mutable: MemTable,
    immutable: VecDeque<MemTable>,
}

impl MemTables {
    pub(crate) fn new(mutable: MemTable, immutable: VecDeque<MemTable>) -> Self {
        Self { mutable, immutable }
    }

    /// Get view of all current memtables
    pub fn view(&self) -> MemTablesView {
        // Maybe flush is better.
        assert!(self.immutable.len() < MEMTABLE_VIEW_MAX);
        let mut array: [MaybeUninit<Skiplist<Comparator>>; MEMTABLE_VIEW_MAX] =
            unsafe { MaybeUninit::uninit().assume_init() };
        array[0] = MaybeUninit::new(self.mutable.skl.clone());
        for (i, s) in self.immutable.iter().enumerate() {
            array[i + 1] = MaybeUninit::new(s.skl.clone());
        }
        MemTablesView {
            tables: unsafe { ManuallyDrop::new(mem::transmute(array)) },
            len: self.immutable.len() + 1,
        }
    }

    /// Get mutable memtable
    pub fn table_mut(&self) -> &MemTable {
        &self.mutable
    }
}
