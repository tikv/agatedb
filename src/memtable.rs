use crate::format::get_ts;
use crate::structs::Entry;
use crate::util::{make_comparator, Comparator};
use crate::value::{self, Value};
use crate::wal::Wal;
use crate::AgateOptions;
use crate::Result;
use bytes::{Bytes, BytesMut};
use skiplist::Skiplist;
use std::collections::VecDeque;
use std::mem::{self, ManuallyDrop, MaybeUninit};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::AtomicU64;

const MEMTABLE_VIEW_MAX: usize = 20;

pub struct MemTable {
    pub(crate) skl: Skiplist<Comparator>,
    pub(crate) wal: Option<Wal>,
    pub(crate) max_version: AtomicU64,
    pub(crate) opt: AgateOptions,
}

impl MemTable {
    /*
    pub fn with_capacity(table_size: u32, max_count: usize) -> MemTable {
        let c = make_comparator();
        MemTable {
            mutable: Skiplist::with_capacity(c, table_size),
            immutable: VecDeque::with_capacity(max_count - 1),
        }
    }

    pub fn view(&self) -> MemTableView {
        // Maybe flush is better.
        assert!(self.immutable.len() + 1 <= 20);
        let mut array: [MaybeUninit<Skiplist<Flsc>>; 20] =
            unsafe { MaybeUninit::uninit().assume_init() };
        array[0] = MaybeUninit::new(self.mutable.clone());
        for (i, s) in self.immutable.iter().enumerate() {
            array[i + 1] = MaybeUninit::new(s.clone());
        }
        MemTableView {
            tables: unsafe { ManuallyDrop::new(mem::transmute(array)) },
            len: self.immutable.len() + 1,
        }
    }
    */

    pub fn new(skl: Skiplist<Comparator>, wal: Option<Wal>, opt: AgateOptions) -> Self {
        Self {
            skl,
            wal,
            opt,
            max_version: AtomicU64::new(0),
        }
    }

    pub fn update_skip_list(&mut self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            unimplemented!()
        }
        Ok(())
    }

    pub fn put(&self, key: Bytes, value: Value) -> Result<()> {
        if let Some(ref wal) = self.wal {
            let entry = Entry::new(
                key.clone(),
                value.value.clone(),
                value.expires_at,
                value.version,
                value.user_meta,
                value.meta,
            );
            wal.write_entry(entry)?;
        }

        // only insert finish marker in WAL
        if value.meta & value::VALUE_FIN_TXN != 0 {
            return Ok(());
        }

        // write to skiplist
        let ts = get_ts(&key);
        self.skl.put(key, value);

        // update max version
        self.max_version
            .fetch_max(ts, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
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
        assert!(self.immutable.len() + 1 <= MEMTABLE_VIEW_MAX);
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

#[cfg(test)]
mod tests {
    use super::*;
    use skiplist::FixedLengthSuffixComparator;

    fn get_comparator() -> FixedLengthSuffixComparator {
        FixedLengthSuffixComparator::new(0)
    }

    fn get_memtable(data: Vec<(String, String)>) -> MemTable {
        let skl = Skiplist::with_capacity(get_comparator(), 4 * 1024 * 1024);
        for (k, v) in data {
            assert!(skl.put(k, v).is_none());
        }
        MemTable::new(skl, None, AgateOptions::default())
    }

    #[test]
    fn test_memtable_put() {
        let mut data = vec![];
        for i in 0..1000 {
            data.push((i.to_string(), i.to_string()));
        }
        let (d1, dx) = data.split_at(250);
        let (d2, dx) = dx.split_at(250);
        let (d3, dx) = dx.split_at(250);
        let (d4, _) = dx.split_at(250);
        let mem_tables = MemTables {
            mutable: get_memtable(d1.to_vec()),
            immutable: VecDeque::from(
                [d2, d3, d4]
                    .iter()
                    .map(|x| get_memtable(x.to_vec()))
                    .collect::<Vec<MemTable>>(),
            ),
        };
        let view = mem_tables.view();
        for k in 0..4 {
            for i in k * 250..(k + 1) * 250 {
                assert_eq!(
                    view.tables()[k].get(i.to_string().as_bytes()),
                    Some(&Bytes::from(i.to_string()))
                );
            }
        }
    }
}
