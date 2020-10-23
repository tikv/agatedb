use crate::format::get_ts;
use crate::structs::Entry;
use crate::util::{make_comparator, Comparator};
use crate::value::Value;
use crate::wal::Wal;
use crate::AgateOptions;
use crate::Result;
use bytes::{Bytes, BytesMut};
use skiplist::Skiplist;
use std::collections::VecDeque;
use std::mem::{self, ManuallyDrop, MaybeUninit};
use std::path::{Path, PathBuf};
use std::ptr;

const MEMTABLE_VIEW_MAX: usize = 20;

pub struct MemTable {
    pub(crate) skl: Skiplist<Comparator>,
    pub(crate) wal: Option<Wal>,
    pub(crate) max_version: u64,
    pub(crate) opt: AgateOptions,
    pub buf: BytesMut,
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
            max_version: 0,
            buf: BytesMut::new(),
        }
    }

    pub fn update_skip_list(&mut self) {
        unimplemented!()
    }

    pub fn put(&mut self, key: Bytes, value: Value) -> Result<()> {
        if let Some(ref mut wal) = self.wal {
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
        value.encode(&mut self.buf);
        let ts = get_ts(&key);
        self.skl.put(key, self.buf.clone());
        if ts > self.max_version {
            self.max_version = ts;
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
    mutable: Skiplist<Comparator>,
    immutable: VecDeque<Skiplist<Comparator>>,
}

impl MemTables {
    pub fn view(&self) -> MemTablesView {
        // Maybe flush is better.
        assert!(self.immutable.len() + 1 <= MEMTABLE_VIEW_MAX);
        let mut array: [MaybeUninit<Skiplist<Comparator>>; MEMTABLE_VIEW_MAX] =
            unsafe { MaybeUninit::uninit().assume_init() };
        array[0] = MaybeUninit::new(self.mutable.clone());
        for (i, s) in self.immutable.iter().enumerate() {
            array[i + 1] = MaybeUninit::new(s.clone());
        }
        MemTablesView {
            tables: unsafe { ManuallyDrop::new(mem::transmute(array)) },
            len: self.immutable.len() + 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skiplist::FixedLengthSuffixComparator;

    fn get_comparator() -> FixedLengthSuffixComparator {
        FixedLengthSuffixComparator::new(0)
    }
    fn get_skip_list(data: Vec<(String, String)>) -> Skiplist<FixedLengthSuffixComparator> {
        let skl = Skiplist::with_capacity(get_comparator(), 4 * 1024 * 1024);
        for (k, v) in data {
            assert!(skl.put(k, v).is_none());
        }
        skl
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
            mutable: get_skip_list(d1.to_vec()),
            immutable: VecDeque::from(
                [d2, d3, d4]
                    .iter()
                    .map(|x| get_skip_list(x.to_vec()))
                    .collect::<Vec<Skiplist<FixedLengthSuffixComparator>>>(),
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
