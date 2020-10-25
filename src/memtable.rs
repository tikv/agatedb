use bytes::Bytes;
use skiplist::{FixedLengthSuffixComparator as Flsc, Skiplist};
use std::collections::VecDeque;
use std::mem::{self, ManuallyDrop, MaybeUninit};
use std::ptr;

pub struct MemTableView {
    tables: ManuallyDrop<[Skiplist<Flsc>; 20]>,
    len: usize,
}

impl MemTableView {
    pub fn get(&self, key: &[u8]) -> Option<&Bytes> {
        for i in 0..self.len {
            let res = self.tables[i].get(key);
            if res.is_some() {
                return res;
            }
        }
        None
    }
}

impl Drop for MemTableView {
    fn drop(&mut self) {
        for i in 0..self.len {
            unsafe {
                ptr::drop_in_place(&mut self.tables[i]);
            }
        }
    }
}

pub struct MemTable {
    mutable: Skiplist<Flsc>,
    immutable: VecDeque<Skiplist<Flsc>>,
}

impl MemTable {
    pub fn with_capacity(table_size: u32, max_count: usize) -> MemTable {
        let c = Flsc::new(8);
        MemTable {
            mutable: Skiplist::with_capacity(c, table_size),
            immutable: VecDeque::with_capacity(max_count - 1),
        }
    }

    pub fn view(&self) -> MemTableView {
        // Maybe flush is better.
        assert!(self.immutable.len() < 20);
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
}
