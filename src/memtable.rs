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
