use std::{
    collections::VecDeque,
    mem::{self, ManuallyDrop, MaybeUninit},
    ptr,
    sync::{atomic::AtomicBool, RwLock, Arc},
};

use bytes::Bytes;
use skiplist::Skiplist;

use crate::{
    entry::Entry,
    format::get_ts,
    util::Comparator,
    value::{self, Value},
    wal::Wal,
    AgateOptions, Result,
};

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
    core: RwLock<MemTableCore>,

    save_after_close: AtomicBool,
    id: usize,
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

    pub fn new(id: usize, skl: Skiplist<Comparator>, wal: Option<Wal>, opt: AgateOptions) -> Self {
        Self {
            skl,
            opt,
            core: RwLock::new(MemTableCore {
                wal,
                max_version: 0,
            }),
            save_after_close: AtomicBool::new(false),
            id,
        }
    }

    pub fn update_skip_list(&self) -> Result<()> {
        let mut core = self.core.write()?;
        let mut max_version = core.max_version;
        if let Some(ref mut wal) = core.wal {
            let mut it = wal.iter()?;
            while let Some(entry) = it.next()? {
                let ts = get_ts(entry.key);
                if ts > max_version {
                    max_version = ts;
                }
                let v = Value {
                    value: Bytes::copy_from_slice(entry.value),
                    meta: entry.meta,
                    user_meta: entry.user_meta,
                    expires_at: entry.expires_at,
                    version: 0,
                };
                self.skl.put(Bytes::copy_from_slice(entry.key), v);
            }
        }
        core.max_version = max_version;
        Ok(())
    }

    pub fn put(&self, key: Bytes, value: Value) -> Result<()> {
        let mut core = self.core.write()?;
        if let Some(ref mut wal) = core.wal {
            let entry = Entry {
                key: key.clone(),
                value: value.value.clone(),
                expires_at: value.expires_at,
                version: value.version,
                user_meta: value.user_meta,
                meta: value.meta,
            };
            // If WAL exceeds opt.value_log_file_size, we'll force flush the memtable.
            wal.write_entry(&entry)?;
        }

        // only insert finish marker in WAL
        if value.meta & value::VALUE_FIN_TXN != 0 {
            return Ok(());
        }

        // write to skiplist
        let ts = get_ts(&key);
        self.skl.put(key, value);

        // update max version
        core.max_version = ts;

        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        let mut core = self.core.write()?;
        if let Some(ref mut wal) = core.wal {
            wal.sync()?;
        }
        Ok(())
    }

    pub(crate) fn should_flush_wal(&self) -> Result<bool> {
        let core = self.core.read()?;
        if let Some(ref wal) = core.wal {
            Ok(wal.should_flush())
        } else {
            Ok(false)
        }
    }

    pub fn mark_save(&self) {
        self.save_after_close
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn drop_no_fail(&mut self) -> Result<()> {
        if self
            .save_after_close
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            let mut core = self.core.write()?;
            let wal = core.wal.take();
            if let Some(wal) = wal {
                wal.close_and_save();
            }
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

impl Drop for MemTable {
    fn drop(&mut self) {
        crate::util::no_fail(self.drop_no_fail(), "MemTable::drop");
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
    mutable: Arc<MemTable>,
    immutable: VecDeque<Arc<MemTable>>,
}

impl MemTables {
    pub(crate) fn new(mutable: Arc<MemTable>, immutable: VecDeque<Arc<MemTable>>) -> Self {
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
    pub fn table_mut(&self) -> Arc<MemTable> {
        self.mutable.clone()
    }

    pub fn table_imm(&self, idx: usize) -> Arc<MemTable> {
        self.immutable[idx].clone()
    }

    pub(crate) fn use_new_table(&mut self, memtable: Arc<MemTable>) {
        let old_mt = std::mem::replace(&mut self.mutable, memtable);
        self.immutable.push_back(old_mt);
    }

    pub(crate) fn nums_of_memtable(&self) -> usize {
        self.immutable.len() + 1
    }

    pub fn pop_imm(&mut self) {
        self.immutable.pop_front().unwrap();
    }
}

impl Drop for MemTables {
    fn drop(&mut self) {
        for memtable in self.immutable.drain(..) {
            memtable.mark_save();
        }
        self.mutable.mark_save();
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;
    use crate::{format::append_ts, util::make_comparator};

    fn get_memtable(data: Vec<(Bytes, Value)>) -> Arc<MemTable> {
        let skl = Skiplist::with_capacity(make_comparator(), 4 * 1024 * 1024);
        let memtable = Arc::new(MemTable::new(0, skl, None, AgateOptions::default()));

        for (k, v) in data {
            assert!(memtable.put(k, v).is_ok());
        }

        memtable
    }

    #[test]
    fn test_memtable_put() {
        let mut data = vec![];
        for i in 0..1000 {
            let mut v = BytesMut::from(i.to_string().as_bytes());
            append_ts(&mut v, i);
            let v = v.freeze();
            data.push((v.clone(), Value::new(v)));
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
                    .collect::<Vec<Arc<MemTable>>>(),
            ),
        };
        let view = mem_tables.view();
        for k in 0..4 {
            for i in k * 250..(k + 1) * 250 {
                let mut v = BytesMut::from(i.to_string().as_str());
                append_ts(&mut v, i);
                let v = v.freeze();

                // get value from skiplist
                let value = view.tables()[k as usize].get(&v).unwrap();
                let expect: Bytes = Value::new(v).into();
                assert_eq!(value, &expect);
            }
        }
    }
}
