use std::{
    collections::VecDeque,
    sync::{atomic::AtomicBool, Arc, RwLock},
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

/// MemTableInner guards WAL and max_version.
/// These data will only be modified on memtable put.
/// Therefore, separating wal and max_version enables
/// concurrent read/write of MemTable.
struct MemTableInner {
    wal: Option<Wal>,
    max_version: u64,
}

pub struct MemTable {
    pub(crate) skl: Skiplist<Comparator>,
    opt: AgateOptions,
    inner: RwLock<MemTableInner>,

    save_after_close: AtomicBool,
    id: usize,
}

impl MemTable {
    pub fn new(id: usize, skl: Skiplist<Comparator>, wal: Option<Wal>, opt: AgateOptions) -> Self {
        Self {
            skl,
            opt,
            inner: RwLock::new(MemTableInner {
                wal,
                max_version: 0,
            }),
            save_after_close: AtomicBool::new(false),
            id,
        }
    }

    pub fn update_skip_list(&self) -> Result<()> {
        let mut inner = self.inner.write()?;
        let mut max_version = inner.max_version;
        if let Some(ref mut wal) = inner.wal {
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
        inner.max_version = max_version;
        Ok(())
    }

    pub fn put(&self, key: Bytes, value: Value) -> Result<()> {
        let mut inner = self.inner.write()?;
        if let Some(ref mut wal) = inner.wal {
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
        // Although we guarantee that the order of all updates pushed to write_ch is
        // the same as getting commit_ts(version), user may set it non-monotonically
        // in managed mode. So we should compare and update.
        inner.max_version = inner.max_version.max(ts);

        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        let mut inner = self.inner.write()?;
        if let Some(ref mut wal) = inner.wal {
            wal.sync()?;
        }
        Ok(())
    }

    pub(crate) fn should_flush_wal(&self) -> Result<bool> {
        let inner = self.inner.read()?;
        if let Some(ref wal) = inner.wal {
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
            let mut inner = self.inner.write()?;
            let wal = inner.wal.take();
            if let Some(wal) = wal {
                wal.close_and_save();
            }
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn max_version(&self) -> u64 {
        self.inner.read().unwrap().max_version
    }
}

impl Drop for MemTable {
    fn drop(&mut self) {
        crate::util::no_fail(self.drop_no_fail(), "MemTable::drop");
    }
}

pub struct MemTablesView {
    tables: Vec<Skiplist<Comparator>>,
}

impl MemTablesView {
    pub fn tables(&self) -> &[Skiplist<Comparator>] {
        &self.tables[..]
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
        let len = self.immutable.len() + 1;

        let mut tables: Vec<Skiplist<Comparator>> = Vec::with_capacity(len);

        tables.push(self.mutable.skl.clone());
        for s in self.immutable.iter() {
            tables.push(s.skl.clone());
        }

        MemTablesView { tables }
    }

    /// Get mutable memtable
    pub fn mut_table(&self) -> Arc<MemTable> {
        self.mutable.clone()
    }

    pub fn imm_table(&self, idx: usize) -> Arc<MemTable> {
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

    pub fn max_version(&self) -> u64 {
        let mut v = self.mutable.max_version();
        v = v.max(
            self.immutable
                .iter()
                .map(|imm| imm.max_version())
                .max()
                .unwrap_or(0),
        );
        v
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
        let skl = Skiplist::with_capacity(make_comparator(), 4 * 1024 * 1024, true);
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
        assert_eq!(mem_tables.mutable.max_version(), 249);
        assert_eq!(mem_tables.immutable[0].max_version(), 499);
        assert_eq!(mem_tables.immutable[1].max_version(), 749);
        assert_eq!(mem_tables.immutable[2].max_version(), 999);
        assert_eq!(mem_tables.max_version(), 999);
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

    #[test]
    fn max_version() {
        let skl = Skiplist::with_capacity(make_comparator(), 4 * 1024 * 1024, true);
        let mem = MemTable::new(0, skl, None, AgateOptions::default());
        for i in 200..300 {
            let mut v = BytesMut::from(i.to_string().as_bytes());
            append_ts(&mut v, i);
            let v = v.freeze();
            mem.put(v.clone(), Value::new(v)).unwrap();
        }
        assert_eq!(mem.max_version(), 299);
        for i in 300..310 {
            let mut v = BytesMut::from(i.to_string().as_bytes());
            append_ts(&mut v, i);
            let v = v.freeze();
            mem.put(v.clone(), Value::new(v)).unwrap();
        }
        assert_eq!(mem.max_version(), 309);
        for i in 295..305 {
            let mut v = BytesMut::from((i * 100).to_string().as_bytes());
            append_ts(&mut v, i);
            let v = v.freeze();
            mem.put(v.clone(), Value::new(v)).unwrap();
        }
        assert_eq!(mem.max_version(), 309);

        let mut data = vec![];
        for i in 100..200 {
            let mut v = BytesMut::from(i.to_string().as_bytes());
            append_ts(&mut v, i);
            let v = v.freeze();
            data.push((v.clone(), Value::new(v)));
        }
        let (d1, d2) = data.split_at(50);

        let mem_tables = MemTables {
            mutable: Arc::new(mem),
            immutable: VecDeque::from(
                [d1, d2]
                    .iter()
                    .map(|x| get_memtable(x.to_vec()))
                    .collect::<Vec<Arc<MemTable>>>(),
            ),
        };
        assert_eq!(mem_tables.mutable.max_version(), 309);
        assert_eq!(mem_tables.immutable[0].max_version(), 149);
        assert_eq!(mem_tables.immutable[1].max_version(), 199);
        assert_eq!(mem_tables.max_version(), 309);
    }
}
