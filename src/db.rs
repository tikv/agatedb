mod opt;

use super::memtable::{MemTable, MemTables};
use super::{Error, Result};
use crate::entry::Entry;
use crate::format::get_ts;
use crate::util::make_comparator;
use crate::value::{self, Request, Value};
use crate::wal::Wal;
use bytes::Bytes;
pub use opt::AgateOptions;
use parking_lot::RwLock;
use skiplist::Skiplist;
use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct Core {
    mt: RwLock<MemTables>,
    opts: AgateOptions,
    next_mem_fid: usize,
}

#[derive(Clone)]
pub struct Agate {
    core: Arc<Core>,
}

const MEMTABLE_FILE_EXT: &str = ".mem";

impl Agate {
    /*
    pub fn get_with_ts(&self, key: &[u8], ts: u64) -> Result<Option<Bytes>> {
        let key = format::key_with_ts(key, ts);
        let view = self.core.memtable.view();
        if let Some(value) = view.get(&key) {
            return Ok(Some(value.clone()));
        }
        unimplemented!()
    }
    */
}

impl Core {
    fn new(opts: AgateOptions) -> Result<Self> {
        // create first mem table
        let mt = Self::open_mem_table(&opts.dir, opts.clone(), 0)?;

        // create agate core
        let core = Self {
            mt: RwLock::new(MemTables::new(mt, VecDeque::new())),
            opts,
            next_mem_fid: 1,
        };

        // TODO: initialize other structures

        Ok(core)
    }

    fn memtable_file_path(base_path: &Path, file_id: usize) -> PathBuf {
        base_path
            .to_path_buf()
            .join(format!("{:05}{}", file_id, MEMTABLE_FILE_EXT))
    }

    fn open_mem_table<P: AsRef<Path>>(
        base_path: P,
        opts: AgateOptions,
        file_id: usize,
    ) -> Result<MemTable> {
        let path = Self::memtable_file_path(base_path.as_ref(), file_id);
        let c = make_comparator();
        // TODO: refactor skiplist to use `u64`
        let skl = Skiplist::with_capacity(c, opts.arena_size() as u32);
        if opts.in_memory {
            return Ok(MemTable::new(skl, None, opts.clone()));
        }
        let wal = Wal::open(path, opts.clone())?;
        // TODO: delete WAL when skiplist ref count becomes zero

        let mem_table = MemTable::new(skl, Some(wal), opts.clone());

        mem_table.update_skip_list()?;

        Ok(mem_table)
    }

    fn open_mem_tables(&mut self) -> Result<()> {
        if self.opts.in_memory {
            return Ok(());
        }
        // TODO: process on-disk structures
        Ok(())
    }

    fn new_mem_table(&mut self) -> Result<MemTable> {
        let mt = Self::open_mem_table(&self.opts.dir, self.opts.clone(), self.next_mem_fid)?;
        self.next_mem_fid += 1;
        Ok(mt)
    }

    pub fn is_closed(&self) -> bool {
        // TODO: check db closed
        false
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Value> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        let view = self.mt.read().view();
        let mut max_value = Value::default();

        let version = get_ts(key);

        for table in view.tables() {
            let mut value = Value::default();

            if let Some(value_data) = table.get(key) {
                value.decode(value_data);
                if value.meta == 0 && value.value.is_empty() {
                    continue;
                }
                if value.version == version {
                    return Ok(value);
                }
                if max_value.version < value.version {
                    max_value = value;
                }
            }
        }

        // max_value will be used in level controller
        panic!("value not available in memtable") // Should get from level controller
    }

    /// `write_to_lsm` will only be called in write thread (or write coroutine).
    ///
    /// By using a fine-grained lock approach, writing to LSM tree acquires:
    /// 1. read lock of memtable list (only block flush)
    /// 2. write lock of mutable memtable WAL (won't block mut-table read).
    /// 3. level controller lock (TBD)
    pub fn write_to_lsm(&self, request: Request) -> Result<()> {
        // TODO: check entries and pointers

        let memtables = self.mt.read();
        let mut_table = memtables.table_mut();

        for entry in request.entries.into_iter() {
            if self.opts.skip_vlog(&entry) {
                // deletion, tombstone, and small values
                mut_table.put(
                    entry.key,
                    Value {
                        value: entry.value,
                        meta: entry.meta & (!value::VALUE_POINTER),
                        user_meta: entry.user_meta,
                        expires_at: entry.expires_at,
                        version: 0,
                    },
                )?;
            } else {
                // write pointer to memtable
                mut_table.put(
                    entry.key,
                    Value {
                        value: Bytes::new(),
                        meta: entry.meta | value::VALUE_POINTER,
                        user_meta: entry.user_meta,
                        expires_at: entry.expires_at,
                        version: 0,
                    },
                )?;
                unimplemented!()
            }
        }
        if self.opts.sync_writes {
            mut_table.sync_wal()?;
        }
        Ok(())
    }
}

impl Agate {
    pub fn get(&self, key: &[u8]) -> Result<Value> {
        self.core.get(key)
    }

    pub fn write_to_lsm(&self, request: Request) -> Result<()> {
        self.core.write_to_lsm(request)
    }

    pub fn open<P: AsRef<Path>>(mut opts: AgateOptions, path: P) -> Result<Self> {
        opts.fix_options()?;

        opts.dir = path.as_ref().to_path_buf();

        if !opts.in_memory {
            if !opts.dir.exists() {
                fs::create_dir_all(&opts.dir)?;
            }
            // TODO: create wal path, acquire database path lock
        }

        // TODO: open or create manifest
        Ok(Agate {
            core: Arc::new(Core::new(opts)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::key_with_ts;
    use bytes::BytesMut;
    use tempfile::tempdir;

    #[test]
    fn test_build() {
        with_agate_test(|_| {});
    }

    fn with_agate_test(f: impl FnOnce(Agate) -> ()) {
        let tmp_dir = tempdir().unwrap();
        let mut options = AgateOptions::default();

        options.create().in_memory(false).value_log_file_size(4096);

        f(Agate::open(options, tmp_dir).unwrap())
    }

    #[test]
    fn test_simple_get_put() {
        with_agate_test(|agate| {
            let key = key_with_ts(BytesMut::from("2333"), 0);
            let value = Bytes::from("2333333333333333");
            let req = Request {
                entries: vec![Entry::new(key.clone(), value.clone())],
                done: None,
                ptrs: vec![],
            };
            agate.write_to_lsm(req).unwrap();
            agate.get(&key).unwrap();
        });
    }
}
