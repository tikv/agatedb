use super::memtable::{MemTable, MemTables, MemTablesView};
use super::{format, Error, Result};
use crate::format::get_ts;
use crate::structs::Entry;
use crate::util::make_comparator;
use crate::value::{Request, Value};
use crate::wal::Wal;
use bytes::Bytes;
use skiplist::Skiplist;
use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;

pub struct Core {
    mt: RwLock<MemTables>,
    opts: AgateOptions,
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

#[derive(Default, Clone)]
pub struct AgateOptions {
    pub create_if_not_exists: bool,
    pub wal_path: Option<PathBuf>,
    pub table_size: u32,
    pub max_table_count: usize,
    pub max_table_size: u64,
    pub in_memory: bool,
}

impl AgateOptions {
    /*
    pub fn create(&mut self) -> &mut AgateOptions {
        self.create_if_not_exists = true;
        self
    }

    pub fn wal_path<P: Into<PathBuf>>(&mut self, p: P) -> &mut AgateOptions {
        self.wal_path = Some(p.into());
        self
    }

    pub fn table_size(&mut self, size: u32) -> &mut AgateOptions {
        self.table_size = size;
        self
    }

    pub fn max_table_count(&mut self, count: usize) -> &mut AgateOptions {
        self.max_table_count = count;
        self
    }

    pub fn open<P: AsRef<Path>>(&mut self, path: P) -> Result<Agate> {
        let p = path.as_ref();
        if !p.exists() {
            if !self.create_if_not_exists {
                return Err(Error::Config(format!("{} doesn't exist", p.display())));
            }
            fs::create_dir_all(p)?;
        }
        let p = self.wal_path.take().unwrap_or_else(|| p.join("WAL"));
        if self.table_size == 0 {
            self.table_size = 32 * 1024 * 1024;
        }
        if self.max_table_count == 0 {
            self.max_table_count = 24;
        }
        Ok(Agate {
            core: Arc::new(Core {
                wal: Wal::open(p)?,
                memtable: MemTable::with_capacity(self.table_size, self.max_table_count),
            }),
        })
    }
    */

    pub fn skip_vlog(&self, entry: &Entry) -> bool {
        unimplemented!()
    }
}

impl Core {
    fn memtable_file_path(base_path: &Path, file_id: usize) -> PathBuf {
        base_path
            .to_path_buf()
            .join(format!("{:05}{}", file_id, MEMTABLE_FILE_EXT))
    }

    fn arena_size(&self) -> u64 {
        self.opts.max_table_size as u64
    }

    pub fn open_mem_table(&self, base_path: &Path, file_id: usize) -> Result<MemTable> {
        let path = Self::memtable_file_path(base_path, file_id);
        let c = make_comparator();
        // TODO: refactor skiplist to use `u64`
        let skl = Skiplist::with_capacity(c, self.arena_size() as u32);
        if self.opts.in_memory {
            return Ok(MemTable::new(skl, None, self.opts.clone()));
        }
        let wal = Wal::open(file_id, path)?;

        // TODO: delete WAL when skiplist ref count becomes zero

        let mut mem_table = MemTable::new(skl, Some(wal), self.opts.clone());

        mem_table.update_skip_list();

        Ok(mem_table)
    }

    pub fn is_closed(&self) -> bool {
        // TODO: check db closed
        false
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Value> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        let view = self.mt.read()?.view();
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
        unimplemented!(); // Should get from level controller
    }

    pub fn write_to_lsm(&self, request: Request) -> Result<()> {
        // TODO: check entries and pointers

        for (i, entry) in request.entries.into_iter().enumerate() {
            if self.opts.skip_vlog(&entry) {
                // deletion or tombstone
                self.mt.read()?.put(
                    entry.key,
                    Value {
                        value: unimplemented!(),
                        meta: entry.meta & (!VALUE_POINTER),
                        user_meta: entry.user_meta,
                        expires_at: entry.expires_at,
                        version: 0
                    },
                )
            } else {
                // write pointer to memtable
                self.mt.read()
            }
        }
        if self.opt.sync_writes {
            self.mt.write()?.sync_wal();
        }
        Ok(())
    }
}
