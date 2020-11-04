use super::memtable::{MemTable, MemTables};
use super::{Error, Result};
use crate::entry::Entry;

use crate::value::{Request, Value};

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;

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

#[derive(Clone)]
pub struct AgateOptions {
    pub path: PathBuf,
    // TODO: docs
    pub in_memory: bool,
    pub sync_writes: bool,

    pub create_if_not_exists: bool,
    pub num_memtables: usize,
    pub mem_table_size: u64,

    pub value_threshold: usize,
    pub value_log_file_size: u64,
    pub value_log_max_entries: u32,
}

impl Default for AgateOptions {
    fn default() -> Self {
        Self {
            create_if_not_exists: false,
            path: PathBuf::new(),
            mem_table_size: 64 << 20,
            num_memtables: 20,
            in_memory: false,
            sync_writes: false,
            value_threshold: 1 << 10,
            value_log_file_size: 1 << 30 - 1,
            value_log_max_entries: 1000000,
        }
        // TODO: add other options
    }
}

impl AgateOptions {
    pub fn create(&mut self) -> &mut AgateOptions {
        self.create_if_not_exists = true;
        self
    }

    pub fn path<P: Into<PathBuf>>(&mut self, p: P) -> &mut AgateOptions {
        self.path = p.into();
        self
    }

    pub fn num_memtables(&mut self, num_memtables: usize) -> &mut AgateOptions {
        self.num_memtables = num_memtables;
        self
    }

    pub fn in_memory(&mut self, in_memory: bool) -> &mut AgateOptions {
        self.in_memory = in_memory;
        self
    }

    pub fn sync_writes(&mut self, sync_writes: bool) -> &mut AgateOptions {
        self.sync_writes = sync_writes;
        self
    }

    pub fn value_log_file_size(&mut self, value_log_file_size: u64) -> &mut AgateOptions {
        self.value_log_file_size = value_log_file_size;
        self
    }

    pub fn value_log_max_entries(&mut self, value_log_max_entries: u32) -> &mut AgateOptions {
        self.value_log_max_entries = value_log_max_entries;
        self
    }

    fn fix_options(&mut self) -> Result<()> {
        if self.in_memory {
            // TODO: find a way to check if path is set, if set, then panic with ConfigError
            self.sync_writes = false;
        }

        Ok(())
    }

    pub fn open<P: AsRef<Path>>(&mut self, path: P) -> Result<Agate> {
        self.fix_options()?;

        self.path = path.as_ref().to_path_buf();

        if !self.in_memory {
            if !self.path.exists() {
                if !self.create_if_not_exists {
                    return Err(Error::Config(format!("{:?} doesn't exist", self.path)));
                }
                fs::create_dir_all(&self.path)?;
            }
            // TODO: create wal path, acquire database path lock
        }

        // TODO: open or create manifest
        Ok(Agate {
            core: Arc::new(Core::new(self.clone())?),
        })
    }

    fn skip_vlog(&self, entry: &Entry) -> bool {
        entry.value.len() < self.value_threshold
    }

    fn arena_size(&self) -> u64 {
        // TODO: take other options into account
        self.mem_table_size as u64
    }
}

impl Core {
    fn new(_opts: AgateOptions) -> Result<Self> {
        unimplemented!()
    }

    fn memtable_file_path(base_path: &Path, file_id: usize) -> PathBuf {
        base_path
            .to_path_buf()
            .join(format!("{:05}{}", file_id, MEMTABLE_FILE_EXT))
    }

    fn open_mem_table<P: AsRef<Path>>(
        _base_path: P,
        _opts: AgateOptions,
        _file_id: usize,
    ) -> Result<MemTable> {
        unimplemented!()
    }

    fn open_mem_tables(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn new_mem_table(&mut self) -> Result<MemTable> {
        unimplemented!()
    }

    pub fn is_closed(&self) -> bool {
        // TODO: check db closed
        false
    }

    pub(crate) fn get(&self, _key: &[u8]) -> Result<Value> {
        unimplemented!()
    }

    /// `write_to_lsm` will only be called in write thread (or write coroutine).
    ///
    /// By using a fine-grained lock approach, writing to LSM tree acquires:
    /// 1. read lock of memtable list (only block flush)
    /// 2. write lock of mutable memtable WAL (won't block mut-table read).
    /// 3. level controller lock (TBD)
    pub fn write_to_lsm(&self, _request: Request) -> Result<()> {
        unimplemented!()
    }
}

impl Agate {
    pub fn get(&self, key: &[u8]) -> Result<Value> {
        self.core.get(key)
    }

    pub fn write_to_lsm(&self, request: Request) -> Result<()> {
        self.core.write_to_lsm(request)
    }
}
