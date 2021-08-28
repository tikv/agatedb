mod opt;

use super::memtable::{MemTable, MemTables};
use super::Result;
use crate::entry::Entry;
use crate::value::{Request, Value};

pub use opt::AgateOptions;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

pub struct Core {
    mt: Mutex<MemTables>,
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
    pub(crate) fn write_to_lsm(&self, _request: Request) -> Result<()> {
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

    pub fn open<P: AsRef<Path>>(mut opts: AgateOptions, path: P) -> Result<Self> {
        opts.fix_options()?;

        opts.dir = path.as_ref().to_path_buf();

        if !opts.in_memory && !opts.dir.exists() {
            fs::create_dir_all(&opts.dir)?;
            // TODO: create wal path, acquire database path lock
        }

        // TODO: open or create manifest
        Ok(Agate {
            core: Arc::new(Core::new(opts)?),
        })
    }
}
