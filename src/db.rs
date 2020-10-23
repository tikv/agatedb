use super::memtable::MemTable;
use super::{format, Error, Result};
use crate::wal::Wal;
use bytes::Bytes;
use std::fs;
use std::path::{Path, PathBuf};
use crate::util::make_comparator;
use std::sync::Arc;
use skiplist::Skiplist;
use std::collections::VecDeque;
use std::sync::RwLock;

pub struct MemTables {
    mut_table: MemTable,
    imm_table: VecDeque<MemTable>,
}
pub struct Core {
    mt: RwLock<MemTables>,
    opts: AgateOptions
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
    pub in_memory: bool
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
}


impl Core {
    fn memtable_file_path(base_path: &Path, file_id: usize) -> PathBuf {
        base_path.to_path_buf().join(format!("{:05}{}", file_id, MEMTABLE_FILE_EXT))
    }

    fn arena_size(&self) -> u64{
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
}
