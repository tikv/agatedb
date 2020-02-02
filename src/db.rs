use super::memtable::MemTable;
use super::{format, Error, Result};
use crate::wal::Wal;
use bytes::{Bytes, BytesMut};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct Core {
    wal: Wal,
    memtable: MemTable,
}

#[derive(Clone)]
pub struct Agate {
    core: Arc<Core>,
}

impl Agate {
    pub fn get_with_ts(&self, key: &[u8], ts: u64) -> Result<Option<Bytes>> {
        let key = format::key_with_ts(key, ts);
        let view = self.core.memtable.view();
        if let Some(value) = view.get(&key) {
            return Ok(Some(value.clone()));
        }
        unimplemented!()
    }
}

#[derive(Default)]
pub struct AgateOptions {
    create_if_not_exists: bool,
    wal_path: Option<PathBuf>,
    table_size: u32,
    max_table_count: usize,
}

impl AgateOptions {
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
}
