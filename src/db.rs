use crate::wal::Wal;
use super::memtable::Memtable;
use super::{Error, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct Core {
    wal: Wal,
    memtable: Memtable,
}

#[derive(Clone)]
pub struct Agate {
    core: Arc<Core>,
}

#[derive(Default)]
pub struct AgateOptions {
    create_if_not_exists: bool,
    wal_path: Option<PathBuf>,
    memtable_size: usize,
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

    pub fn memtable_size(&mut self, size: usize) -> &mut AgateOptions {
        self.memtable_size = size;
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
        if self.memtable_size == 0 {
            self.memtable_size = 32 * 1024 * 1024;
        }
        Ok(Agate {
            core: Arc::new(Core {
                wal: Wal::open(p)?,
                memtable: Memtable::with_capacity(self.memtable_size),
            }),
        })
    }
}

#[repr(C)]
pub(crate) enum OpType {
    Put,
    Delete,
}

pub(crate) struct Record<'a> {
    op_type: OpType,
    key: &'a [u8],
    value: &'a [u8],
}

impl Agate {
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.write(OpType::Put, key, value)
    }

    pub fn delete(&self, key: &[u8]) {
        self.write(OpType::Delete, key, b"")
    }

    fn write(&self, op_type: OpType, key: &[u8], value: &[u8]) {
        self.core.wal.write()
    }
}
