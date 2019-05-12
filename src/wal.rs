use super::Result;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

pub struct Wal {
    f: File,
    path: PathBuf,
}

impl Wal {
    pub fn open(path: PathBuf) -> Result<Wal> {
        let f = OpenOptions::new().append(true).create(true).open(&path)?;
        Ok(Wal { f, path })
    }
}
