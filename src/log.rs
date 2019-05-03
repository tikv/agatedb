use super::Result;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

pub struct Log {
    f: File,
    path: PathBuf,
}

impl Log {
    pub fn open(path: PathBuf) -> Result<Log> {
        let f = OpenOptions::new().append(true).create(true).open(&path)?;
        Ok(Log { f, path })
    }
}
