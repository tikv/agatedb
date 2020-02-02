use super::Result;
use bytes::{BufMut, Bytes, BytesMut};
use crc::crc32::{Digest, IEEE};
use std::fs::{File, OpenOptions};
use std::mem::MaybeUninit;
use std::path::PathBuf;
use std::ptr;

struct Header {
    key_len: usize,
    value_len: usize,
}

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
