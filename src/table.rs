mod builder;

use crate::checksum;
use crate::opt::Options;
use crate::Error;
use crate::Result;
use bytes::Bytes;
use proto::meta::{checksum::Algorithm as ChecksumAlgorithm, BlockOffset, Checksum, TableIndex};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

enum MmapFile {
    File { name: PathBuf, file: fs::File },
    Memory { data: Vec<u8> },
}

impl MmapFile {
    pub fn is_in_memory(&self) -> bool {
        match self {
            Self::File { .. } => false,
            Self::Memory { .. } => true,
        }
    }
}

pub struct TableInner {
    file: MmapFile,
    table_size: usize,
    smallest: Bytes,
    biggest: Bytes,
    id: u64,
    checksum: Bytes,
    estimated_size: u64,
    index_start: usize,
    index_len: usize,
    opt: Options,
}

pub struct Table {
    inner: Arc<TableInner>,
}

/*
impl Drop for TableInner {
    fn drop(&mut self) {
        let f = match self.file.take() {
            Some(f) => f,
            None => return,
        };
        f.file.set_len(0).unwrap();
        drop(f.file);
        fs::remove_file(&f.path).unwrap();
    }
}
*/

pub struct Block {
    offset: usize,
    data: Bytes,
    checksum: Vec<u8>,
    entries_index_start: usize,
    entry_offsets: Vec<u32>,
    checksum_len: usize,
}

impl Block {
    fn size(&self) -> u64 {
        3 * std::mem::size_of::<usize>() as u64
            + self.data.len() as u64
            + self.checksum.len() as u64
            + self.entry_offsets.len() as u64 * std::mem::size_of::<u32>() as u64
    }

    fn verify_checksum(&self) -> Result<()> {
        let chksum = prost::Message::decode(self.data.clone())?;
        checksum::verify_checksum(&self.data, &chksum)
    }
}

fn parse_file_id(name: &str) -> Result<u64> {
    if !name.ends_with(".sst") {
        return Err(Error::InvalidFilename(name.to_string()));
    }
    match name[..name.len() - 4].parse() {
        Ok(id) => Ok(id),
        Err(_) => Err(Error::InvalidFilename(name.to_string())),
    }
}

impl Table {
    pub fn open(path: &Path, opt: Options) -> Result<Table> {
        let f = fs::File::create(path)?;
        let file_name = path.file_name().unwrap().to_str().unwrap();
        let id = parse_file_id(file_name)?;
        let meta = f.metadata()?;
        let table_size = meta.len();
        Ok(Table {
            inner: Arc::new(TableInner {
                file: MmapFile::File {
                    file: f,
                    name: path.to_path_buf(),
                },
                table_size: table_size as usize,
                smallest: Bytes::new(),
                biggest: Bytes::new(),
                id,
                checksum: Bytes::new(),
                estimated_size: 0,
                index_start: 0,
                index_len: 0,
                opt,
            }),
        })
    }

    pub fn open_in_memory(data: Vec<u8>, id: u64, opt: Options) -> Result<Table> {
        let table_size = data.len();
        Ok(Table {
            inner: Arc::new(TableInner {
                file: MmapFile::Memory { data },
                opt,
                table_size,
                id,
                smallest: Bytes::new(),
                biggest: Bytes::new(),
                checksum: Bytes::new(),
                estimated_size: 0,
                index_start: 0,
                index_len: 0,
            })
        })
    }

    fn is_in_memory(&self) -> bool {
        self.inner.file.is_in_memory()
    }

    fn max_version(&self) -> u64 {
        unimplemented!()
        // self.fetch_index()?.max_version()
    }

    fn init_biggest_and_smallest(&mut self) -> Result<()> {
        // self.read_index()?;
        unimplemented!()
    }
}
