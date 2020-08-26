mod builder;

use std::fs;
use bytes::Bytes;
use proto::meta::{TableIndex, Checksum, Checksum_Algorithm, BlockOffset};
use crate::opt::Options;
use proto::meta::BlockOffset;
use std::io::Error;
use crate::Result;
use crate::checksum;
use std::path::Path;

struct File {
    name: PathBuf,
    file: fs::File,
}

pub struct TableInner {
    file: Option<File>,
    table_size: usize,
    smallest: Bytes,
    biggest: Bytes,
    id: u64,
    checksum: u64,
    estimated_size: u64,
    index_start: usize,
    index_len: usize,
    is_in_memory: bool,
    opt: Options,
    no_of_blocks: usize,
}

pub struct Table {
    inner: Arc<Table>,
}

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

pub struct Block {
    offset: usize,
    data: Bytes,
    checksum: Vec<u8>,
    checksum: u64,
    entries_index_start: usize,
    entry_offsets: Vec<u32>,
    checksum_len: usize,
}

impl Block {
    fn size(&self) -> u64 {
        3 * mem::size_of::<usize>() as u64 + self.data.len() as u64 + self.checksum.len() as u64 + self.entry_offsets.len() as u64 * mem::size_of::<u32>() as u64
    }

    fn verify_checksum(&self) -> Result<()> {
        let mut chksum = CheckSum::default();
        chksum.merge_from_bytes(&self.checksum)?;
        checksum::verify_checksum(&self.data, &chksum)
    }
}

fn parse_file_id(name: &str) -> (u64, bool) {
    if !name.ends_with(".sst") {
        (0, false)
    }
    match name[..name.len() - 4].parse() {
        Ok(id) => (id, true),
        Err(_) => return (0, false),
    }
}

impl Table {
    pub fn open(path: &Path, opt: Options) -> Result<Table> {
        let f = fs::File::open(path)?;
        let file_name = path.base_name();
        let (id, ok) = parse_file_id(file_name);
        if !ok {
            return Err();
        }
        let meta = f.metadata()?;
        let table_size = meta.len();
        let mut t = Table {
            inner: Arc::new(Table {
                file: Some(File {
                    name: path.to_buf(),
                    file: f,
                }),
                table_size,
                smallest: Bytes::new(),
                biggest: Bytes::new(),
                id,
                checksum: 0,
                estimated_size: 0,
                index_start: 0,
                index_len: 0,
                is_in_memory: false,
                opt,
                no_of_blocks: 0,
            }),
        };

    }

    fn init_biggest_and_smallest(&mut self) -> Result<()> {
        self.read_index()?;
        
    }
}
