// mod builder;
mod opt;
mod checksum;

use std::{fs, path::PathBuf, sync::Arc};
use bytes::Bytes;
use proto::meta::{TableIndex, Checksum, checksum::Algorithm as Checksum_Algorithm, BlockOffset};
use opt::Options;
use common::{Result, Error};
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
    inner: Arc<TableInner>,
}

impl Drop for TableInner {
    fn drop(&mut self) {
        let f = match self.file.take() {
            Some(f) => f,
            None => return,
        };
        f.file.set_len(0).unwrap();
        drop(f.file);
        // TODO: remove file
        // fs::remove_file(&f.path).unwrap();
    }
}

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
        3 * std::mem::size_of::<usize>() as u64 + self.data.len() as u64 + self.checksum.len() as u64 + self.entry_offsets.len() as u64 * std::mem::size_of::<u32>() as u64
    }

    fn verify_checksum(&self) -> Result<()> {
        let mut chksum = Checksum::default();
        unimplemented!();
        // chksum.merge_from_bytes(&self.checksum)?;
        checksum::verify_checksum(&self.data, &chksum)
    }
}

fn parse_file_id(name: &str) -> (u64, bool) {
    if !name.ends_with(".sst") {
        return (0, false);
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
            return Err("failed to parse file id");
        }
        let meta = f.metadata()?;
        let table_size = meta.len();
        let mut t = Table {
            inner: Arc::new(TableInner {
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

        t.init_biggest_and_smallest()?;

        // TODO: verify checksum

        Ok(t)
    }

    fn init_biggest_and_smallest(&mut self) -> Result<()> {
        self.read_index()?;
        // TODO: complete this function
    }
}
