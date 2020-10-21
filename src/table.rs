pub(crate) mod builder;
mod iterator;


use crate::opt::Options;

use crate::Result;
use bytes::{Buf, Bytes};


use proto::meta::{BlockOffset, TableIndex};
use std::fs;
use std::io::{Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

/// MmapFile stores SST data. `File` refers to a file on disk,
/// and `Memory` refers to data in memory.
// TODO: use a mmap library instead of handling I/O on our own
enum MmapFile {
    File {
        name: PathBuf,
        // TODO: remove this mutex and allow multi-thread read
        file: Mutex<fs::File>,
    },
    Memory {
        data: Bytes,
    },
}

impl MmapFile {
    /// Returns if data is in memory.
    pub fn is_in_memory(&self) -> bool {
        match self {
            Self::File { .. } => false,
            Self::Memory { .. } => true,
        }
    }
}

/// TableInner stores data of an SST.
/// It is immutable once created and initialized.
pub struct TableInner {
    /// file struct of SST
    file: MmapFile,
    /// size of SST
    table_size: usize,
    /// smallest key
    smallest: Bytes,
    /// biggest key
    biggest: Bytes,
    /// SST id
    id: u64,
    /// checksum of SST
    checksum: Bytes,
    /// estimated size, only used on encryption or compression
    estimated_size: u32,
    /// index of SST
    index: TableIndex,
    /// start position of index
    index_start: usize,
    /// length of index
    index_len: usize,
    /// table options
    opt: Options,
}

pub struct Table {
    inner: Arc<TableInner>,
}

impl TableInner {
    /// Create an SST from bytes data generated with table builder
    pub fn create(_path: &Path, _data: Bytes, _opt: Options) -> Result<TableInner> {
        unimplemented!()
    }

    /// Open an existing SST on disk
    pub fn open(_path: &Path, _opt: Options) -> Result<TableInner> {
        unimplemented!()
    }

    /// Open an existing SST from data in memory
    pub fn open_in_memory(_data: Bytes, _id: u64, _opt: Options) -> Result<TableInner> {
        unimplemented!()
    }

    fn init_biggest_and_smallest(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn init_index(&mut self) -> Result<&BlockOffset> {
        unimplemented!()
    }

    fn key_splits(&mut self, _n: usize, _prefix: Bytes) -> Vec<String> {
        unimplemented!()
    }

    fn fetch_index(&self) -> &TableIndex {
        return &self.index;
        // TODO: encryption
    }

    fn offsets_length(&self) -> usize {
        self.fetch_index().offsets.len()
    }

    fn offsets(&self, idx: usize) -> Option<&BlockOffset> {
        self.fetch_index().offsets.get(idx)
    }

    fn block(&self, _idx: usize, _use_cache: bool) -> Result<Arc<Block>> {
        unimplemented!()
    }

    fn index_key(&self) -> u64 {
        self.id
    }

    /// Get number of keys in SST
    pub fn key_count(&self) -> u32 {
        self.fetch_index().key_count
    }

    /// Get size of index
    pub fn index_size(&self) -> usize {
        self.index_len
    }

    /// Get size of bloom filter
    pub fn bloom_filter_size(&self) -> usize {
        self.fetch_index().bloom_filter.len()
    }

    /// Get size of SST
    pub fn size(&self) -> u64 {
        self.table_size as u64
    }

    /// Get smallest key of current table
    pub fn smallest(&self) -> &Bytes {
        &self.smallest
    }

    /// Get biggest key of current table
    pub fn biggest(&self) -> &Bytes {
        &self.biggest
    }

    /// Get filename of current SST. Returns `<memtable>` if in-memory.
    pub fn filename(&self) -> String {
        match &self.file {
            MmapFile::Memory { .. } => "<memtable>".to_string(),
            MmapFile::File { name, .. } => name.to_string_lossy().into_owned(),
        }
    }

    /// Get SST id
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Check if the table doesn't contain an entry with bloom filter.
    /// Always return false if no bloom filter is present in SST.
    pub fn does_not_have(_hash: u32) -> bool {
        false
        // TODO: add bloom filter
    }

    fn read_bloom_filter(&self) {
        unimplemented!()
    }

    pub(crate) fn read_table_index(&self) -> Result<TableIndex> {
        unimplemented!()
    }

    fn verify_checksum(&self) -> Result<()> {
        unimplemented!()
    }

    fn read(&self, offset: usize, size: usize) -> Result<Bytes> {
        self.bytes(offset, size)
    }

    fn bytes(&self, _offset: usize, _size: usize) -> Result<Bytes> {
        unimplemented!()
    }

    fn is_in_memory(&self) -> bool {
        self.file.is_in_memory()
    }

    fn max_version(&self) -> u64 {
        unimplemented!()
    }
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

/// Block contains several entries. It can be obtained from an SST.
#[derive(Default)]
pub struct Block {
    offset: usize,
    data: Bytes,
    checksum: Bytes,
    entries_index_start: usize,
    entry_offsets: Vec<u32>,
    checksum_len: usize,
}

/*
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
*/
