pub(crate) mod builder;
mod iterator;

use crate::opt::Options;
use bytes::Bytes;
use proto::meta::TableIndex;
use std::fs;
use std::path::PathBuf;
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
