pub(crate) mod builder;
mod iterator;
mod merge_iterator;

use crate::checksum;
use crate::iterator_trait::AgateIterator;
use crate::opt::Options;
use crate::Error;
use crate::Result;

use bytes::{Buf, Bytes};
use iterator::{Iterator as TableIterator, ITERATOR_NOCACHE, ITERATOR_REVERSED};
use memmap::{Mmap, MmapOptions};
use prost::Message;
use proto::meta::{BlockOffset, Checksum, TableIndex};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// MmapFile stores SST data. `File` refers to a file on disk,
/// and `Memory` refers to data in memory.
// TODO: use a mmap library instead of handling I/O on our own
enum MmapFile {
    File {
        name: PathBuf,
        file: fs::File,
        mmap: Mmap,
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

    pub fn open(path: &Path, file: std::fs::File) -> Result<Self> {
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        Ok(MmapFile::File {
            file,
            mmap,
            name: path.to_path_buf(),
        })
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
    opts: Options,
}

pub struct Table {
    inner: Arc<TableInner>,
}

/// `AsRef<TableInner>` is only used in `init_biggest_and_smallest`
/// to construct a table iterator from `&TableInner`.
impl AsRef<TableInner> for TableInner {
    fn as_ref(&self) -> &TableInner {
        self
    }
}

impl TableInner {
    /// Create an SST from bytes data generated with table builder
    fn create(path: &Path, data: Bytes, opts: Options) -> Result<TableInner> {
        let mut f = fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        f.write(&data)?;
        // TODO: pass file object directly to open and sync write
        drop(f);
        Self::open(path, opts)
    }

    /// Open an existing SST on disk
    fn open(path: &Path, opts: Options) -> Result<TableInner> {
        let f = fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;
        let file_name = path.file_name().unwrap().to_str().unwrap();
        let id = parse_file_id(file_name)?;
        let meta = f.metadata()?;
        let table_size = meta.len();
        let mut inner = TableInner {
            file: MmapFile::open(path, f)?,
            table_size: table_size as usize,
            smallest: Bytes::new(),
            biggest: Bytes::new(),
            id,
            checksum: Bytes::new(),
            estimated_size: 0,
            index: TableIndex::default(),
            index_start: 0,
            index_len: 0,
            opts,
        };
        inner.init_biggest_and_smallest()?;
        // TODO: verify checksum
        Ok(inner)
    }

    /// Open an existing SST from data in memory
    fn open_in_memory(data: Bytes, id: u64, opts: Options) -> Result<TableInner> {
        let table_size = data.len();
        let mut inner = TableInner {
            file: MmapFile::Memory { data },
            opts,
            table_size,
            id,
            smallest: Bytes::new(),
            biggest: Bytes::new(),
            checksum: Bytes::new(),
            estimated_size: 0,
            index: TableIndex::default(),
            index_start: 0,
            index_len: 0,
        };
        inner.init_biggest_and_smallest()?;
        Ok(inner)
    }

    fn init_biggest_and_smallest(&mut self) -> Result<()> {
        let ko = self.init_index()?;
        self.smallest = Bytes::from(ko.key.clone());
        let mut it = TableIterator::new(&self, ITERATOR_REVERSED | ITERATOR_NOCACHE);
        it.rewind();
        if !it.valid() {
            return Err(Error::TableRead(format!(
                "failed to initialize biggest for table {}",
                self.filename()
            )));
        }
        self.biggest = Bytes::copy_from_slice(it.key());
        Ok(())
    }

    fn init_index(&mut self) -> Result<&BlockOffset> {
        let mut read_pos = self.table_size;

        // read checksum length from last 4 bytes
        read_pos -= 4;
        let mut buf = self.read(read_pos, 4)?;
        let checksum_len = buf.get_u32() as usize;

        // read checksum
        read_pos -= checksum_len;
        let buf = self.read(read_pos, checksum_len)?;
        let chksum = Checksum::decode(buf)?;

        // read index size from footer
        read_pos -= 4;
        let mut buf = self.read(read_pos, 4)?;
        self.index_len = buf.get_u32() as usize;

        // read index
        read_pos -= self.index_len;
        self.index_start = read_pos;
        let data = self.read(read_pos, self.index_len)?;
        checksum::verify_checksum(&data, &chksum)?;

        self.index = self.read_table_index()?;

        // TODO: compression
        self.estimated_size = self.table_size as u32;

        // TODO: has bloom filter

        Ok(&self.index.offsets[0])
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

    fn block(&self, idx: usize, _use_cache: bool) -> Result<Arc<Block>> {
        // TODO: support cache
        if idx >= self.offsets_length() {
            return Err(Error::TableRead("block out of index".to_string()));
        }
        let block_offset = self.offsets(idx).ok_or(Error::TableRead(format!(
            "failed to get offset block {}",
            idx
        )))?;

        let offset = block_offset.offset as usize;
        let data = self.read(offset, block_offset.len as usize)?;

        let mut read_pos = data.len() - 4; // first read checksum length
        let checksum_len = (&data[read_pos..read_pos + 4]).get_u32() as usize;

        if checksum_len > data.len() {
            return Err(Error::TableRead("invalid checksum length".to_string()));
        }

        // read checksum
        read_pos -= checksum_len;
        let checksum = data.slice(read_pos..read_pos + checksum_len);

        // read num entries
        read_pos -= 4;
        let num_entries = (&data[read_pos..read_pos + 4]).get_u32() as usize;

        let entries_index_start = read_pos - num_entries * 4;
        let entries_index_end = entries_index_start + num_entries * 4;

        let mut entry_offsets_ptr = &data[entries_index_start..entries_index_end];
        let mut entry_offsets = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            entry_offsets.push(entry_offsets_ptr.get_u32_le());
        }

        Ok(Arc::new(Block {
            offset,
            entries_index_start,
            // Drop checksum and checksum length.
            // The checksum is calculated for actual data + entry index + index length
            data: data.slice(..read_pos + 4),
            entry_offsets,
            checksum_len,
            checksum,
        }))
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
        let data = self.read(self.index_start, self.index_len)?;
        // TODO: prefetch
        let result = Message::decode(data)?;
        Ok(result)
    }

    fn verify_checksum(&self) -> Result<()> {
        let table_index = self.fetch_index();
        for i in 0..table_index.offsets.len() {
            let block = self.block(i, true)?;
            // TODO: table opts
            block.verify_checksum()?;
        }
        Ok(())
    }

    fn read(&self, offset: usize, size: usize) -> Result<Bytes> {
        self.bytes(offset, size)
    }

    fn bytes(&self, offset: usize, size: usize) -> Result<Bytes> {
        match &self.file {
            MmapFile::Memory { data } => {
                if offset + size > data.len() {
                    Err(Error::TableRead(format!(
                        "out of range, offset={}, size={}, len={}",
                        offset,
                        size,
                        data.len()
                    )))
                } else {
                    Ok(data.slice(offset..offset + size))
                }
            }
            MmapFile::File { mmap, .. } => {
                if offset + size > mmap.len() {
                    Err(Error::TableRead(format!(
                        "out of range, offset={}, size={}, len={}",
                        offset,
                        size,
                        mmap.len()
                    )))
                } else {
                    Ok(Bytes::copy_from_slice(&mmap[offset..offset + size]))
                }
            }
        }
    }

    fn is_in_memory(&self) -> bool {
        self.file.is_in_memory()
    }

    fn max_version(&self) -> u64 {
        unimplemented!()
        // self.fetch_index()?.max_version()
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

impl Block {
    fn size(&self) -> u64 {
        3 * std::mem::size_of::<usize>() as u64
            + self.data.len() as u64
            + self.checksum.len() as u64
            + self.entry_offsets.len() as u64 * std::mem::size_of::<u32>() as u64
    }

    fn verify_checksum(&self) -> Result<()> {
        let chksum = prost::Message::decode(self.checksum.clone())?;
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
    /// Create an SST from bytes data generated with table builder
    pub fn create(path: &Path, data: Bytes, opts: Options) -> Result<Table> {
        Ok(Table {
            inner: Arc::new(TableInner::create(path, data, opts)?),
        })
    }

    /// Open an existing SST on disk
    pub fn open(path: &Path, opts: Options) -> Result<Table> {
        Ok(Table {
            inner: Arc::new(TableInner::open(path, opts)?),
        })
    }

    /// Open an existing SST from data in memory
    pub fn open_in_memory(data: Bytes, id: u64, opts: Options) -> Result<Table> {
        Ok(Table {
            inner: Arc::new(TableInner::open_in_memory(data, id, opts)?),
        })
    }

    /// Get block numbers
    pub(crate) fn offsets_length(&self) -> usize {
        self.inner.offsets_length()
    }

    /// Get all block offsets
    pub(crate) fn offsets(&self, idx: usize) -> Option<&BlockOffset> {
        self.inner.offsets(idx)
    }

    /// Get one block from table
    pub(crate) fn block(&self, block_pos: usize, use_cache: bool) -> Result<Arc<Block>> {
        self.inner.block(block_pos, use_cache)
    }

    /// Get an iterator to this table
    pub fn new_iterator(&self, opt: usize) -> TableIterator<Arc<TableInner>> {
        TableIterator::new(self.inner.clone(), opt)
    }

    /// Get max version of this table
    pub fn max_version(&self) -> u64 {
        self.inner.max_version()
    }
}
