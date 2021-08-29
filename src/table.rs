pub(crate) mod builder;
pub mod concat_iterator;
mod iterator;
pub mod merge_iterator;

pub use concat_iterator::ConcatIterator;
pub use merge_iterator::{Iterators as TableIterators, MergeIterator};
pub type TableIterator = TableRefIterator<Arc<TableInner>>;

use crate::bloom::Bloom;
use crate::checksum;
use crate::iterator_trait::AgateIterator;
use crate::opt::{ChecksumVerificationMode, Options};
use crate::Error;
use crate::Result;

use iterator::{TableRefIterator, ITERATOR_NOCACHE, ITERATOR_REVERSED};

use bytes::{Buf, Bytes};
use memmap2::{Mmap, MmapOptions};
use prost::Message;
use proto::meta::{BlockOffset, Checksum, TableIndex};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
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
    None,
}

impl MmapFile {
    /// Returns if data is in memory.
    pub fn is_in_memory(&self) -> bool {
        match self {
            Self::File { .. } => false,
            Self::Memory { .. } => true,
            Self::None => unreachable!(),
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
    /// true if there's bloom filter in table
    has_bloom_filter: bool,
    /// table options
    opts: Options,
    /// by default, when `TableInner` is dropped, the SST file will be
    /// deleted. By setting this to true, it won't be deleted.
    save_after_close: AtomicBool,
}

/// Table is simply an Arc to its internal TableInner structure.
/// You may clone it without much overhead.
#[derive(Clone)]
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
        f.write_all(&data)?;
        // TODO: pass file object directly to open and sync write
        drop(f);
        Self::open(path, opts)
    }

    /// Open an existing SST on disk
    fn open(path: &Path, opts: Options) -> Result<TableInner> {
        use ChecksumVerificationMode::*;

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
            has_bloom_filter: false,
            save_after_close: AtomicBool::new(false),
        };
        inner.init_biggest_and_smallest()?;

        if matches!(inner.opts.checksum_mode, OnTableAndBlockRead | OnTableRead) {
            inner.verify_checksum()?;
        }

        Ok(inner)
    }

    /// Open an existing SST from data in memory
    fn open_in_memory(data: Bytes, id: u64, opts: Options) -> Result<TableInner> {
        use ChecksumVerificationMode::*;

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
            has_bloom_filter: false,
            save_after_close: AtomicBool::new(false),
        };
        inner.init_biggest_and_smallest()?;

        if matches!(inner.opts.checksum_mode, OnTableAndBlockRead | OnTableRead) {
            inner.verify_checksum()?;
        }

        Ok(inner)
    }

    fn init_biggest_and_smallest(&mut self) -> Result<()> {
        let ko = self.init_index()?;
        self.smallest = Bytes::from(ko.key.clone());
        let mut it = TableRefIterator::new(&self, ITERATOR_REVERSED | ITERATOR_NOCACHE);
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

        // bloom filter
        self.has_bloom_filter = !self.index.bloom_filter.is_empty();

        Ok(&self.index.offsets[0])
    }

    // split the table into at least (n - 1) ranges (when n >= blocks) based on block offsets
    fn key_splits(&mut self, n: usize, prefix: Bytes) -> Vec<Bytes> {
        if n == 0 {
            return vec![];
        }

        let offset_length = self.offsets_length();
        let jump = (offset_length / n).max(1);

        let mut result = vec![];

        for i in (0..offset_length).step_by(jump) {
            let block = self.offsets(i).unwrap();
            if block.key.starts_with(&prefix) {
                result.push(Bytes::copy_from_slice(&block.key))
            }
        }

        result
    }

    fn fetch_index(&self) -> &TableIndex {
        &self.index
        // TODO: encryption
    }

    fn offsets_length(&self) -> usize {
        self.fetch_index().offsets.len()
    }

    fn offsets(&self, idx: usize) -> Option<&BlockOffset> {
        self.fetch_index().offsets.get(idx)
    }

    fn block(&self, idx: usize, _use_cache: bool) -> Result<Arc<Block>> {
        use ChecksumVerificationMode::*;

        // TODO: support cache
        if idx >= self.offsets_length() {
            return Err(Error::TableRead("block out of index".to_string()));
        }
        let block_offset = self
            .offsets(idx)
            .ok_or_else(|| Error::TableRead(format!("failed to get offset block {}", idx)))?;

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

        // Drop checksum and checksum length.
        // The checksum is calculated for actual data + entry index + index length
        let data = data.slice(..read_pos + 4);

        let blk = Arc::new(Block {
            offset,
            entries_index_start,
            data,
            entry_offsets,
            checksum_len,
            checksum,
        });

        if matches!(self.opts.checksum_mode, OnTableAndBlockRead | OnBlockRead) {
            blk.verify_checksum()?;
        }

        Ok(blk)
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
            MmapFile::None => unreachable!(),
        }
    }

    /// Get SST id
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn does_not_have(&self, hash: u32) -> bool {
        if self.has_bloom_filter {
            let index = self.fetch_index();
            let bloom = Bloom::new(&index.bloom_filter);
            !bloom.may_contain(hash)
        } else {
            false
        }
    }

    pub fn has_bloom_filter(&self) -> bool {
        self.has_bloom_filter
    }

    pub(crate) fn read_table_index(&self) -> Result<TableIndex> {
        let data = self.read(self.index_start, self.index_len)?;
        // TODO: prefetch
        let result = Message::decode(data)?;
        Ok(result)
    }

    fn verify_checksum(&self) -> Result<()> {
        use ChecksumVerificationMode::*;

        let table_index = self.fetch_index();
        for i in 0..table_index.offsets.len() {
            // When using OnBlockRead or OnTableAndBlockRead, we do not need to verify block
            // checksum now. But we still need to check if there is an encoding error in block.
            let block = self.block(i, true)?;
            if !matches!(self.opts.checksum_mode, OnBlockRead | OnTableAndBlockRead) {
                block.verify_checksum()?;
            }
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
            MmapFile::None => unreachable!(),
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

impl Drop for TableInner {
    fn drop(&mut self) {
        if let MmapFile::File { file, mmap, name } =
            std::mem::replace(&mut self.file, MmapFile::None)
        {
            drop(mmap);
            // It is possible that table is opened in read-only mode,
            // so we cannot set_len.
            // file.set_len(0).unwrap();
            if !self
                .save_after_close
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                drop(file);
                fs::remove_file(&name).unwrap();
            }
        }
    }
}

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
    pub fn new_iterator(&self, opt: usize) -> TableIterator {
        TableRefIterator::new(self.inner.clone(), opt)
    }

    /// Get max version of this table
    pub fn max_version(&self) -> u64 {
        self.inner.max_version()
    }

    pub fn has_bloom_filter(&self) -> bool {
        self.inner.has_bloom_filter()
    }

    pub fn does_not_have(&self, hash: u32) -> bool {
        self.inner.does_not_have(hash)
    }

    /// Get size of SST
    pub fn size(&self) -> u64 {
        self.inner.size()
    }

    /// Get ID of SST
    pub fn id(&self) -> u64 {
        self.inner.id()
    }

    pub fn biggest(&self) -> &Bytes {
        self.inner.biggest()
    }

    pub fn smallest(&self) -> &Bytes {
        self.inner.smallest()
    }

    pub fn is_in_memory(&self) -> bool {
        self.inner.is_in_memory()
    }

    pub fn mark_save(&self) {
        self.inner
            .save_after_close
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

fn id_to_filename(id: u64) -> String {
    format!("{:06}.sst", id)
}

pub fn new_filename<P: AsRef<Path>>(id: u64, dir: P) -> PathBuf {
    dir.as_ref().join(id_to_filename(id))
}
