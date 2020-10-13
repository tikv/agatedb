mod builder;
mod iterator;

use crate::checksum;
use crate::opt::Options;
use crate::Error;
use crate::Result;
use bytes::{Bytes, Buf};
use proto::meta::{checksum::Algorithm as ChecksumAlgorithm, BlockOffset, Checksum, TableIndex};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use prost::Message;

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
    estimated_size: u32,
    index: TableIndex,
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

#[derive(Default)]
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

impl TableInner {
    pub fn open(path: &Path, opt: Options) -> Result<TableInner> {
        let f = fs::File::create(path)?;
        let file_name = path.file_name().unwrap().to_str().unwrap();
        let id = parse_file_id(file_name)?;
        let meta = f.metadata()?;
        let table_size = meta.len();
        Ok(TableInner {
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
            index: TableIndex::default(),
            index_start: 0,
            index_len: 0,
            opt,
        })
    }

    pub fn open_in_memory(data: Vec<u8>, id: u64, opt: Options) -> Result<TableInner> {
        let table_size = data.len();
        Ok(TableInner {
            file: MmapFile::Memory { data },
            opt,
            table_size,
            id,
            smallest: Bytes::new(),
            biggest: Bytes::new(),
            checksum: Bytes::new(),
            estimated_size: 0,
            index: TableIndex::default(),
            index_start: 0,
            index_len: 0,
        })
    }

    fn init_biggest_and_smallest(&mut self) -> Result<()> {
        let ko = self.init_index()?;
        self.smallest = Bytes::from(ko.key.clone());
        // let mut it = self.new_iterator(REVERSED | NOCACHE);
        // it.rewind();
        // if !it.valid() {
        //     return Err(Error::TableRead(format!("failed to initialize biggest for table {}", self.filename())));
        // }
        // self.biggest = Bytes::from(it.key());
        unimplemented!();
        Ok(())
    }

    fn init_index(&mut self) -> Result<&BlockOffset> {
        let mut read_pos = self.table_size;
        
        // read checksum length from last 4 bytes
        read_pos -= 4;
        let mut buf = self.read(read_pos, 4)?;
        let checksum_len = buf.get_u32() as usize;
        if (checksum_len as i32) < 0 {
            return Err(Error::TableRead("checksum length less than zero".to_string()));
        }

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

    fn key_splits(&mut self, n: usize, prefix: Bytes) -> Vec<String> {
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

    fn block(&self, idx: usize) -> Result<Arc<Block>> { // TODO: support cache
        if idx >= self.offsets_length() {
            return Err(Error::TableRead("block out of index".to_string()));
        }

        let ko = self.offsets(idx).ok_or(Error::TableRead(format!("failed to get offset block {}", idx)))?;
        let blk = Block {
            offset: ko.offset as usize,
            ..Block::default()
        };

        unimplemented!();

        Ok(Arc::new(blk))
    }

    fn read_table_index(&mut self) -> Result<TableIndex> {
        unimplemented!();
    }

    fn read(&mut self, offset: usize, size: usize) -> Result<Bytes> {
        self.bytes(offset, size)
    }

    fn bytes(&mut self, offset: usize, size: usize) -> Result<Bytes> {
        unimplemented!()
    }

    fn filename(&self) -> String {
        match &self.file {
            MmapFile::Memory {..} => "".to_string(),
            MmapFile::File { name, ..} => name.to_string_lossy().into_owned()
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

impl Table {
    pub fn open(path: &Path, opt: Options) -> Result<Table> {
        TableInner::open(path, opt).map(|table| Table {
            inner: Arc::new(table),
        })
    }

    pub fn open_in_memory(data: Vec<u8>, id: u64, opt: Options) -> Result<Table> {
        TableInner::open_in_memory(data, id, opt).map(|table| Table {
            inner: Arc::new(table),
        })
    }
}
