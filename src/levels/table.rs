use bytes::Bytes;
use memmap::Mmap;
use proto::meta;
use std::fs::File;

pub struct Block {
    offset: usize,
    data: Bytes,
    checksum: Bytes,
}

pub struct Table {
    file: File,
    table_size: usize,
    block_index: Vec<meta::BlockOffset>,
    mmap: Mmap,
    smallest: *const u8,
    biggest: *const u8,
    id: u64,
}
