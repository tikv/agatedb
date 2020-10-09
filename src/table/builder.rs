use crate::format::get_ts;
use crate::opt::Options;
use crate::value::Value;
use crate::{checksum, util};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use proto::meta::{checksum::Algorithm as ChecksumAlg, BlockOffset, Checksum, TableIndex};
use std::{u16, u32};
use prost::Message;

#[repr(C)]
struct Header {
    overlap: u16,
    diff: u16,
}

impl Header {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u32_le((self.overlap as u32) << 16 | self.diff as u32);
    }

    fn decode(&mut self, bytes: &mut Bytes) {
        let h = bytes.get_u32_le();
        self.overlap = (h >> 16) as u16;
        self.diff = h as u16;
    }
}

pub struct Builder {
    buf: BytesMut,
    base_key: Bytes,
    base_offset: u32,
    entry_offsets: Vec<u32>,
    table_index: TableIndex,
    key_hashes: Vec<u64>,
    options: Options,
    max_version: u64,
}

impl Builder {
    pub fn new(options: Options) -> Builder {
        Builder {
            buf: BytesMut::with_capacity(1 << 20),
            table_index: TableIndex::default(),
            key_hashes: Vec::with_capacity(1024),
            base_key: Bytes::new(),
            base_offset: 0,
            entry_offsets: vec![],
            options,
            max_version: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn key_diff<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        util::bytes_diff(&self.base_key, key)
    }

    fn add_helper(&mut self, key: &Bytes, v: Value, vlog_len: u32) {
        self.key_hashes
            .push(farmhash::fingerprint64(&key[..key.len() - 8]));
        // TODO: check ts
        let diff_key = if self.base_key.is_empty() {
            self.base_key = key.clone();
            key
        } else {
            self.key_diff(key)
        };
        assert!(key.len() - diff_key.len() <= u16::MAX as usize);
        assert!(diff_key.len() <= u16::MAX as usize);
        let h = Header {
            overlap: (key.len() - diff_key.len()) as u16,
            diff: diff_key.len() as u16,
        };
        assert!(self.buf.len() <= u32::MAX as usize);
        self.entry_offsets
            .push(self.buf.len() as u32 - self.base_offset);

        // Layout: header, diffKey, value.
        h.encode(&mut self.buf);
        self.buf.put_slice(diff_key);
        v.encode(&mut self.buf);
        let sst_size = v.encoded_size() as usize + diff_key.len() + 4;
        self.table_index.estimated_size += sst_size as u32 + vlog_len;
    }

    fn finish_block(&mut self) {
        if self.entry_offsets.is_empty() {
            return;
        }
        for offset in &self.entry_offsets {
            self.buf.put_u32_le(*offset);
        }
        self.buf.put_u32_le(self.entry_offsets.len() as u32);

        let cs = self.build_checksum(&self.buf[self.base_offset as usize..]);
        self.write_checksum(cs);

        self.add_block_to_index();
    }

    fn add_block_to_index(&mut self) {
        let block = BlockOffset {
            key: self.base_key.to_vec(),
            offset: self.base_offset,
            len: self.buf.len() as u32 - self.base_offset,
        };
        self.table_index.offsets.push(block);
    }

    fn should_finish_block(&self, key: &[u8], value: &Value) -> bool {
        if self.entry_offsets.is_empty() {
            return false;
        }
        let entries_offsets_size = (self.entry_offsets.len() + 1) * 4 +
            4 + // size of list
            8 + // sum64 in checksum proto
            4; // checksum length
        assert!(entries_offsets_size < u32::MAX as usize);
        let estimated_size = (self.buf.len() as u32) - self.base_offset + 6 /* header size for entry */ + key.len() as u32 + value.encoded_size() as u32 + entries_offsets_size as u32;
        assert!(self.buf.len() + (estimated_size as usize) < u32::MAX as usize);
        estimated_size > self.options.block_size as u32
    }

    pub fn add(&mut self, key: &Bytes, value: Value, vlog_len: u32) {
        if self.should_finish_block(&key, &value) {
            self.finish_block();
            self.base_key.clear();
            assert!(self.buf.len() < u32::MAX as usize);
            self.base_offset = self.buf.len() as u32;
            self.entry_offsets.clear();
        }
        self.add_helper(key, value, vlog_len);
    }

    pub fn reach_capacity(&self, capacity: u64) -> bool {
        let block_size = self.buf.len() as u32 + // length of buffer
                                 self.entry_offsets.len() as u32 * 4 + // all entry offsets size
                                 4 + // count of all entry offsets
                                 8 + // checksum bytes
                                 4; // checksum length
        let estimated_size = block_size + 
                                  4 + // index length
                                  5 * self.table_index.offsets.len() as u32; // TODO: why 5?
        estimated_size as u64 > capacity
    }

    pub fn finish(&mut self) -> Bytes {
        self.finish_block();
        if self.buf.is_empty() {
            return Bytes::new();
        }
        let mut bytes = BytesMut::new();
        self.table_index.encode(&mut bytes).unwrap();
        assert!(bytes.len() < u32::MAX as usize);
        self.buf.put_slice(&bytes);
        self.buf.put_u32_le(bytes.len() as u32);
        let cs = self.build_checksum(&bytes);
        self.write_checksum(cs);
        self.buf.clone().freeze()
    }

    fn build_checksum(&self, data: &[u8]) -> Checksum {
        Checksum {
            sum: checksum::calculate_checksum(data, ChecksumAlg::Crc32c),
            algo: ChecksumAlg::Crc32c as i32
        }
    }

    fn write_checksum(&mut self, checksum: Checksum) {
        let mut res = BytesMut::new();
        checksum.encode(&mut res).unwrap();
        let len = res.len();
        assert!(len < u32::MAX as usize);
        self.buf.put_slice(&res);
        self.buf.put_u32(len as u32);
    }
}
