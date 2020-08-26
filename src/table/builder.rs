use bytes::{Bytes, BytesMut, BufMut, Buf};
use proto::meta::{TableIndex, Checksum, Checksum_Algorithm, BlockOffset};
use std::{u16, u32};
use crate::value::Value;
use protobuf::Message;
use crate::{util, checksum};
use crate::opt::Options;

#[repr(C)]
struct Header {
    overlap: u16,
    diff: u16,
}

impl Header {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u32_le(self.overlap as u32 << 16 | self.diff as u32);
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
        }
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn key_diff<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        util::bytes_diff(&self.base_key, key)
    }

    fn add_helper(&mut self, key: &Bytes, v: Value, vlog_len: usize) {
        self.key_hashes.push(farmhash::fingerprint64(&key[..key.len() - 8]));
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
        self.entry_offsets.push(self.buf.len() as u32 - self.base_offset);
        h.encode(&mut self.buf);
        self.buf.put_slice(diff_key);
        v.encode(&mut self.buf);
        let sst_size = v.encoded_size() as usize + diff_key.len() + 4;
        self.table_index.estimated_size += (sst_size + vlog_len) as u64;
    }

    fn finish_block(&mut self) {
        for offset in &self.entry_offsets {
            self.buf.put_u32_le(*offset);
        }
        self.buf.put_u32_le(self.entry_offsets.len() as u32);
        let cs = self.build_checksum(unsafe { self.buf.get_unchecked(self.base_offset as usize..) });
        self.write_checksum(cs);
        let mut bo = BlockOffset::default();
        bo.set_key(self.base_key.clone());
        bo.offset = self.base_offset;
        bo.len = self.buf.len() - self.base_offset;
        self.table_index.offsets.push(bo);
    }

    fn should_finish_block(&self, key: &[u8], value: &Value) -> bool {
        if self.entry_offsets.is_empty() {
            return false;
        }
        let size = (self.entry_offsets.len() + 1) * 4 + 4 + 8 + 4;
        assert!(size < u32::MAX as usize);
        let estimated_size = self.buf.len() - self.base_offset + 6 + key.len() + value.encoded_size() + size;
        estimated_size > self.options.block_size
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
        let block_size = self.buf.len() + self.entry_offsets.len() * 4 + 4 + 8 + 4;
        let estimated_size = block_size + 4 + 5 * self.table_index.offsets.len();
        estimated_size > block_size
    }

    pub fn finish(&mut self) -> Bytes {
        self.finish_block();
        let bytes = self.table_index.write_to_bytes().unwrap();
        assert!(bytes.len() < u32::MAX as usize);
        self.buf.put_slice(&bytes);
        self.buf.put_u32_le(bytes.len());
        let cs = self.build_checksum(&bytes);
        self.write_checksum(cs);
        self.buf.freeze()
    }

    fn build_checksum(&self, data: &[u8]) -> Checksum {
        let mut checksum = Checksum::default();
        checksum.sum = checksum::calculate_checksum(data, Checksum_Algorithm::CRC32C);
        checksum.set_algo(Checksum_Algorithm::CRC32C);
        checksum.write_to_bytes().unwrap();
        checksum
    }

    fn write_checksum(&mut self, checksum: Checksum) {
        let res = checksum.write_to_bytes().unwrap();
        let len = res.len();
        assert!(len < u32::MAX as usize);
        self.buf.put_slice(res);
        self.buf.put_u32(len as u32);
    }
}

