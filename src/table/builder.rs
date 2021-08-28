use crate::bloom::Bloom;
use crate::format::user_key;
use crate::opt::Options;
use crate::value::Value;
use crate::{checksum, util};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use proto::meta::{checksum::Algorithm as ChecksumAlg, BlockOffset, Checksum, TableIndex};

/// Entry header stores the difference between current key and block base key.
/// `overlap` is the common prefix of key and base key, and diff is the length
/// of different part.
#[repr(C)]
#[derive(Default)]
pub struct Header {
    pub overlap: u16,
    pub diff: u16,
}

pub const HEADER_SIZE: usize = std::mem::size_of::<Header>();

impl Header {
    pub fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u32_le((self.overlap as u32) << 16 | self.diff as u32);
    }

    pub fn decode(&mut self, bytes: &mut Bytes) {
        let h = bytes.get_u32_le();
        self.overlap = (h >> 16) as u16;
        self.diff = h as u16;
    }
}

/// Builder builds an SST.
pub struct Builder {
    buf: BytesMut,
    base_key: Bytes,
    base_offset: u32,
    entry_offsets: Vec<u32>,
    table_index: TableIndex,
    key_hashes: Vec<u32>,
    options: Options,
    max_version: u64,
}

impl Builder {
    /// Create new builder from options
    pub fn new(options: Options) -> Builder {
        Builder {
            // approximately 16MB index + table size
            buf: BytesMut::with_capacity((16 << 20) + options.table_size as usize),
            table_index: TableIndex::default(),
            key_hashes: Vec::with_capacity(1024),
            base_key: Bytes::new(),
            base_offset: 0,
            entry_offsets: vec![],
            options,
            max_version: 0,
        }
    }

    /// Check if the builder is empty
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn key_diff<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        util::bytes_diff(&self.base_key, key)
    }

    fn add_helper(&mut self, key: &Bytes, v: Value, vlog_len: u32) {
        self.key_hashes.push(farmhash::fingerprint32(user_key(key)));
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
        self.buf.put_u32(self.entry_offsets.len() as u32);

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
        let estimated_size = (self.buf.len() as u32)
            - self.base_offset + 6 /* header size for entry */
            + key.len() as u32
            + value.encoded_size() as u32
            + entries_offsets_size as u32;
        assert!(self.buf.len() + (estimated_size as usize) < u32::MAX as usize);
        estimated_size > self.options.block_size as u32
    }

    /// Add key-value pair to table
    pub fn add(&mut self, key: &Bytes, value: Value, vlog_len: u32) {
        if self.should_finish_block(key, &value) {
            self.finish_block();
            self.base_key.clear();
            assert!(self.buf.len() < u32::MAX as usize);
            self.base_offset = self.buf.len() as u32;
            self.entry_offsets.clear();
        }
        self.add_helper(key, value, vlog_len);
    }

    /// Check if entries reach its capacity
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

    /// Finalize the table
    pub fn finish(&mut self) -> Bytes {
        self.finish_block();
        if self.buf.is_empty() {
            return Bytes::new();
        }
        let mut bytes = BytesMut::new();
        // TODO: move boundaries and build index if we need to encrypt or compress
        if self.options.bloom_false_positive > 0.0 {
            let bits_per_key =
                Bloom::bloom_bits_per_key(self.key_hashes.len(), self.options.bloom_false_positive);
            let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);
            self.table_index.bloom_filter = bloom.to_vec();
        }
        // append index to buffer
        self.table_index.encode(&mut bytes).unwrap();
        assert!(bytes.len() < u32::MAX as usize);
        self.buf.put_slice(&bytes);
        self.buf.put_u32(bytes.len() as u32);
        // append checksum
        let cs = self.build_checksum(&bytes);
        self.write_checksum(cs);
        // TODO: eliminate clone if we do not need builder any more after finish
        self.buf.clone().freeze()
    }

    fn build_checksum(&self, data: &[u8]) -> Checksum {
        Checksum {
            sum: checksum::calculate_checksum(data, ChecksumAlg::Crc32c),
            algo: ChecksumAlg::Crc32c as i32,
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::tests::build_test_table;
    use crate::table::Table;
    use crate::AgateIterator;
    use crate::{format::key_with_ts, ChecksumVerificationMode};
    use tempfile::tempdir;

    const TEST_KEYS_COUNT: usize = 100000;

    #[test]
    fn test_table_index() {
        // TODO: use cache
        let opts = Options {
            block_size: 4 * 1024,
            bloom_false_positive: 0.01,
            table_size: 30 << 20,
            checksum_mode: crate::opt::ChecksumVerificationMode::OnTableAndBlockRead,
        };

        let mut builder = Builder::new(opts.clone());
        let tmp_dir = tempdir().unwrap();
        let filename = tmp_dir.path().join("1.sst".to_string());

        let mut block_first_keys = vec![];

        for i in 0..TEST_KEYS_COUNT {
            let k = key_with_ts(format!("{:016x}", i).as_str(), (i + 1) as u64);
            let v = Bytes::from(i.to_string());
            let vs = Value::new(v);
            if i == 0 {
                block_first_keys.push(k.clone());
            } else if builder.should_finish_block(&k, &vs) {
                block_first_keys.push(k.clone());
            }
            builder.add(&k, vs, 0);
        }

        let table = Table::create(&filename, builder.finish(), opts).unwrap();

        // TODO: data key in options

        assert_eq!(table.offsets_length(), block_first_keys.len());

        let idx = table.inner.read_table_index().unwrap();

        for i in 0..idx.offsets.len() {
            assert_eq!(block_first_keys[i], idx.offsets[i].key);
        }

        // TODO: support max_version
        // assert_eq!(TEST_KEYS_COUNT, table.max_version());
    }

    fn test_with_bloom_filter(with_blooms: bool) {
        let key_prefix = b"p";
        let key_count = 1000;
        let opts = Options {
            block_size: 0,
            bloom_false_positive: if with_blooms { 0.01 } else { 0.0 },
            table_size: 0,
            checksum_mode: ChecksumVerificationMode::OnTableRead,
        };

        let table = build_test_table(key_prefix, key_count, opts);

        assert_eq!(table.has_bloom_filter(), with_blooms);

        let mut it = table.new_iterator(0);
        let mut count = 0;
        it.rewind();
        while it.valid() {
            count += 1;
            let hash = farmhash::fingerprint32(user_key(it.key()));
            assert!(!table.does_not_have(hash));
            it.next();
        }
        assert_eq!(key_count, count);
    }

    #[test]
    fn test_bloom_filter() {
        test_with_bloom_filter(false);
        test_with_bloom_filter(true);
    }

    #[test]
    fn test_empty_builder() {
        let opt = Options {
            bloom_false_positive: 0.1,
            block_size: 0,
            table_size: 0,
            checksum_mode: crate::opt::ChecksumVerificationMode::NoVerification,
        };

        let mut b = Builder::new(opt);

        b.finish();
    }

    #[test]
    fn test_header_encode_decode() {
        let mut header = Header {
            overlap: 23333,
            diff: 23334,
        };
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        let mut buf = buf.freeze();
        header.decode(&mut buf);
        assert_eq!(header.overlap, 23333);
        assert_eq!(header.diff, 23334);
    }
}
