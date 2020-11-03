use crate::entry::{Entry, EntryRef};
use crate::util::binary::{
    decode_varint_u32, decode_varint_u64, encode_varint_u32_to_array, encode_varint_u64_to_array,
    varint_u32_bytes_len, varint_u64_bytes_len,
};
use crate::value::EntryReader;
use crate::value::ValuePointer;
use crate::AgateOptions;
use crate::Error;
use crate::Result;
use bytes::{BufMut, Bytes, BytesMut};
use memmap::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

pub const MAX_HEADER_SIZE: usize = 21;

/// `Header` stores metadata of an entry in WAL and in value log.
#[derive(Default, Debug, PartialEq)]
pub struct Header {
    /// length of key
    pub key_len: u32,
    /// length of value
    pub value_len: u32,
    /// entry expire date
    pub expires_at: u64,
    /// metadata
    pub(crate) meta: u8,
    /// user metadata
    pub user_meta: u8,
}

impl Header {
    /// Get length of header if being encoded
    pub fn encoded_len(&self) -> usize {
        1 + 1
            + varint_u64_bytes_len(self.expires_at) as usize
            + varint_u32_bytes_len(self.key_len) as usize
            + varint_u32_bytes_len(self.value_len) as usize
    }

    /// Encode header into bytes
    pub fn encode(&self, bytes: &mut BytesMut) {
        let encoded_len = self.encoded_len();
        bytes.reserve(encoded_len);
        unsafe {
            let buf = bytes.bytes_mut();
            assert!(buf.len() >= encoded_len);
            *(*buf.get_unchecked_mut(0)).as_mut_ptr() = self.meta;
            *(*buf.get_unchecked_mut(1)).as_mut_ptr() = self.user_meta;
            let mut index = 2;
            index += encode_varint_u32_to_array(
                (*buf.get_unchecked_mut(index)).as_mut_ptr(),
                self.key_len,
            );
            index += encode_varint_u32_to_array(
                (*buf.get_unchecked_mut(index)).as_mut_ptr(),
                self.value_len,
            );
            index += encode_varint_u64_to_array(
                (*buf.get_unchecked_mut(index)).as_mut_ptr(),
                self.expires_at,
            );
            bytes.advance_mut(index);
        }
        debug_assert_eq!(bytes.len(), encoded_len);
    }

    /// Decode header from bytes
    pub fn decode(&mut self, bytes: &[u8]) -> Result<usize> {
        self.meta = bytes[0];
        self.user_meta = bytes[1];
        let mut read = 2;
        let (key_len, cnt) = decode_varint_u32(&bytes[read..])?;
        read += cnt as usize;
        self.key_len = key_len;
        let (value_len, cnt) = decode_varint_u32(&bytes[read..])?;
        read += cnt as usize;
        self.value_len = value_len;
        let (expires_at, cnt) = decode_varint_u64(&bytes[read..])?;
        read += cnt as usize;
        self.expires_at = expires_at;
        Ok(read)
    }
}

/// WAL of a memtable
///
/// TODO: delete WAL file when reference to WAL (or memtable) comes to 0
pub struct Wal {
    path: PathBuf,
    file: File,
    mmap_file: MmapMut,
    opts: AgateOptions,
    write_at: u32,
    buf: BytesMut,
    size: u32,
}

impl Wal {
    pub fn open(path: PathBuf, opts: AgateOptions) -> Result<Wal> {
        unimplemented!()
    }

    fn bootstrap(&mut self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn write_entry(&mut self, entry: &Entry) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&mut self) -> Result<()> {
        unimplemented!()
    }

    pub fn zero_next_entry(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn encode_entry(mut buf: &mut BytesMut, entry: &Entry) {
        unimplemented!()
    }

    fn decode_entry(buf: &mut Bytes) -> Result<Entry> {
        unimplemented!()
    }

    fn read(&self, p: ValuePointer) -> Result<Bytes> {
        unimplemented!()
    }

    pub fn truncate(&mut self, end: u64) -> Result<()> {
        unimplemented!()
    }

    fn done_writing(&mut self, offset: u32) -> Result<()> {
        unimplemented!()
    }

    pub fn iter(&mut self) -> Result<WalIterator> {
        unimplemented!()
    }
}

pub struct WalIterator<'a> {
    reader: BufReader<&'a mut File>,
    entry_reader: EntryReader,
}

impl<'a> WalIterator<'a> {
    pub fn new(reader: BufReader<&'a mut File>) -> Self {
        unimplemented!()
    }

    pub fn next(&mut self) -> Option<Result<EntryRef<'_>>> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_encode() {
        let header = Header {
            key_len: 233333,
            value_len: 2333,
            expires_at: std::u64::MAX - 2333333,
            user_meta: b'A',
            meta: b'B',
        };

        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        let mut buf = buf.freeze();

        let mut new_header = Header::default();
        new_header.decode(&mut buf).unwrap();
        assert_eq!(new_header, header);
    }
}
