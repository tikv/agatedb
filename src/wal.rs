use crate::util::binary::{
    decode_varint_u32, decode_varint_u64, encode_varint_u32_to_array, encode_varint_u64_to_array,
    varint_u32_bytes_len, varint_u64_bytes_len,
};
use bytes::{BufMut, Bytes, BytesMut};
use crate::structs::Entry;
use crate::AgateOptions;
use crate::Result;
use memmap::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use bytes::BytesMut;
use std::path::PathBuf;

const MAX_HEADER_SIZE: usize = 21;

/// `Header` stores metadata of an entry in WAL and in value log.
#[derive(Default, Debug, PartialEq)]
struct Header {
    /// length of key
    key_len: u32,
    /// length of value
    value_len: u32,
    /// entry expire date
    expires_at: u64,
    /// metadata
    meta: u8,
    /// user metadata
    user_meta: u8,
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
    pub fn decode(&mut self, bytes: &mut Bytes) -> Result<usize> {
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
}

impl Wal {
    pub fn open(path: PathBuf, opts: AgateOptions) -> Result<Wal> {
        let (file, bootstrap) = if path.exists() {
            (
                OpenOptions::new()
                    .create(false)
                    .read(true)
                    .write(true)
                    .open(&path)?,
                false,
            )
        } else {
            let mut file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?;
            // TODO: use mmap to specify size instead of filling up the file
            crate::util::fill_file(&mut file, 2 * opts.value_log_file_size)?;
            file.sync_all()?;
            (file, true)
        };
        let mmap_file = unsafe { MmapOptions::new().map_mut(&file)? };
        let mut wal = Wal {
            path,
            file,
            mmap_file,
            opts,
            write_at: 0
        };

        if bootstrap {
            wal.bootstrap()?;
        }

        // TODO: we should read vlog headers and data key from wal.
        // But at this time, I'm not sure about this part. So we just
        // let the WAL to solely store entries.

        Ok(wal)
    }

    fn bootstrap(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn write_entry(&self, _entry: Entry) -> Result<()> {
        // TODO: not implementaed
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.mmap_file.flush()?;
        Ok(())
    }

    pub fn zero_next_entry(&mut self) -> Result<()> {
        let range =
            &mut self.mmap_file[self.write_at as usize..self.write_at as usize + MAX_HEADER_SIZE];
        // TODO: optimize zero fill
        range.fill(0);
        Ok(())
    }

    pub fn encode_entry(buf: &mut BytesMut, entry: &Entry, offset: u32) {
        let header = Header {
            key_len: entry.key.len(),
            value_len: entry.value.len(),

        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    #[test]
    fn test_wal_create() {
        let tmp_dir = TempDir::new("agatedb").unwrap();
        let mut opts = AgateOptions::default();
        opts.value_log_file_size(4096);
        Wal::open(tmp_dir.path().join("1.wal"), opts).unwrap();
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
