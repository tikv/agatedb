use crate::entry::{Entry, EntryRef};
use crate::value::{EntryReader, ValuePointer};
use crate::AgateOptions;
use crate::Error;
use crate::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use memmap::{MmapMut, MmapOptions};
use prost::{decode_length_delimiter, encode_length_delimiter, length_delimiter_len};
use std::fs::{File, OpenOptions};

use std::fs;
use std::io::Cursor;
use std::mem::ManuallyDrop;
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
            + length_delimiter_len(self.expires_at as usize)
            + length_delimiter_len(self.key_len as usize)
            + length_delimiter_len(self.value_len as usize)
    }

    /// Encode header into bytes
    pub fn encode(&self, bytes: &mut BytesMut) {
        let encoded_len = self.encoded_len();
        bytes.reserve(encoded_len);

        bytes.put_u8(self.meta);
        bytes.put_u8(self.user_meta);
        encode_length_delimiter(self.key_len as usize, bytes).unwrap();
        encode_length_delimiter(self.value_len as usize, bytes).unwrap();
        encode_length_delimiter(self.expires_at as usize, bytes).unwrap();
    }

    /// Decode header from bytes
    pub fn decode(&mut self, mut bytes: &mut impl Buf) -> Result<()> {
        self.meta = bytes.get_u8();
        self.user_meta = bytes.get_u8();
        self.key_len = decode_length_delimiter(&mut bytes)? as u32;
        self.value_len = decode_length_delimiter(&mut bytes)? as u32;
        self.expires_at = decode_length_delimiter(&mut bytes)? as u64;
        Ok(())
    }
}

/// WAL of a memtable
///
/// TODO: delete WAL file when reference to WAL (or memtable) comes to 0
pub struct Wal {
    path: PathBuf,
    file: ManuallyDrop<File>,
    mmap_file: ManuallyDrop<MmapMut>,
    opts: AgateOptions,
    write_at: u32,
    buf: BytesMut,
    size: u32,
    save_after_close: bool,
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
            let file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?;
            // TODO: use mmap to specify size instead of filling up the file
            file.set_len(2 * opts.value_log_file_size)?;
            file.sync_all()?;
            (file, true)
        };
        let mmap_file = unsafe { MmapOptions::new().map_mut(&file)? };
        let mut wal = Wal {
            path,
            file: ManuallyDrop::new(file),
            size: mmap_file.len() as u32,
            mmap_file: ManuallyDrop::new(mmap_file),
            opts,
            write_at: 0, // TODO: current implementation doesn't have keyID and baseIV header
            buf: BytesMut::new(),
            save_after_close: false,
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
        self.zero_next_entry()?;
        Ok(())
    }

    pub(crate) fn write_entry(&mut self, entry: &Entry) -> Result<()> {
        self.buf.clear();
        Self::encode_entry(&mut self.buf, entry);
        self.mmap_file[self.write_at as usize..self.write_at as usize + self.buf.len()]
            .clone_from_slice(&self.buf[..]);
        self.write_at += self.buf.len() as u32;
        self.zero_next_entry()?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
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

    pub(crate) fn encode_entry(mut buf: &mut BytesMut, entry: &Entry) -> usize {
        let header = Header {
            key_len: entry.key.len() as u32,
            value_len: entry.value.len() as u32,
            expires_at: entry.expires_at,
            meta: entry.meta,
            user_meta: entry.user_meta,
        };

        // write header to buffer
        header.encode(&mut buf);

        // write key and value to buffer
        // TODO: encryption
        buf.extend_from_slice(&entry.key);
        buf.extend_from_slice(&entry.value);

        // TODO: add CRC32 check

        return buf.len();
    }

    fn decode_entry(mut buf: &mut Bytes) -> Result<Entry> {
        let mut header = Header::default();
        let _header_len = header.decode(&mut buf)?;
        let kv = buf;
        Ok(Entry {
            meta: header.meta,
            user_meta: header.user_meta,
            expires_at: header.expires_at,
            key: kv.slice(..header.key_len as usize),
            value: kv.slice(
                header.key_len as usize..header.key_len as usize + header.value_len as usize,
            ),
            version: 0,
        })
    }

    pub(crate) fn read(&self, p: &ValuePointer) -> Result<Bytes> {
        let offset = p.offset;
        let size = self.mmap_file.len() as u64;
        let value_size = p.len;
        let log_size = self.size;

        if offset as u64 >= size
            || offset as u64 + value_size as u64 > size
            || offset as u64 + value_size as u64 > log_size as u64
        {
            return Err(Error::LogRead("EOF".to_string()));
        }

        Ok(Bytes::copy_from_slice(
            &self.mmap_file[offset as usize..offset as usize + value_size as usize],
        ))
    }

    pub fn truncate(&mut self, end: u64) -> Result<()> {
        // TODO: check read only
        let metadata = self.file.metadata()?;
        if metadata.len() == end {
            return Ok(());
        }
        self.size = end as u32;
        self.file.set_len(end)?;
        Ok(())
    }

    pub(crate) fn done_writing(&mut self, offset: u32) -> Result<()> {
        if self.opts.sync_writes {
            self.file.sync_all()?;
        }
        self.truncate(offset as u64)?;
        Ok(())
    }

    /// Get WAL iterator
    pub fn iter(&mut self) -> Result<WalIterator> {
        Ok(WalIterator::new(Cursor::new(
            &self.mmap_file[0..self.size as usize],
        )))
    }

    pub fn should_flush(&self) -> bool {
        self.write_at as u64 > self.opts.value_log_file_size
    }

    pub(crate) fn size(&self) -> u32 {
        self.size
    }

    pub(crate) fn set_size(&mut self, size: u32) {
        self.size = size;
    }

    pub(crate) fn set_len(&mut self, len: u64) -> Result<()> {
        self.file.set_len(len)?;
        Ok(())
    }

    pub(crate) fn data(&mut self) -> &mut MmapMut {
        &mut self.mmap_file
    }

    pub fn close_and_save(mut self) {
        self.save_after_close = true;
    }
}
pub struct WalIterator<'a> {
    /// `reader` stores the file to read
    reader: Cursor<&'a [u8]>,
    /// `entry_reader` operates on `reader` and buffers entry information
    entry_reader: EntryReader,
}

impl<'a> WalIterator<'a> {
    pub fn new(reader: Cursor<&'a [u8]>) -> Self {
        Self {
            reader,
            entry_reader: EntryReader::new(),
        }
    }

    /// Get next entry from WAL
    pub fn next(&mut self) -> Result<Option<EntryRef<'_>>> {
        use std::io::ErrorKind;

        let entry = self.entry_reader.entry(&mut self.reader);

        match entry {
            Ok(entry) => {
                if entry.is_zero() {
                    return Ok(None);
                }
                // TODO: process transaction-related metadata
                Ok(Some(entry))
            }
            Err(Error::Io(err)) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    Ok(None)
                } else {
                    Err(Error::Io(err))
                }
            }
            Err(err) => Err(err),
        }
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.mmap_file);
        }
        let file = unsafe { ManuallyDrop::take(&mut self.file) };
        if !self.save_after_close {
            file.set_len(0).unwrap();
            drop(file);
            fs::remove_file(&self.path).unwrap();
        }
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
        opts.value_log_file_size = 4096;
        Wal::open(tmp_dir.path().join("1.wal"), opts).unwrap();
    }

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

    #[test]
    fn test_wal_iterator() {
        let tmp_dir = TempDir::new("agatedb").unwrap();
        let mut opts = AgateOptions::default();
        opts.value_log_file_size = 4096;
        let wal_path = tmp_dir.path().join("1.wal");
        let mut wal = Wal::open(wal_path, opts).unwrap();
        for i in 0..20 {
            let entry = Entry::new(Bytes::from(i.to_string()), Bytes::from(i.to_string()));
            wal.write_entry(&entry).unwrap();
        }
        let mut it = wal.iter().unwrap();
        let mut cnt = 0;
        while let Some(entry) = it.next().unwrap() {
            assert_eq!(entry.key, cnt.to_string().as_bytes());
            assert_eq!(entry.value, cnt.to_string().as_bytes());
            cnt += 1;
        }
    }
}
