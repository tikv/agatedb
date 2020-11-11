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
        if bytes.len() < 2 {
            return Err(Error::VarDecode("failed to decode, length < 2"));
        }
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
            size: mmap_file.len() as u32,
            mmap_file,
            opts,
            write_at: 0, // TODO: current implementation doesn't have keyID and baseIV header
            buf: BytesMut::new(),
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

    fn decode_entry(buf: &mut Bytes) -> Result<Entry> {
        let mut header = Header::default();
        let header_len = header.decode(buf)?;
        let kv = buf.slice(header_len..);
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

    fn read(&self, p: ValuePointer) -> Result<Bytes> {
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

    pub fn iter(&mut self) -> Result<WalIterator> {
        self.file.seek(SeekFrom::Start(0))?;
        Ok(WalIterator::new(BufReader::new(&mut self.file)))
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

    pub(crate) fn data(&mut self) -> &mut MmapMut {
        &mut self.mmap_file
    }
}

pub struct WalIterator<'a> {
    reader: BufReader<&'a mut File>,
    entry_reader: EntryReader,
}

impl<'a> WalIterator<'a> {
    pub fn new(reader: BufReader<&'a mut File>) -> Self {
        Self {
            reader,
            entry_reader: EntryReader::new(),
        }
    }

    pub fn next(&mut self) -> Option<Result<EntryRef<'_>>> {
        use std::io::ErrorKind;

        let entry = self.entry_reader.entry(&mut self.reader);
        match entry {
            Ok(entry) => {
                if entry.is_zero() {
                    return None;
                }
                // TODO: process transaction-related metadata
                Some(Ok(entry))
            }
            Err(Error::Io(err)) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    None
                } else {
                    return Some(Err(Error::Io(err)));
                }
            }
            Err(err) => Some(Err(err)),
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
        opts.value_log_file_size(4096);
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
        opts.value_log_file_size(4096);
        let wal_path = tmp_dir.path().join("1.wal");
        let mut wal = Wal::open(wal_path, opts).unwrap();
        for i in 0..20 {
            let entry = Entry::new(Bytes::from(i.to_string()), Bytes::from(i.to_string()));
            wal.write_entry(&entry).unwrap();
        }
        let mut it = wal.iter().unwrap();
        let mut cnt = 0;
        while let Some(entry) = it.next() {
            let entry = entry.unwrap();
            assert_eq!(entry.key, cnt.to_string().as_bytes());
            assert_eq!(entry.value, cnt.to_string().as_bytes());
            cnt += 1;
        }
    }
}
