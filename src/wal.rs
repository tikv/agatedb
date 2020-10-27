use super::Result;
use crate::util::{decode_varint_uncheck, encode_varint_uncheck, varint_len};
use bytes::{BufMut, Bytes, BytesMut};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;


#[derive(Default, Debug, PartialEq)]
struct Header {
    key_len: u32,
    value_len: u32,
    expires_at: u64,
    meta: u8,
    user_meta: u8,
}

impl Header {
    pub fn encoded_len(&self) -> usize {
        1 + 1
            + varint_len(self.expires_at as usize)
            + varint_len(self.key_len as usize)
            + varint_len(self.value_len as usize)
    }

    pub fn encode(&self, bytes: &mut BytesMut) {
        let encoded_len = self.encoded_len();
        bytes.reserve(encoded_len);
        let read = unsafe {
            let buf = bytes.bytes_mut();
            *(*buf.get_unchecked_mut(0)).as_mut_ptr() = self.meta;
            *(*buf.get_unchecked_mut(1)).as_mut_ptr() = self.user_meta;
            let mut read = 2;
            read += encode_varint_uncheck(buf.get_unchecked_mut(read..), self.key_len as u64);
            read += encode_varint_uncheck(buf.get_unchecked_mut(read..), self.value_len as u64);
            read += encode_varint_uncheck(buf.get_unchecked_mut(read..), self.expires_at);
            read
        };
        assert_eq!(read, encoded_len);
        unsafe {
            bytes.advance_mut(read);
        }
    }

    pub fn decode(&mut self, bytes: &mut Bytes) -> usize {
        self.meta = bytes[0];
        self.user_meta = bytes[1];
        let mut index = 2;
        let (key_len, count) = unsafe { decode_varint_uncheck(&bytes[index..]) };
        self.key_len = key_len as u32;
        index += count;
        let (value_len, count) = unsafe { decode_varint_uncheck(&bytes[index..]) };
        self.value_len = value_len as u32;
        index += count;
        let (expires_at, count) = unsafe { decode_varint_uncheck(&bytes[index..]) };
        self.expires_at = expires_at;
        index += count;
        index
    }
}

pub struct Wal {
    f: File,
    path: PathBuf,
}

impl Wal {
    pub fn open(path: PathBuf) -> Result<Wal> {
        let f = OpenOptions::new().append(true).create(true).open(&path)?;
        Ok(Wal { f, path })
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
        new_header.decode(&mut buf);
        assert_eq!(new_header, header);
    }
}
