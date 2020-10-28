use super::Result;
use crate::util::binary::{
    decode_varint_u32, decode_varint_u64, encode_varint_u32_to_array, encode_varint_u64_to_array,
    varint_u32_bytes_len, varint_u64_bytes_len,
};
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
            + varint_u64_bytes_len(self.expires_at) as usize
            + varint_u32_bytes_len(self.key_len) as usize
            + varint_u32_bytes_len(self.value_len) as usize
    }

    pub fn encode(&self, bytes: &mut BytesMut) {
        let encoded_len = self.encoded_len();
        bytes.reserve(encoded_len);
        unsafe {
            let buf = bytes.bytes_mut();
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
        new_header.decode(&mut buf).unwrap();
        assert_eq!(new_header, header);
    }
}
