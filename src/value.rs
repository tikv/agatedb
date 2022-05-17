use std::io::{Cursor, Read};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crossbeam_channel::Sender;
use prost::{decode_length_delimiter, encode_length_delimiter, length_delimiter_len};

use crate::{
    entry::{Entry, EntryRef},
    wal::Header,
    Error, Result,
};

pub const VALUE_DELETE: u8 = 1 << 0;
pub const VALUE_POINTER: u8 = 1 << 1;
pub const VALUE_DISCARD_EARLIER_VERSIONS: u8 = 1 << 2;
pub const VALUE_MERGE_ENTRY: u8 = 1 << 3;
pub const VALUE_TXN: u8 = 1 << 6;
pub const VALUE_FIN_TXN: u8 = 1 << 7;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct Value {
    pub meta: u8,
    pub user_meta: u8,
    pub expires_at: u64,
    pub value: Bytes,
    pub version: u64,
}

impl From<Value> for Bytes {
    fn from(value: Value) -> Bytes {
        // TODO: we can reduce unnecessary copy by re-writing `encode`
        let mut buf = BytesMut::new();
        value.encode(&mut buf);
        buf.freeze()
    }
}

impl Value {
    pub fn new(value: Bytes) -> Self {
        Self {
            value,
            ..Self::default()
        }
    }

    pub fn new_with_meta(value: Bytes, meta: u8, user_meta: u8) -> Self {
        Self {
            meta,
            user_meta,
            value,
            ..Self::default()
        }
    }

    pub fn encoded_size(&self) -> usize {
        std::mem::size_of::<u8>() * 2
            + length_delimiter_len(self.expires_at as usize)
            + self.value.len()
    }

    pub fn decode(&mut self, mut bytes: Bytes) {
        self.meta = bytes.get_u8();
        self.user_meta = bytes.get_u8();

        self.expires_at = decode_length_delimiter(&mut bytes).unwrap() as u64;
        self.value = bytes;
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.meta);
        buf.put_u8(self.user_meta);

        encode_length_delimiter(self.expires_at as usize, buf).unwrap();
        buf.put_slice(&self.value);
    }
}

/// A request contains multiple entries to be written into LSM tree.
#[derive(Clone)]
pub struct Request {
    /// Entries contained in this request
    pub entries: Vec<Entry>,
    /// Offset in vLog (will be updated upon processing the request)
    pub ptrs: Vec<ValuePointer>,
    /// Use channel to notify that the value has been persisted to disk
    pub done: Option<Sender<Result<()>>>,
}

/// `ValuePointer` records the position of value saved in value log.
#[derive(Clone, Default, Debug)]
pub struct ValuePointer {
    pub file_id: u32,
    pub len: u32,
    pub offset: u32,
}

impl ValuePointer {
    pub fn decode(&mut self, mut bytes: &[u8]) {
        self.file_id = bytes.get_u32_le();
        self.len = bytes.get_u32_le();
        self.offset = bytes.get_u32_le();
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.file_id);
        buf.put_u32_le(self.len);
        buf.put_u32_le(self.offset);
    }

    pub fn encoded_size() -> usize {
        std::mem::size_of::<u32>() * 3
    }
}

/// `EntryReader` reads entries from the `Cursor` with the `entry` function.
pub struct EntryReader {
    key: Vec<u8>,
    value: Vec<u8>,
    header: Header,
}

impl EntryReader {
    pub fn new() -> Self {
        Self {
            key: vec![],
            value: vec![],
            header: Header::default(),
        }
    }

    /// Entry returns header, key and value.
    pub fn entry(&mut self, reader: &mut Cursor<&[u8]>) -> Result<EntryRef> {
        self.header.decode(reader)?;
        if self.header.key_len > (1 << 16) {
            return Err(Error::LogRead(
                "key length must not be larger than 1 << 16".to_string(),
            ));
        }
        self.key.resize(self.header.key_len as usize, 0);
        reader.read_exact(&mut self.key)?;
        self.value.resize(self.header.value_len as usize, 0);
        reader.read_exact(&mut self.value)?;
        Ok(EntryRef {
            key: &self.key,
            value: &self.value,
            meta: self.header.meta,
            user_meta: self.header.user_meta,
            expires_at: self.header.expires_at,
            // This `version` is currently not used anywhere, and we may remove it later.
            version: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let check = |v: &Value| {
            let mut buf = BytesMut::new();
            v.encode(&mut buf);

            let bytes = buf.freeze();

            let mut new_v = Value::default();
            new_v.decode(bytes);

            assert_eq!(*v, new_v);
        };

        let v = Value::new(Bytes::from("hello world"));
        check(&v);

        let v = Value::new_with_meta(Bytes::from("hello world"), 1, 2);
        check(&v);
    }
}
