use bytes::Bytes;

const DELETE: u8 = 1 << 0;
const VALUE_POINTER: u8 = 1 << 1;

pub struct Entry {
    pub key: Bytes,
    pub value: Bytes,
    pub(crate) meta: u8,
    pub user_meta: u8,
    pub expires_at: u64,
    pub(crate) version: u64
}

impl Entry {
    pub fn new(key: Bytes, value: Bytes) -> Entry {
        Entry {
            key,
            value,
            meta: 0,
            user_meta: 0,
            expires_at: 0,
            version: 0
        }
    }

    pub fn mark_delete(&mut self) {
        self.meta |= DELETE;
    }

    // TODO: entry encoding will be done later, as current WAL encodes header and key / value separately
    /*
    pub fn encoded_len(&self) -> usize {
        let kl = self.key.len();
        let vl = self.value.len();
        kl + vl + varint_len(kl) + varint_len(vl) + 1
    }

    pub fn encode(&self, bytes: &mut BytesMut) {
        let encoded_len = self.encoded_len();
        bytes.reserve(encoded_len);
        unsafe {
            let buf = bytes.bytes_mut();
            *(*buf.get_unchecked_mut(0)).as_mut_ptr() = self.meta;
            let mut read = 1;
            read += encode_varint_uncheck(buf.get_unchecked_mut(read..), self.key.len() as u64);
            read += encode_varint_uncheck(buf.get_unchecked_mut(read..), self.value.len() as u64);
            ptr::copy_nonoverlapping(
                self.key.as_ptr(),
                buf.as_mut_ptr().add(read) as _,
                self.key.len(),
            );
            ptr::copy_nonoverlapping(
                self.value.as_ptr(),
                buf.as_mut_ptr().add(read + self.key.len()) as _,
                self.value.len(),
            );
        }
        unsafe {
            bytes.advance_mut(encoded_len);
        }
    }*/
}
