use bytes::Bytes;
use core::mem::MaybeUninit;

const DELETE: u8 = 1 << 0;
const VALUE_POINTER: u8 = 1 << 1;

pub struct Entry {
    pub key: Bytes,
    pub value: Bytes,
    pub meta: u8,
}

fn varint_len(l: usize) -> usize {
    if l < 0x80 {
        1
    } else if l < (1 << 14) {
        2
    } else if l < (1 << 21) {
        3
    } else if l < (1 << 28) {
        4
    } else if l < (1usize << 35) {
        5
    } else if l < (1usize << 42) {
        6
    } else if l < (1usize << 49) {
        7
    } else if l < (1usize << 56) {
        8
    } else if l < (1usize << 63) {
        9
    } else {
        10
    }
}

unsafe fn encode_varint_uncheck(bytes: &mut [MaybeUninit<u8>], mut u: u64) -> usize {
    let mut p = bytes.as_mut_ptr();
    let mut c = 0;
    while u >= 0x80 {
        (*(&mut *p).as_mut_ptr()) = (u as u8 & 0x7f) | 0x80;
        p = p.add(1);
        u >>= 7;
        c += 1;
    }
    (*(&mut *p).as_mut_ptr()) = u as u8;
    c + 1
}

unsafe fn decode_varint_uncheck(bytes: &[u8]) -> (u64, usize) {
    let mut a = 0;
    let mut p = bytes.as_ptr();
    let mut c = 0;
    while *p >= 0x80 {
        a = (a << 7) | (*p & 0x7f) as u64;
        p = p.add(1);
        c += 1;
    }
    ((a << 7) | (*p) as u64, c + 1)
}

impl Entry {
    pub fn new(key: Bytes, value: Bytes) -> Entry {
        Entry {
            key,
            value,
            meta: 0,
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
