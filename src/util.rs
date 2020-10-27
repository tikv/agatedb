pub use skiplist::{FixedLengthSuffixComparator, KeyComparator};
use std::{cmp, ptr};
pub static COMPARATOR: FixedLengthSuffixComparator = FixedLengthSuffixComparator::new(8);
use bytes::BytesMut;
use std::mem::MaybeUninit;

unsafe fn u64(ptr: *const u8) -> u64 {
    ptr::read_unaligned(ptr as *const u64)
}

unsafe fn u32(ptr: *const u8) -> u32 {
    ptr::read_unaligned(ptr as *const u32)
}

#[inline]
pub fn bytes_diff<'a, 'b>(base: &'a [u8], target: &'b [u8]) -> &'b [u8] {
    let end = cmp::min(base.len(), target.len());
    let mut i = 0;
    unsafe {
        while i + 8 <= end {
            if u64(base.as_ptr().add(i)) != u64(target.as_ptr().add(i)) {
                break;
            }
            i += 8;
        }
        if i + 4 <= end {
            if u32(base.as_ptr().add(i)) == u32(target.as_ptr().add(i)) {
                i += 4;
            }
        }
        while i < end {
            if base.get_unchecked(i) != target.get_unchecked(i) {
                return target.get_unchecked(i..);
            }
            i += 1;
        }
        target.get_unchecked(end..)
    }
}

/// simple rewrite of golang sort.Search
pub fn search<F>(n: usize, mut f: F) -> usize
where
    F: FnMut(usize) -> bool,
{
    let mut i = 0;
    let mut j = n;
    while i < j {
        let h = (i + j) / 2;
        if !f(h) {
            i = h + 1;
        } else {
            j = h;
        }
    }
    i
}

pub fn varint_len(l: usize) -> usize {
    if l < 0x80 {
        1
    } else if l < (1usize << 14) {
        2
    } else if l < (1usize << 21) {
        3
    } else if l < (1usize << 28) {
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

pub unsafe fn encode_varint_uncheck(bytes: &mut [MaybeUninit<u8>], mut u: u64) -> usize {
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

pub unsafe fn decode_varint_uncheck(bytes: &[u8]) -> (u64, usize) {
    let mut x: u64 = 0;
    let mut s = 0;
    let mut i = 0;
    let mut p = bytes.as_ptr();
    loop {
        let b = *p;
        if b < 0x80 {
            if i > 9 || i == 9 && b > 1 {
                return (0, std::usize::MAX);
            }
            return (x | ((b as u64) << s), i + 1);
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
        i += 1;
        p = p.add(1);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_varint_decode_encode() {}
}
