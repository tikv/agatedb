use bytes::{BufMut, Bytes, BytesMut};
use std::{ptr, u64};

pub fn key_with_ts(key: impl Into<BytesMut>, ts: u64) -> Bytes {
    let mut key = key.into();
    append_ts(&mut key, ts);
    key.freeze()
}

/// Append a ts to make this key be the first one within range.
pub fn key_with_ts_first(key: impl Into<BytesMut>) -> Bytes {
    key_with_ts(key, std::u64::MAX)
}

/// Append a ts to make this key be the last one within range.
pub fn key_with_ts_last(key: impl Into<BytesMut>) -> Bytes {
    key_with_ts(key, 0)
}

pub fn append_ts(key: &mut BytesMut, ts: u64) {
    key.reserve(8);
    let res = (u64::MAX - ts).to_be();
    let buf = key.chunk_mut();
    unsafe {
        ptr::copy_nonoverlapping(
            &res as *const u64 as *const u8,
            buf.as_mut_ptr() as *mut _,
            8,
        );
        key.advance_mut(8);
    }
}

pub fn get_ts(key: &[u8]) -> u64 {
    let mut ts: u64 = 0;
    unsafe {
        let src = &key[key.len() - 8..];
        ptr::copy_nonoverlapping(src.as_ptr(), &mut ts as *mut u64 as *mut u8, 8);
    }
    u64::MAX - u64::from_be(ts)
}

pub fn user_key(key: &[u8]) -> &[u8] {
    &key[..key.len() - 8]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_ts() {
        let key = key_with_ts("aaa", 0);
        assert_eq!(get_ts(&key), 0);
    }
}
