use crate::{Error, Result};
use bytes::BytesMut;
use std::{cmp, mem, ptr, slice, u64};

fn decode_varint_u32_slow(data: &[u8]) -> Result<(u32, u8)> {
    let mut res = 0;
    for i in 0..data.len() {
        let b = data[i];
        res |= (b as u32 & 0x7f) << (i * 7);
        if b < 0x80 {
            return Ok((res, i as u8 + 1));
        }
    }
    Err(Error::VarDecode("Truncated"))
}

fn decode_varint_u32_fallback(data: &[u8]) -> Result<(u32, u8)> {
    if data.len() >= 5 || *data.last().unwrap() < 0x80 {
        let mut res = 0;
        for i in 0..4 {
            let b = unsafe { *data.get_unchecked(i) };
            res |= ((b & 0x7f) as u32) << (i * 7);
            if b < 0x80 {
                return Ok((res, i as u8 + 1));
            }
        }
        let b = unsafe { *data.get_unchecked(4) };
        res |= (b as u32) << (4 * 7);
        // Technically, it should be <= 15, but protobuf requires not check the boundary.
        if b < 0x80 {
            return Ok((res, 5));
        }
        for i in 5..cmp::min(10, data.len()) {
            let b = unsafe { *data.get_unchecked(i) };
            if b < 0x80 {
                return Ok((res, i as u8 + 1));
            }
        }
        if data.len() >= 10 {
            return Err(Error::VarDecode("Corrupted"));
        } else {
            return Err(Error::VarDecode("Truncated"));
        }
    }
    decode_varint_u32_slow(data)
}

fn decode_varint_u64_slow(data: &[u8]) -> Result<(u64, u8)> {
    let mut res = 0;
    for i in 0..data.len() {
        let b = data[i];
        res |= (b as u64 & 0x7f) << (i * 7);
        if b < 0x80 {
            return Ok((res, i as u8 + 1));
        }
    }
    Err(Error::VarDecode("Truncated"))
}

fn decode_varint_u64_fallback(data: &[u8]) -> Result<(u64, u8)> {
    if data.len() >= 10 || *data.last().unwrap() < 0x80 {
        let mut res = 0;
        for i in 0..9 {
            let b = unsafe { *data.get_unchecked(i) };
            res |= ((b & 0x7f) as u64) << (i * 7);
            if b < 0x80 {
                return Ok((res, i as u8 + 1));
            }
        }
        let b = unsafe { *data.get_unchecked(9) };
        if b <= 1 {
            res |= (b as u64) << (9 * 7);
            return Ok((res, 10));
        }
        return Err(Error::VarDecode("Corrupted"));
    }
    decode_varint_u64_slow(data)
}

#[inline]
pub fn decode_varint_u32(data: &[u8]) -> Result<(u32, u8)> {
    if !data.is_empty() {
        // process with value < 127 independently at first
        // since it matches most of the cases.
        if data[0] < 0x80 {
            let res = u32::from(data[0]);
            return Ok((res, 1));
        }

        return decode_varint_u32_fallback(data);
    }

    Err(Error::VarDecode("Truncated"))
}

#[inline]
pub fn decode_varint_u64(data: &[u8]) -> Result<(u64, u8)> {
    if !data.is_empty() {
        // process with value < 127 independently at first
        // since it matches most of the cases.
        if data[0] < 0x80 {
            let res = u64::from(data[0]);
            return Ok((res, 1));
        }

        return decode_varint_u64_fallback(data);
    }

    Err(Error::VarDecode("Truncated"))
}

macro_rules! var_encode {
    ($t:ty, $arr_func:ident, $enc_func:ident, $max:expr) => {
        pub unsafe fn $arr_func(start: *mut u8, mut n: $t) -> usize {
            let mut cur = start;
            while n >= 0x80 {
                ptr::write(cur, 0x80 | n as u8);
                n >>= 7;
                cur = cur.add(1);
            }
            ptr::write(cur, n as u8);
            cur as usize - start as usize + 1
        }

        pub fn $enc_func(data: &mut BytesMut, n: $t) {
            let left = data.capacity() - data.len();
            if left >= $max {
                let old_len = data.len();
                unsafe {
                    let start = data.as_mut_ptr().add(old_len);
                    let len = $arr_func(start, n);
                    data.set_len(old_len + len);
                }
                return;
            }

            let mut buf = [0; $max];

            unsafe {
                let start = buf.as_mut_ptr();
                let len = $arr_func(start, n);
                let written = slice::from_raw_parts(start, len);
                data.extend_from_slice(written);
            }
        }
    };
}

var_encode!(u32, encode_varint_u32_to_array, encode_varint_u32, 5);
var_encode!(u64, encode_varint_u64_to_array, encode_varint_u64, 10);

#[inline]
pub fn varint_u32_bytes_len(mut n: u32) -> u32 {
    n |= 0x01;
    ((31 - n.leading_zeros()) * 9 + 73) / 64
}

#[inline]
pub fn varint_u64_bytes_len(mut n: u64) -> u32 {
    n |= 0x01;
    ((63 - n.leading_zeros()) * 9 + 73) / 64
}

#[inline]
pub fn encode_bytes(data: &mut BytesMut, bytes: &[u8]) {
    encode_varint_u32(data, bytes.len() as u32);
    data.extend_from_slice(bytes);
}

#[inline]
pub fn decode_bytes<'a>(data: &'a [u8]) -> Result<(&'a [u8], usize)> {
    let (len, read) = decode_varint_u32(data)?;
    let res = if data.len() > len as usize {
        unsafe { data.get_unchecked(read as usize..len as usize) }
    } else {
        return Err(Error::VarDecode("Truncated"));
    };
    Ok((res, read as usize + len as usize))
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{i64, u32};

    macro_rules! check_res {
        ($num:ident, $res:expr, $len:expr, 1) => {
            let (res, read) = $res;
            assert_eq!(res, $num, "decode for {}", $num);
            assert_eq!(usize::from(read), $len, "decode for {}", $num);
        };
        ($num:ident, $res:expr, $len:expr, 0) => {
            let res = $res;
            assert_eq!(res, $num, "decode for {}", $num);
        };
    }

    macro_rules! make_test {
        ($test_name:ident, $cases:expr, $enc:ident, $dec:ident, $len:ident, $check_len:tt) => {
            #[test]
            fn $test_name() {
                let cases = $cases;

                let mut large_buffer = BytesMut::new();

                for (bin, n) in cases {
                    let mut output = BytesMut::new();
                    $enc(&mut output, n);
                    assert_eq!(output, bin, "encode for {}", n);

                    large_buffer.clear();
                    $enc(&mut large_buffer, n);
                    assert_eq!(large_buffer, bin, "encode for {}", n);

                    assert_eq!($len(n), bin.len() as u32, "{:?}", n);
                    check_res!(n, $dec(&mut output).unwrap(), bin.len(), $check_len);
                }
            }
        };
        ($test_name:ident, $cases:expr, $enc:ident, $dec:ident, $len:ident) => {
            make_test!($test_name, $cases, $enc, $dec, $len, 1);
        };
    }

    make_test!(
        test_varint_u64,
        vec![
            (vec![1], 1),
            (vec![150, 1], 150),
            (vec![127], 127),
            (vec![128, 127], 16256),
            (vec![128, 128, 127], 2080768),
            (vec![128, 128, 128, 127], 266338304),
            (vec![128, 128, 128, 128, 127], 34091302912),
            (vec![128, 128, 128, 128, 128, 127], 4363686772736),
            (vec![128, 128, 128, 128, 128, 128, 127], 558551906910208),
            (
                vec![128, 128, 128, 128, 128, 128, 128, 127],
                71494644084506624
            ),
            (
                vec![128, 128, 128, 128, 128, 128, 128, 128, 127],
                9151314442816847872
            ),
            (vec![255, 255, 255, 255, 15], u32::MAX as u64),
            (
                vec![255, 255, 255, 255, 255, 255, 255, 255, 255, 1],
                u64::MAX
            ),
        ],
        encode_varint_u64,
        decode_varint_u64,
        varint_u64_bytes_len
    );

    make_test!(
        test_varint_u32,
        vec![
            (vec![1], 1),
            (vec![150, 1], 150),
            (vec![127], 127),
            (vec![128, 127], 16256),
            (vec![128, 128, 127], 2080768),
            (vec![128, 128, 128, 127], 266338304),
            (vec![255, 255, 255, 255, 15], u32::MAX),
        ],
        encode_varint_u32,
        decode_varint_u32,
        varint_u32_bytes_len
    );
}
