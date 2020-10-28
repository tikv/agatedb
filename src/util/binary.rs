// Copyright 2017 Jay Lee. Licensed under MIT.

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
pub fn decode_varint_s32(data: &[u8]) -> Result<(i32, u8)> {
    let (r, n) = decode_varint_u32(data)?;
    Ok((unzig_zag_32(r), n))
}

#[inline]
pub fn decode_varint_i32(data: &[u8]) -> Result<(i32, u8)> {
    let (r, n) = decode_varint_u32(data)?;
    Ok((r as i32, n))
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
pub fn decode_varint_s64(data: &[u8]) -> Result<(i64, u8)> {
    let (r, n) = decode_varint_u64(data)?;
    Ok((unzig_zag_64(r), n))
}

#[inline]
pub fn decode_varint_i64(data: &[u8]) -> Result<(i64, u8)> {
    let (r, n) = decode_varint_u64(data)?;
    Ok((r as i64, n))
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
pub fn encode_varint_s32(data: &mut BytesMut, n: i32) {
    let n = zig_zag_32(n);
    encode_varint_u32(data, n);
}

#[inline]
pub fn encode_varint_i32(data: &mut BytesMut, n: i32) {
    if n >= 0 {
        encode_varint_u32(data, n as u32)
    } else {
        encode_varint_i64(data, n as i64)
    }
}

#[inline]
pub fn encode_varint_s64(data: &mut BytesMut, n: i64) {
    let n = zig_zag_64(n);
    encode_varint_u64(data, n);
}

#[inline]
pub fn encode_varint_i64(data: &mut BytesMut, n: i64) {
    let n = n as u64;
    encode_varint_u64(data, n);
}

#[inline]
pub fn varint_u32_bytes_len(mut n: u32) -> u32 {
    n |= 0x01;
    ((31 - n.leading_zeros()) * 9 + 73) / 64
}

#[inline]
pub fn varint_i32_bytes_len(i: i32) -> u32 {
    varint_i64_bytes_len(i as i64)
}

#[inline]
pub fn varint_s32_bytes_len(n: i32) -> u32 {
    varint_u32_bytes_len(zig_zag_32(n))
}

#[inline]
pub fn varint_u64_bytes_len(mut n: u64) -> u32 {
    n |= 0x01;
    ((63 - n.leading_zeros()) * 9 + 73) / 64
}

#[inline]
pub fn varint_i64_bytes_len(i: i64) -> u32 {
    varint_u64_bytes_len(i as u64)
}

#[inline]
pub fn varint_s64_bytes_len(n: i64) -> u32 {
    varint_u64_bytes_len(zig_zag_64(n))
}

#[inline]
pub fn fixed_i64_bytes_len(_: i64) -> u32 {
    8
}

#[inline]
pub fn fixed_s64_bytes_len(_: i64) -> u32 {
    8
}

#[inline]
pub fn fixed_i32_bytes_len(_: i32) -> u32 {
    4
}

#[inline]
pub fn fixed_s32_bytes_len(_: i32) -> u32 {
    4
}

#[inline]
pub fn f32_bytes_len(_: f32) -> u32 {
    4
}

#[inline]
pub fn f64_bytes_len(_: f64) -> u32 {
    8
}

#[inline]
pub fn zig_zag_64(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

#[inline]
pub fn unzig_zag_64(n: u64) -> i64 {
    ((n >> 1) ^ (!(n & 1)).wrapping_add(1)) as i64
}

#[inline]
pub fn zig_zag_32(n: i32) -> u32 {
    ((n << 1) ^ (n >> 31)) as u32
}

#[inline]
pub fn unzig_zag_32(n: u32) -> i32 {
    ((n >> 1) ^ (!(n & 1)).wrapping_add(1)) as i32
}

#[inline]
pub fn encode_tag(field_number: u32, wired_id: u32) -> u32 {
    (field_number << 3) | wired_id
}

macro_rules! encode_fixed {
    ($t:ty, $ti:ident, $len:expr, $enc_func:ident, $dec_func:ident) => {
        #[inline]
        pub fn $enc_func(data: &mut BytesMut, n: $t) {
            let n = n.to_le();
            let b: [u8; $len] = unsafe { mem::transmute(n) };
            data.extend_from_slice(&b)
        }

        #[inline]
        pub fn $dec_func(data: &[u8]) -> Result<$t> {
            let mut b: $t = 0;
            if data.len() >= $len {
                unsafe {
                    let buf = &mut b as *mut $t as *mut u8;
                    let data = data.as_ptr();
                    ptr::copy_nonoverlapping(data, buf, $len);
                }
                Ok($ti::from_le(b))
            } else {
                Err(Error::VarDecode("Corrupted"))
            }
        }
    };
}

encode_fixed!(i32, i32, 4, encode_fixed_i32, decode_fixed_i32);
encode_fixed!(i64, i64, 8, encode_fixed_i64, decode_fixed_i64);

#[inline]
pub fn encode_fixed_f32(data: &mut BytesMut, n: f32) {
    let n = n.to_bits();
    encode_fixed_i32(data, n as i32)
}

#[inline]
pub fn decode_fixed_f32(data: &[u8]) -> Result<f32> {
    decode_fixed_i32(data).map(|n| f32::from_bits(n as u32))
}

#[inline]
pub fn encode_fixed_f64(data: &mut BytesMut, n: f64) {
    let n = n.to_bits();
    encode_fixed_i64(data, n as i64)
}

#[inline]
pub fn decode_fixed_f64(data: &[u8]) -> Result<f64> {
    decode_fixed_i64(data).map(|n| f64::from_bits(n as u64))
}

#[inline]
pub fn encode_fixed_s32(data: &mut BytesMut, n: i32) {
    encode_fixed_i32(data, zig_zag_32(n) as i32)
}

#[inline]
pub fn decode_fixed_s32(data: &[u8]) -> Result<i32> {
    decode_fixed_i32(data).map(|i| unzig_zag_32(i as u32))
}

#[inline]
pub fn encode_fixed_s64(data: &mut BytesMut, n: i64) {
    encode_fixed_i64(data, zig_zag_64(n) as i64)
}

#[inline]
pub fn decode_fixed_s64(data: &[u8]) -> Result<i64> {
    decode_fixed_i64(data).map(|i| unzig_zag_64(i as u64))
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
        test_varint_i64,
        vec![
            (vec![1], 1),
            (vec![255, 255, 255, 255, 255, 255, 255, 255, 255, 1], -1),
            (vec![2], 2),
            (vec![254, 255, 255, 255, 255, 255, 255, 255, 255, 1], -2),
            (vec![255, 255, 255, 255, 7], 2147483647),
            (
                vec![128, 128, 128, 128, 248, 255, 255, 255, 255, 1],
                -2147483648
            ),
            (vec![0], 0),
            (vec![128, 1], 128),
            (vec![150, 1], 150),
        ],
        encode_varint_i64,
        decode_varint_i64,
        varint_i64_bytes_len
    );

    make_test!(
        test_varint_s64,
        vec![
            (vec![2], 1),
            (vec![1], -1),
            (vec![4], 2),
            (vec![3], -2),
            (vec![0], 0),
            (vec![128, 2], 128),
            (vec![255, 1], -128),
            (vec![172, 2], 150),
            (vec![254, 255, 255, 255, 15], 2147483647),
            (vec![255, 255, 255, 255, 15], -2147483648),
            (
                vec![254, 255, 255, 255, 255, 255, 255, 255, 255, 1],
                9223372036854775807
            ),
            (
                vec![255, 255, 255, 255, 255, 255, 255, 255, 255, 1],
                -9223372036854775808
            ),
        ],
        encode_varint_s64,
        decode_varint_s64,
        varint_s64_bytes_len
    );

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

    make_test!(
        test_varint_i32,
        vec![
            (vec![1], 1),
            (vec![255, 255, 255, 255, 255, 255, 255, 255, 255, 1], -1),
            (vec![254, 255, 255, 255, 255, 255, 255, 255, 255, 1], -2),
            (vec![0], 0),
            (vec![128, 1], 128),
            (vec![150, 1], 150),
            (vec![255, 255, 255, 255, 7], 2147483647),
            (
                vec![128, 128, 128, 128, 248, 255, 255, 255, 255, 1],
                -2147483648
            ),
        ],
        encode_varint_i32,
        decode_varint_i32,
        varint_i32_bytes_len
    );

    make_test!(
        test_varint_s32,
        vec![
            (vec![2], 1),
            (vec![1], -1),
            (vec![4], 2),
            (vec![3], -2),
            (vec![0], 0),
            (vec![128, 2], 128),
            (vec![255, 1], -128),
            (vec![172, 2], 150),
            (vec![254, 255, 255, 255, 15], 2147483647),
            (vec![255, 255, 255, 255, 15], -2147483648),
        ],
        encode_varint_s32,
        decode_varint_s32,
        varint_s32_bytes_len
    );

    make_test!(
        test_fixed_i32,
        vec![
            (vec![1, 0, 0, 0], 1),
            (vec![255, 255, 255, 255], -1),
            (vec![2, 0, 0, 0], 2),
            (vec![254, 255, 255, 255], -2),
            (vec![0, 0, 0, 0], 0),
            (vec![128, 0, 0, 0], 128),
            (vec![128, 255, 255, 255], -128),
            (vec![150, 0, 0, 0], 150),
            (vec![255, 255, 255, 127], 2147483647),
            (vec![0, 0, 0, 128], -2147483648),
        ],
        encode_fixed_i32,
        decode_fixed_i32,
        fixed_i32_bytes_len,
        0
    );

    make_test!(
        test_fixed_i64,
        vec![
            (vec![1, 0, 0, 0, 0, 0, 0, 0], 1),
            (vec![255, 255, 255, 255, 255, 255, 255, 255], -1),
            (vec![2, 0, 0, 0, 0, 0, 0, 0], 2),
            (vec![254, 255, 255, 255, 255, 255, 255, 255], -2),
            (vec![0, 0, 0, 0, 0, 0, 0, 0], 0),
            (vec![128, 0, 0, 0, 0, 0, 0, 0], 128),
            (vec![128, 255, 255, 255, 255, 255, 255, 255], -128),
            (vec![150, 0, 0, 0, 0, 0, 0, 0], 150),
            (vec![255, 255, 255, 127, 0, 0, 0, 0], 2147483647),
            (vec![0, 0, 0, 128, 255, 255, 255, 255], -2147483648),
            (vec![255, 255, 255, 255, 255, 255, 255, 127], i64::MAX),
            (vec![0, 0, 0, 0, 0, 0, 0, 128], i64::MIN),
        ],
        encode_fixed_i64,
        decode_fixed_i64,
        fixed_i64_bytes_len,
        0
    );

    make_test!(
        test_f32,
        vec![
            (vec![0x00, 0x00, 0x80, 0x3f], 1f32),
            (vec![0x00, 0x00, 0x80, 0xbf], -1f32),
            (vec![0x00, 0x00, 0x00, 0x40], 2f32),
            (vec![0x00, 0x00, 0x00, 0xc0], -2f32),
            (vec![0x00, 0x00, 0x00, 0x00], 0f32),
            (vec![0x00, 0x00, 0x00, 0x43], 128f32),
            (vec![0x00, 0x00, 0x00, 0xc3], -128f32),
            (vec![0xc3, 0xf5, 0x48, 0x40], 3.14),
            (vec![0x00, 0x00, 0x00, 0x4f], 2147483647f32),
        ],
        encode_fixed_f32,
        decode_fixed_f32,
        f32_bytes_len,
        0
    );

    make_test!(
        test_f64,
        vec![
            (vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f], 1f64),
            (vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0xbf], -1f64),
            (vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40], 2f64),
            (vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0], -2f64),
            (vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], 0f64),
            (vec![0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40], 3.14),
        ],
        encode_fixed_f64,
        decode_fixed_f64,
        f64_bytes_len,
        0
    );
}
