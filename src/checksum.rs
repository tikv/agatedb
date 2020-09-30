use crate::{Error, Result};
use crc::crc32;
use proto::meta::{checksum::Algorithm as ChecksumAlgorithm, Checksum};

pub fn calculate_checksum(data: &[u8], algo: ChecksumAlgorithm) -> u64 {
    match algo {
        ChecksumAlgorithm::Crc32c => crc32::checksum_castagnoli(data) as u64,
        ChecksumAlgorithm::XxHash64 => xxhash::checksum(data),
    }
}

pub fn verify_checksum(data: &[u8], expected: &Checksum) -> Result<()> {
    let actual = calculate_checksum(data, ChecksumAlgorithm::from_i32(expected.algo).unwrap());
    if actual == expected.sum {
        return Ok(());
    }
    Err(Error::InvalidChecksum(format!(
        "checksum not correct, expected {:?}, got {}",
        expected, actual
    )))
}

mod xxhash {
    use std::{ptr, u64};

    const PRIME1: u64 = 11400714785074694791;
    const PRIME2: u64 = 14029467366897019727;
    const PRIME3: u64 = 1609587929392839161;
    const PRIME4: u64 = 9650029242287828579;
    const PRIME5: u64 = 2870177450012600261;

    #[inline]
    fn mul(lhs: u64, rhs: u64) -> u64 {
        lhs.overflowing_mul(rhs).0
    }

    #[inline]
    fn add(lhs: u64, rhs: u64) -> u64 {
        lhs.overflowing_add(rhs).0
    }

    #[inline]
    fn sub(lhs: u64, rhs: u64) -> u64 {
        lhs.overflowing_sub(rhs).0
    }

    #[inline]
    fn u64(bytes: *const u8) -> u64 {
        let u = unsafe { ptr::read_unaligned(bytes as *const u64) };
        u64::from_le(u)
    }

    #[inline]
    fn u32(bytes: *const u8) -> u32 {
        let u = unsafe { ptr::read_unaligned(bytes as *const u32) };
        u32::from_le(u)
    }

    #[inline]
    fn round(acc: u64, input: u64) -> u64 {
        mul(add(acc, mul(input, PRIME2)).rotate_left(31), PRIME1)
    }

    #[inline]
    fn merge_round(acc: u64, val: u64) -> u64 {
        add(mul(acc ^ round(0, val), PRIME1), PRIME4)
    }

    pub fn checksum(mut bytes: &[u8]) -> u64 {
        let mut h;
        let len = bytes.len();
        if bytes.len() >= 32 {
            let (mut v1, mut v2, mut v3, mut v4) = (add(PRIME1, PRIME2), PRIME2, 0, sub(0, PRIME1));
            loop {
                v1 = round(v1, u64(bytes.as_ptr()));
                v2 = round(v2, u64(unsafe { bytes.as_ptr().add(8) }));
                v3 = round(v3, u64(unsafe { bytes.as_ptr().add(16) }));
                v4 = round(v4, u64(unsafe { bytes.as_ptr().add(24) }));
                bytes = unsafe { bytes.get_unchecked(32..) };
                if bytes.len() < 32 {
                    break;
                }
            }
            h = add(v1.rotate_left(1), v2.rotate_left(7));
            h = add(h, v3.rotate_left(12));
            h = add(h, v4.rotate_left(18));
            h = merge_round(h, v1);
            h = merge_round(h, v2);
            h = merge_round(h, v3);
            h = merge_round(h, v4)
        } else {
            h = PRIME5;
        }
        h += len as u64;
        let (mut i, end) = (0, bytes.len());
        if i + 8 <= end {
            i += 8;
            h ^= round(0, u64(bytes.as_ptr()));
            h = add(mul(h.rotate_left(27), PRIME1), PRIME4);

            if i + 8 <= end {
                i += 8;
                h ^= round(0, unsafe { u64(bytes.as_ptr().add(8)) });
                h = add(mul(h.rotate_left(27), PRIME1), PRIME4);

                if i + 8 <= end {
                    i += 8;
                    h ^= round(0, unsafe { u64(bytes.as_ptr().add(16)) });
                    h = add(mul(h.rotate_left(27), PRIME1), PRIME4);

                    if i + 8 <= end {
                        i += 8;
                        h ^= round(0, unsafe { u64(bytes.as_ptr().add(24)) });
                        h = add(mul(h.rotate_left(27), PRIME1), PRIME4);
                    }
                }
            }
        }
        if i + 4 <= end {
            h ^= mul(unsafe { u32(bytes.as_ptr().add(i)) } as u64, PRIME1);
            h = add(mul(h.rotate_left(23), PRIME2), PRIME3);
            i += 4;
        }
        while i < end {
            h ^= mul(unsafe { *bytes.get_unchecked(i) as u64 }, PRIME5);
            h = mul(h.rotate_left(11), PRIME1);
            i += 1;
        }
        h ^= h >> 33;
        h = mul(h, PRIME2);
        h ^= h >> 29;
        h = mul(h, PRIME3);
        h ^= h >> 32;
        h
    }

    #[cfg(test)]
    #[test]
    fn test_checksum() {
        let cases = vec![
            ("empty", "", 0xef46db3751d8e999),
            ("a", "a", 0xd24ec4f1a98c6e5b),
            ("as", "as", 0x1c330fb2d66be179),
            ("asd", "asd", 0x631c37ce72a97393),
            ("asdf", "asdf", 0x415872f599cea71e),
            (
                "len=63",
                // Exactly 63 characters, which exercises all code paths.
                "Call me Ishmael. Some years ago--never mind how long precisely-",
                0x02a2e85470d6fd96,
            ),
        ];
        for (name, input, expect) in cases {
            assert_eq!(checksum(input.as_bytes()), expect, "{}", name);
        }
    }
}
