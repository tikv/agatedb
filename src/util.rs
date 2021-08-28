pub use skiplist::FixedLengthSuffixComparator as Comparator;
pub use skiplist::{FixedLengthSuffixComparator, KeyComparator};

use crate::format::user_key;
use crate::Result;

use std::fs::File;
use std::path::Path;
use std::{cmp, ptr};

pub static COMPARATOR: FixedLengthSuffixComparator = make_comparator();

pub const fn make_comparator() -> FixedLengthSuffixComparator {
    FixedLengthSuffixComparator::new(8)
}

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
        if i + 4 <= end && u32(base.as_ptr().add(i)) == u32(target.as_ptr().add(i)) {
            i += 4;
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

pub fn sync_dir(path: &impl AsRef<Path>) -> Result<()> {
    File::open(path.as_ref())?.sync_all()?;
    Ok(())
}

pub fn same_key(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    return user_key(a) == user_key(b);
}
