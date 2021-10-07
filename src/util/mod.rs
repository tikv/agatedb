mod btree;
pub use btree::{BTree, BTreeIterator, BTreePageIterator, ComparableRecord, Page, PageIterator};

use bytes::Bytes;
pub use skiplist::FixedLengthSuffixComparator as Comparator;
pub use skiplist::{FixedLengthSuffixComparator, KeyComparator};

use std::fs::File;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{cmp, ptr};
use std::{collections::hash_map::DefaultHasher, sync::atomic::AtomicBool};

use crate::format::user_key;
use crate::Result;

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

/// simple rewrite of golang sort.Search. It will return the first one match `f`. If this
/// does not exsit, it will return the size of array.
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

pub fn has_any_prefixes(s: &[u8], list_of_prefixes: &[Bytes]) -> bool {
    list_of_prefixes.iter().any(|y| s.starts_with(y))
}

pub fn same_key(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    return user_key(a) == user_key(b);
}

pub fn default_hash(h: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    h.hash(&mut hasher);
    hasher.finish()
}

pub fn unix_time() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_millis() as u64
}

pub fn sync_dir(path: &impl AsRef<Path>) -> Result<()> {
    File::open(path.as_ref())?.sync_all()?;
    Ok(())
}

static FAILED: AtomicBool = AtomicBool::new(false);

pub fn no_fail<T>(result: Result<T>, id: &str) {
    if let Err(err) = result {
        eprintln!("WARN: {}, {:?}", id, err);
        FAILED.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

pub fn panic_if_fail() {
    if FAILED.load(std::sync::atomic::Ordering::SeqCst) {
        panic!("failed");
    }
}

// TODO: use enum for this struct
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct KeyRange {
    pub left: Bytes,
    pub right: Bytes,
    pub inf: bool,
}

impl KeyRange {
    pub fn is_empty(&self) -> bool {
        if self.inf {
            false
        } else {
            self.left.is_empty() && self.right.is_empty()
        }
    }

    pub fn inf() -> Self {
        Self {
            left: Bytes::new(),
            right: Bytes::new(),
            inf: true,
        }
    }

    pub fn new(left: Bytes, right: Bytes) -> Self {
        Self {
            left,
            right,
            inf: false,
        }
    }

    pub fn extend(&mut self, range: &Self) {
        if self.is_empty() {
            *self = range.clone();
            return;
        }
        if range.left.len() == 0
            || COMPARATOR.compare_key(&range.left, &self.left) == std::cmp::Ordering::Less
        {
            self.left = range.left.clone();
        }
        if range.right.len() == 0
            || COMPARATOR.compare_key(&range.left, &self.left) == std::cmp::Ordering::Greater
        {
            self.right = range.right.clone();
        }
        if range.inf {
            self.inf = true;
            self.left = Bytes::new();
            self.right = Bytes::new();
        }
    }

    pub fn overlaps_with(&self, dst: &Self) -> bool {
        if self.is_empty() {
            return true;
        }
        if self.inf || dst.inf {
            return true;
        }
        if COMPARATOR.compare_key(&self.left, &dst.right) == std::cmp::Ordering::Greater {
            return false;
        }
        if COMPARATOR.compare_key(&self.right, &dst.left) == std::cmp::Ordering::Less {
            return false;
        }
        true
    }
}

impl Default for KeyRange {
    fn default() -> Self {
        Self {
            left: Bytes::new(),
            right: Bytes::new(),
            inf: false,
        }
    }
}
