use bytes::{Bytes, BytesMut};
use enum_dispatch::enum_dispatch;

use super::concat_iterator::ConcatIterator;
use super::TableIterator;
use crate::iterator_trait::AgateIterator;
use crate::util::{KeyComparator, COMPARATOR};
use crate::Value;

/// `Iterators` includes all iterator types for AgateDB.
/// By packing them into an enum, we could reduce the
/// overhead of dynamic dispatch.
#[enum_dispatch(AgateIterator)]
pub enum Iterators {
    MergeIterator(MergeIterator),
    ConcatIterator(ConcatIterator),
    TableIterator(TableIterator),
    #[cfg(test)]
    VecIterator(tests::VecIterator),
}

/// `MergeIterator` merges two `Iterators` into one by sequentially emitting
/// elements from two child iterators.
///
/// TODO: save iterators in a slice instead of as a binary tree
pub struct MergeIterator {
    left: IteratorNode,
    right: IteratorNode,
    is_left_small: bool,
    reverse: bool,
    current_key: BytesMut,
}

/// `IteratorNode` buffers the iterator key in its own struct, to
/// reduce the overhead of fetching key during merging iterators.
struct IteratorNode {
    valid: bool,
    key: BytesMut,
    iter: Box<Iterators>,
}

impl IteratorNode {
    fn new(iter: Box<Iterators>) -> Self {
        Self {
            valid: false,
            key: BytesMut::new(),
            iter,
        }
    }

    fn set_key(&mut self) {
        self.valid = self.iter.valid();
        if self.valid {
            self.key.clear();
            self.key.extend_from_slice(self.iter.key());
        }
    }

    fn next(&mut self) {
        self.iter.next();
        self.set_key();
    }

    fn rewind(&mut self) {
        self.iter.rewind();
        self.set_key();
    }

    fn seek(&mut self, key: &Bytes) {
        self.iter.seek(key);
        self.set_key();
    }
}

impl MergeIterator {
    #[inline]
    fn smaller_mut(&mut self) -> &mut IteratorNode {
        if self.is_left_small {
            &mut self.left
        } else {
            &mut self.right
        }
    }

    #[inline]
    fn bigger_mut(&mut self) -> &mut IteratorNode {
        if !self.is_left_small {
            &mut self.left
        } else {
            &mut self.right
        }
    }

    #[inline]
    fn smaller(&self) -> &IteratorNode {
        if self.is_left_small {
            &self.left
        } else {
            &self.right
        }
    }

    #[inline]
    fn bigger(&self) -> &IteratorNode {
        if !self.is_left_small {
            &self.left
        } else {
            &self.right
        }
    }

    fn swap_small(&mut self) {
        self.is_left_small = !self.is_left_small;
    }

    fn fix(&mut self) {
        use std::cmp::Ordering::*;

        if !self.bigger().valid {
            return;
        }

        if !self.smaller().valid {
            self.swap_small();
            return;
        }

        match COMPARATOR.compare_key(&self.smaller().key, &self.bigger().key) {
            Equal => {
                self.right.next();
                if !self.is_left_small {
                    self.swap_small();
                }
            }
            Less => {
                if self.reverse {
                    self.swap_small();
                }
            }
            Greater => {
                if !self.reverse {
                    self.swap_small();
                }
            }
        }
    }

    fn set_current(&mut self) {
        self.current_key.clear();
        if self.is_left_small {
            self.current_key.extend_from_slice(&self.left.key);
        } else {
            self.current_key.extend_from_slice(&self.right.key);
        }
    }

    /// Construct a single merge iterator from multiple iterators
    ///
    /// If the iterator emits elements in descending order, set `reverse` to true.
    pub fn from_iterators(mut iters: Vec<Box<Iterators>>, reverse: bool) -> Box<Iterators> {
        match iters.len() {
            0 => panic!("no element in iters"),
            1 => iters.pop().unwrap(),
            2 => {
                let right = iters.pop().unwrap();
                let left = iters.pop().unwrap();
                Box::new(Iterators::from(MergeIterator {
                    reverse,
                    left: IteratorNode::new(left),
                    right: IteratorNode::new(right),
                    is_left_small: true,
                    current_key: BytesMut::new(),
                }))
            }
            _ => {
                let mid = iters.len() / 2;
                let right = iters.split_off(mid);
                let left = iters;
                Box::new(Iterators::from(MergeIterator {
                    reverse,
                    left: IteratorNode::new(Self::from_iterators(left, reverse)),
                    right: IteratorNode::new(Self::from_iterators(right, reverse)),
                    is_left_small: true,
                    current_key: BytesMut::new(),
                }))
            }
        }
    }
}

impl AgateIterator for MergeIterator {
    fn next(&mut self) {
        while self.valid() {
            if self.smaller().key != self.current_key {
                break;
            }
            self.smaller_mut().next();
            self.fix();
        }
        self.set_current();
    }

    fn rewind(&mut self) {
        self.left.rewind();
        self.right.rewind();
        self.fix();
        self.set_current();
    }

    fn seek(&mut self, key: &Bytes) {
        self.left.seek(key);
        self.right.seek(key);
        self.fix();
        self.set_current();
    }

    fn key(&self) -> &[u8] {
        &self.smaller().key
    }

    fn value(&self) -> Value {
        self.smaller().iter.value()
    }

    fn valid(&self) -> bool {
        self.smaller().valid
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{key_with_ts, user_key};

    pub struct VecIterator {
        vec: Vec<Bytes>,
        pos: usize,
        reversed: bool,
    }

    impl VecIterator {
        pub fn new(vec: Vec<Bytes>, reversed: bool) -> Self {
            VecIterator {
                vec,
                pos: 0,
                reversed,
            }
        }
    }

    impl AgateIterator for VecIterator {
        fn next(&mut self) {
            self.pos += 1;
        }

        fn rewind(&mut self) {
            self.pos = 0;
        }

        fn seek(&mut self, key: &Bytes) {
            let found_entry_idx = crate::util::search(self.vec.len(), |idx| {
                use std::cmp::Ordering::*;
                if self.reversed {
                    COMPARATOR.compare_key(&self.vec[idx], &key) != Greater
                } else {
                    COMPARATOR.compare_key(&self.vec[idx], &key) != Less
                }
            });
            self.pos = found_entry_idx;
        }

        fn key(&self) -> &[u8] {
            &self.vec[self.pos]
        }

        fn value(&self) -> Value {
            Value::new(self.vec[self.pos].clone())
        }

        fn valid(&self) -> bool {
            self.pos < self.vec.len()
        }
    }

    fn gen_vec_data(n: usize, predicate: impl Fn(usize) -> bool) -> Vec<Bytes> {
        (0..n)
            .filter(|x| predicate(*x))
            .map(|i| key_with_ts(format!("{:012x}", i).as_str(), 0))
            .collect()
    }

    /// `assert_bytes_eq` will first convert `left` and `right` into `bytes::Bytes`, and call `assert_eq!`.
    /// When asserting eq, `Bytes` is more readable than `&[u8]` in output, as it will show characters as-is
    /// and only escape control characters. For example, it will show `aaa` instead of `[97, 97, 97]`.
    macro_rules! assert_bytes_eq {
        ($left:expr, $right:expr) => {
            assert_eq!(
                Bytes::copy_from_slice($left),
                Bytes::copy_from_slice($right)
            )
        };
    }

    fn check_sequence_both(mut iter: Box<Iterators>, n: usize, reversed: bool) {
        // test sequentially iterate
        let mut cnt = 0;
        iter.rewind();
        while iter.valid() {
            let check_cnt = if reversed { n - 1 - cnt } else { cnt };
            assert_bytes_eq!(
                user_key(iter.key()),
                format!("{:012x}", check_cnt).as_bytes()
            );
            cnt += 1;
            iter.next();
        }
        assert_eq!(cnt, n);

        iter.rewind();

        // test seek
        for i in 10..n - 10 {
            iter.seek(&key_with_ts(
                BytesMut::from(format!("{:012x}", i).as_bytes()),
                0,
            ));
            for j in 0..10 {
                assert!(iter.valid());
                let expected_key = if reversed {
                    format!("{:012x}", i - j).to_string()
                } else {
                    format!("{:012x}", i + j).to_string()
                };
                assert_bytes_eq!(user_key(iter.key()), expected_key.as_bytes());
                iter.next();
            }
        }
    }

    fn check_sequence(iter: Box<Iterators>, n: usize) {
        check_sequence_both(iter, n, false);
    }

    fn check_reverse_sequence(iter: Box<Iterators>, n: usize) {
        check_sequence_both(iter, n, true);
    }

    #[test]
    fn test_vec_iter_seek() {
        let data = gen_vec_data(0xfff, |_| true);
        let mut iter = VecIterator::new(data, false);
        for i in 0..0xfff {
            iter.seek(&key_with_ts(
                BytesMut::from(format!("{:012x}", i).as_bytes()),
                0,
            ));
            assert_bytes_eq!(user_key(iter.key()), format!("{:012x}", i).as_bytes());
        }
        let mut data = gen_vec_data(0xfff, |_| true);
        data.reverse();
        let mut iter = VecIterator::new(data, true);
        for i in 0..0xfff {
            iter.seek(&key_with_ts(
                BytesMut::from(format!("{:012x}", i).as_bytes()),
                0,
            ));
            assert_bytes_eq!(user_key(iter.key()), format!("{:012x}", i).as_bytes());
        }
    }

    #[test]
    fn test_merge_2iters_iterate() {
        let a = gen_vec_data(0xfff, |x| x % 5 == 0);
        let b = gen_vec_data(0xfff, |x| x % 5 != 0);
        let mut rev_a = a.clone();
        rev_a.reverse();
        let mut rev_b = b.clone();
        rev_b.reverse();

        let iter_a = Box::new(Iterators::from(VecIterator::new(a, false)));
        let iter_b = Box::new(Iterators::from(VecIterator::new(b, false)));
        let merge_iter = MergeIterator::from_iterators(vec![iter_a, iter_b], false);

        check_sequence(merge_iter, 0xfff);

        let iter_a = Box::new(Iterators::from(VecIterator::new(rev_a, true)));
        let iter_b = Box::new(Iterators::from(VecIterator::new(rev_b, true)));
        let merge_iter = MergeIterator::from_iterators(vec![iter_a, iter_b], true);
        check_reverse_sequence(merge_iter, 0xfff);
    }

    #[test]
    fn test_merge_5iters_iterate() {
        // randomly determine sequence of 5 iterators
        let vec_map = vec![2, 4, 1, 3, 0];
        let vec_map_size = vec_map.len();
        let vecs: Vec<Vec<Bytes>> = vec_map
            .into_iter()
            .map(|i| gen_vec_data(0xfff, |x| x % vec_map_size == i))
            .collect();
        let rev_vecs: Vec<Vec<Bytes>> = vecs
            .iter()
            .map(|x| {
                let mut y = x.clone();
                y.reverse();
                y
            })
            .collect();

        let iters: Vec<Box<Iterators>> = vecs
            .into_iter()
            .map(|vec| Box::new(Iterators::from(VecIterator::new(vec, false))))
            .collect();

        check_sequence(MergeIterator::from_iterators(iters, false), 0xfff);

        let rev_iters: Vec<Box<Iterators>> = rev_vecs
            .into_iter()
            .map(|vec| Box::new(Iterators::from(VecIterator::new(vec, true))))
            .collect();

        check_reverse_sequence(MergeIterator::from_iterators(rev_iters, true), 0xfff);
    }
}
