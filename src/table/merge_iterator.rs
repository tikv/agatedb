use bytes::{Bytes, BytesMut};
use std::sync::Arc;

use super::iterator::Iterator;
use super::TableInner;
use crate::structs::AgateIterator;
use crate::util::{KeyComparator, COMPARATOR};
use crate::Value;

type TableIterator = Iterator<Arc<TableInner>>;

pub struct MergeIterator {
    left: IteratorNode,
    right: IteratorNode,
    is_left_small: bool,
    reverse: bool,
    current_key: BytesMut,
}

enum Iterators {
    Merge(Box<MergeIterator>),
    // TODO: Concat(ConcatIterator),
    Table(Box<TableIterator>),
    Dynamic(Box<dyn AgateIterator>),
}

macro_rules! impl_iterators {
    ($self: ident, $func: ident) => {
        match $self {
            Iterators::Merge(x) => x.$func(),
            Iterators::Table(x) => x.$func(),
            Iterators::Dynamic(x) => x.$func(),
        }
    };
}

impl Iterators {
    pub fn valid(&self) -> bool {
        impl_iterators!(self, valid)
    }

    pub fn key(&self) -> &[u8] {
        impl_iterators!(self, key)
    }

    pub fn next(&mut self) {
        impl_iterators!(self, next)
    }

    pub fn rewind(&mut self) {
        impl_iterators!(self, next)
    }

    pub fn seek(&mut self, key: &Bytes) {
        match self {
            Iterators::Merge(x) => x.seek(key),
            Iterators::Table(x) => x.seek(key),
            Iterators::Dynamic(x) => x.seek(key),
        }
    }

    pub fn value(&self) -> Value {
        impl_iterators!(self, value)
    }
}

struct IteratorNode {
    valid: bool,
    key: BytesMut,
    iter: Iterators,
}

impl IteratorNode {
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
