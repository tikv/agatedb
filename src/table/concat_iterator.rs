use bytes::Bytes;

use super::{iterator::ITERATOR_REVERSED, AgateIterator, Table, TableIterator};
use crate::{
    util::{KeyComparator, COMPARATOR},
    value::Value,
};

/// ConcatIterator iterates on SSTs with no overlap keys.
pub struct ConcatIterator {
    cur: Option<usize>,
    iters: Vec<Option<TableIterator>>,
    tables: Vec<Table>,
    opt: usize,
}

impl ConcatIterator {
    /// Create `ConcatIterator` from a list of tables. Tables must have been sorted
    /// and have no overlap keys.
    pub fn from_tables(tables: Vec<Table>, opt: usize) -> Self {
        let iters = tables.iter().map(|_| None).collect();

        ConcatIterator {
            cur: None,
            iters,
            tables,
            opt,
        }
    }

    fn set_idx(&mut self, idx: usize) {
        if idx >= self.iters.len() {
            self.cur = None;
            return;
        }
        if self.iters[idx].is_none() {
            self.iters[idx] = Some(self.tables[idx].new_iterator(self.opt));
        }
        self.cur = Some(idx);
    }

    fn iter_mut(&mut self) -> &mut TableIterator {
        self.iters[self.cur.unwrap()].as_mut().unwrap()
    }

    fn iter_ref(&self) -> &TableIterator {
        self.iters[self.cur.unwrap()].as_ref().unwrap()
    }
}

impl AgateIterator for ConcatIterator {
    fn next(&mut self) {
        let cur = self.cur.unwrap();
        let cur_iter = self.iter_mut();
        cur_iter.next();
        if cur_iter.valid() {
            return;
        }
        loop {
            if self.opt & ITERATOR_REVERSED == 0 {
                self.set_idx(cur + 1);
            } else if cur == 0 {
                self.cur = None;
            } else {
                self.set_idx(cur - 1);
            }

            if self.cur.is_some() {
                self.iter_mut().rewind();
                if self.iter_ref().valid() {
                    return;
                }
            } else {
                return;
            }
        }
    }

    fn rewind(&mut self) {
        if self.iters.is_empty() {
            return;
        }
        if self.opt & ITERATOR_REVERSED == 0 {
            self.set_idx(0);
        } else {
            self.set_idx(self.iters.len() - 1);
        }

        self.iter_mut().rewind();
    }

    fn seek(&mut self, key: &Bytes) {
        use std::cmp::Ordering::*;
        let idx;
        if self.opt & ITERATOR_REVERSED == 0 {
            idx = crate::util::search(self.tables.len(), |idx| {
                COMPARATOR.compare_key(self.tables[idx].biggest(), key) != Less
            });
            if idx >= self.tables.len() {
                self.cur = None;
                return;
            }
        } else {
            let n = self.tables.len();
            let ridx = crate::util::search(self.tables.len(), |idx| {
                COMPARATOR.compare_key(self.tables[n - 1 - idx].smallest(), key) != Greater
            });
            if ridx >= self.tables.len() {
                self.cur = None;
                return;
            }
            idx = n - 1 - ridx;
        }

        self.set_idx(idx);
        self.iter_mut().seek(key);
    }

    fn key(&self) -> &[u8] {
        self.iter_ref().key()
    }

    fn value(&self) -> Value {
        self.iter_ref().value()
    }

    fn valid(&self) -> bool {
        if self.cur.is_some() {
            self.iter_ref().valid()
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use rand::prelude::*;

    use super::*;
    use crate::{
        format::{key_with_ts, user_key},
        table::{
            tests::{build_table_data, get_test_table_options},
            Table,
        },
    };

    fn build_test_tables() -> (Vec<Table>, usize) {
        let mut tables = vec![];
        let mut cnt = 0;
        let mut rng = thread_rng();

        for i in 0..20 {
            let size = rng.gen_range(100..200);
            let data: Vec<(Bytes, Bytes)> = (cnt..cnt + size)
                .map(|x| {
                    (
                        Bytes::from(format!("{:012x}", x)),
                        Bytes::from(format!("{:012x}", x)),
                    )
                })
                .collect();
            let data = build_table_data(data, get_test_table_options());
            cnt += size;
            tables.push(Table::open_in_memory(data, i, get_test_table_options()).unwrap());
        }

        (tables, cnt)
    }

    #[test]
    fn test_concat_iterator() {
        let (tables, cnt) = build_test_tables();
        let mut iter = ConcatIterator::from_tables(tables, 0);

        iter.rewind();

        // test iterate
        for i in 0..cnt {
            assert!(iter.valid());
            assert_eq!(user_key(iter.key()), format!("{:012x}", i).as_bytes());
            iter.next();
        }
        assert!(!iter.valid());

        // test seek
        for i in 10..cnt - 10 {
            iter.seek(&key_with_ts(
                BytesMut::from(format!("{:012x}", i).as_bytes()),
                0,
            ));
            for j in 0..10 {
                assert_eq!(user_key(iter.key()), format!("{:012x}", i + j).as_bytes());
                iter.next();
            }
        }
    }
}
