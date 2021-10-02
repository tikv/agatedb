use super::iterator::ITERATOR_REVERSED;
use super::{AgateIterator, TableIterator};
use crate::table::table_accessor::TableAccessorIterator;
use crate::value::Value;

use bytes::Bytes;

/// ConcatIterator iterates on SSTs with no overlap keys.
pub struct ConcatIterator {
    cur_iter: Option<TableIterator>,
    accessor: Box<dyn TableAccessorIterator>,
    opt: usize,
}

impl ConcatIterator {
    /// Create `ConcatIterator` from a list of tables. Tables must have been sorted
    /// and have no overlap keys.
    pub fn from_tables(accessor: Box<dyn TableAccessorIterator>, opt: usize) -> Self {
        ConcatIterator {
            cur_iter: None,
            accessor,
            opt,
        }
    }

    fn iter_mut(&mut self) -> &mut TableIterator {
        self.cur_iter.as_mut().unwrap()
    }

    fn iter_ref(&self) -> &TableIterator {
        self.cur_iter.as_ref().unwrap()
    }
}

impl AgateIterator for ConcatIterator {
    fn next(&mut self) {
        let cur_iter = self.iter_mut();
        cur_iter.next();
        if cur_iter.valid() {
            return;
        }
        drop(cur_iter);
        self.cur_iter = None;
        loop {
            if self.opt & ITERATOR_REVERSED == 0 {
                self.accessor.next();
            } else {
                self.accessor.prev();
            }
            if !self.accessor.valid() {
                return;
            }
            self.cur_iter = self.accessor.table().map(|t| t.new_iterator(self.opt));
            self.iter_mut().rewind();
            if self.iter_ref().valid() {
                return;
            }
            self.cur_iter = None;
        }
    }

    fn rewind(&mut self) {
        if self.opt & ITERATOR_REVERSED == 0 {
            self.accessor.seek_first();
        } else {
            self.accessor.seek_last();
        }
        self.cur_iter = self.accessor.table().map(|t| t.new_iterator(self.opt));
        if self.cur_iter.is_none() {
            return;
        }
        self.iter_mut().rewind();
    }

    fn seek(&mut self, key: &Bytes) {
        self.cur_iter = None;
        if self.opt & ITERATOR_REVERSED == 0 {
            self.accessor.seek(key);
            if !self.accessor.valid() {
                return;
            }
            self.cur_iter = self.accessor.table().map(|t| t.new_iterator(self.opt));
        } else {
            self.accessor.seek_for_previous(key);
            if !self.accessor.valid() {
                return;
            }
            self.cur_iter = self.accessor.table().map(|t| t.new_iterator(self.opt));
        }
        self.iter_mut().seek(key);
    }

    fn key(&self) -> &[u8] {
        self.iter_ref().key()
    }

    fn value(&self) -> Value {
        self.iter_ref().value()
    }

    fn valid(&self) -> bool {
        if self.cur_iter.is_some() {
            self.iter_ref().valid()
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::table::{Table, TableAccessor, VecTableAccessor};
    use crate::{
        format::{key_with_ts, user_key},
        table::tests::{build_table_data, get_test_table_options},
    };
    use bytes::Bytes;
    use bytes::BytesMut;
    use rand::prelude::*;

    fn build_test_tables() -> (Vec<Table>, usize) {
        let mut tables = vec![];
        let mut cnt = 0;
        let mut rng = thread_rng();

        for i in 0..20 {
            let size = rng.gen_range(100, 200);
            let data: Vec<(Bytes, Bytes)> = (cnt..cnt + size)
                .map(|x| {
                    (
                        Bytes::from(format!("{:012x}", x).to_string()),
                        Bytes::from(format!("{:012x}", x).to_string()),
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
        let accessor = VecTableAccessor::create(tables);
        let mut iter =
            ConcatIterator::from_tables(Box::new(VecTableAccessor::new_iterator(accessor)), 0);

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
