use bytes::Bytes;

use super::{iterator::ITERATOR_REVERSED, AgateIterator, Table, TableIterator};
use crate::{
    util::{KeyComparator, COMPARATOR},
    value::Value,
};

/// ConcatIterator iterates on SSTs with no overlap keys.
pub struct ConcatIterator {
    cur: usize,
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
            cur: 0,
            iters,
            tables,
            opt,
        }
    }

    fn set_idx(&mut self, idx: usize) {
        assert!(idx == std::usize::MAX || idx <= self.tables.len());
        self.cur = idx;

        if idx == std::usize::MAX || idx == self.iters.len() {
            return;
        }

        if self.iters[idx].is_none() {
            self.iters[idx] = Some(self.tables[idx].new_iterator(self.opt));
        }
    }

    fn iter_mut(&mut self) -> &mut TableIterator {
        self.iters[self.cur].as_mut().unwrap()
    }

    fn iter_ref(&self) -> &TableIterator {
        self.iters[self.cur].as_ref().unwrap()
    }
}

impl AgateIterator for ConcatIterator {
    fn next(&mut self) {
        if self.cur != std::usize::MAX && self.cur < self.iters.len() {
            let cur_iter = self.iter_mut();
            cur_iter.next();
            if cur_iter.valid() {
                return;
            }
        }

        #[allow(clippy::collapsible_else_if)]
        loop {
            if self.opt & ITERATOR_REVERSED == 0 {
                if self.cur == std::usize::MAX {
                    self.rewind();
                    return;
                } else if self.cur < self.iters.len() {
                    self.set_idx(self.cur + 1);

                    if self.cur == self.iters.len() {
                        return;
                    }
                } else {
                    return;
                }
            } else {
                if self.cur == std::usize::MAX {
                    return;
                } else if self.cur > 0 {
                    self.set_idx(self.cur - 1);
                } else {
                    self.set_idx(std::usize::MAX);
                    return;
                }
            }

            self.iter_mut().rewind();
            if self.iter_ref().valid() {
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
            self.set_idx(idx);
            if idx >= self.tables.len() {
                return;
            }
        } else {
            let n = self.tables.len();
            let ridx = crate::util::search(self.tables.len(), |idx| {
                COMPARATOR.compare_key(self.tables[n - 1 - idx].smallest(), key) != Greater
            });
            self.set_idx(ridx);
            if ridx >= self.tables.len() {
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
        if self.cur != std::usize::MAX && self.cur < self.iters.len() {
            self.iter_ref().valid()
        } else {
            false
        }
    }

    fn prev(&mut self) {
        if self.cur != std::usize::MAX && self.cur < self.iters.len() {
            let cur_iter = self.iter_mut();
            cur_iter.prev();
            if cur_iter.valid() {
                return;
            }
        }

        #[allow(clippy::collapsible_else_if)]
        loop {
            if self.opt & ITERATOR_REVERSED == 0 {
                if self.cur == std::usize::MAX {
                    return;
                } else if self.cur > 0 {
                    self.set_idx(self.cur - 1);
                } else {
                    self.set_idx(std::usize::MAX);
                    return;
                }
            } else {
                if self.cur == std::usize::MAX {
                    self.to_last();
                    return;
                } else if self.cur < self.iters.len() {
                    self.set_idx(self.cur + 1);

                    if self.cur == self.iters.len() {
                        return;
                    }
                } else {
                    return;
                }
            }

            self.iter_mut().to_last();
            if self.iter_ref().valid() {
                return;
            }
        }
    }

    fn to_last(&mut self) {
        if self.iters.is_empty() {
            return;
        }

        if self.opt & ITERATOR_REVERSED == 0 {
            self.set_idx(self.iters.len() - 1);
        } else {
            self.set_idx(0);
        }

        self.iter_mut().to_last();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rand::prelude::*;

    use super::*;
    use crate::{
        table::{
            tests::{build_table_data, get_test_table_options},
            Table,
        },
        util::test::{check_iterator_normal_operation, check_iterator_out_of_bound},
    };

    fn build_test_tables() -> (Vec<Table>, usize) {
        let mut tables = vec![];
        let mut cnt = 0;
        let mut rng = thread_rng();

        for i in 0..20 {
            let size = rng.gen_range(100, 200);
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

        let iter = ConcatIterator::from_tables(tables.clone(), 0);
        check_iterator_normal_operation(iter, cnt, false);

        let iter = ConcatIterator::from_tables(tables, ITERATOR_REVERSED);
        check_iterator_normal_operation(iter, cnt, true);
    }

    #[test]
    fn test_concat_iterator_out_of_bound() {
        let (tables, cnt) = build_test_tables();

        let iter = ConcatIterator::from_tables(tables.clone(), 0);
        check_iterator_out_of_bound(iter, cnt, false);

        let iter = ConcatIterator::from_tables(tables, ITERATOR_REVERSED);
        check_iterator_out_of_bound(iter, cnt, true);
    }
}
