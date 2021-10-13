use bytes::Bytes;
use std::cmp;
use std::collections::HashSet;
use std::sync::Arc;

use super::table_accessor::{TableAccessor, TableAccessorIterator};
use super::Table;

use crate::util::ComparableRecord;

pub struct VecTableAccessorInner {
    tables: Vec<Table>,
    total_size: u64,
}

pub struct VecTableAccessor {
    inner: Arc<VecTableAccessorInner>,
}

pub struct VecTableAccessorIterator {
    inner: Arc<VecTableAccessorInner>,
    cursor: Option<usize>,
}

impl TableAccessorIterator for VecTableAccessorIterator {
    fn seek(&mut self, key: &Bytes) {
        let idx = crate::util::search(self.inner.tables.len(), |idx| {
            self.inner.tables[idx].largest().cmp(&key) != cmp::Ordering::Less
        });
        if idx >= self.inner.tables.len() {
            self.cursor = None;
            return;
        }
        self.cursor = Some(idx);
    }

    fn seek_for_previous(&mut self, key: &Bytes) {
        let n = self.inner.tables.len();
        let ridx = crate::util::search(self.inner.tables.len(), |idx| {
            self.inner.tables[n - 1 - idx].smallest().cmp(&key) != cmp::Ordering::Greater
        });
        if ridx >= self.inner.tables.len() {
            self.cursor = None;
            return;
        }
        self.cursor = Some(n - 1 - ridx);
    }

    fn seek_first(&mut self) {
        if self.inner.tables.len() > 0 {
            self.cursor = Some(0);
        } else {
            self.cursor = None;
        }
    }

    fn seek_last(&mut self) {
        if self.inner.tables.len() > 0 {
            self.cursor = Some(self.inner.tables.len() - 1);
        } else {
            self.cursor = None;
        }
    }

    fn prev(&mut self) {
        if let Some(cursor) = self.cursor.take() {
            if cursor > 0 {
                self.cursor = Some(cursor - 1);
            }
        }
    }

    fn next(&mut self) {
        if let Some(cursor) = self.cursor.take() {
            if cursor + 1 < self.inner.tables.len() {
                self.cursor = Some(cursor + 1);
            }
        }
    }

    fn table(&self) -> Option<Table> {
        self.cursor
            .as_ref()
            .map(|cursor| self.inner.tables[*cursor].clone())
    }

    fn valid(&self) -> bool {
        self.cursor.is_some()
    }
}

impl TableAccessor for VecTableAccessor {
    type Iter = VecTableAccessorIterator;

    fn create(mut tables: Vec<Table>) -> Self {
        let mut total_size = 0;
        for table in &tables {
            total_size += table.size();
        }

        tables.sort_by(|x, y| x.smallest().cmp(y.smallest()));
        VecTableAccessor {
            inner: Arc::new(VecTableAccessorInner { tables, total_size }),
        }
    }

    fn get(&self, key: &Bytes) -> Option<Table> {
        let idx = crate::util::search(self.inner.tables.len(), |idx| {
            self.inner.tables[idx].largest().cmp(&key) != cmp::Ordering::Less
        });
        if idx >= self.inner.tables.len() {
            None
        } else {
            Some(self.inner.tables[idx].clone())
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.tables.is_empty()
    }

    fn len(&self) -> usize {
        self.inner.tables.len()
    }

    fn total_size(&self) -> u64 {
        self.inner.total_size
    }

    fn new_iterator(&self) -> Self::Iter {
        VecTableAccessorIterator {
            inner: self.inner.clone(),
            cursor: None,
        }
    }

    fn replace_tables(&self, to_del: &[Table], to_add: &[Table]) -> Self {
        // TODO: handle deletion
        let mut to_del_map = HashSet::new();
        for table in to_del {
            to_del_map.insert(table.id());
        }

        let mut tables = Vec::with_capacity(self.inner.tables.len() + to_add.len());
        let mut total_size = self.inner.total_size;

        for table in &self.inner.tables {
            if !to_del_map.contains(&table.id()) {
                tables.push(table.clone());
                continue;
            }
            total_size = total_size.saturating_sub(table.size());
        }

        for table in to_add {
            total_size += table.size();
            tables.push(table.clone());
        }

        tables.sort_by(|x, y| x.smallest().cmp(y.smallest()));
        let inner = Arc::new(VecTableAccessorInner { tables, total_size });
        VecTableAccessor { inner }
    }
}
