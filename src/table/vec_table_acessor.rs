use bytes::Bytes;
use std::cmp;
use std::collections::HashSet;
use std::sync::Arc;

use super::table_accessor::{TableAccessor, TableAccessorIterator};
use super::Table;

use crate::util::{KeyComparator, COMPARATOR};

pub struct VecTableAccessor {
    tables: Vec<Table>,
    total_size: u64,
}

pub struct VecTableAccessorIterator {
    inner: Arc<VecTableAccessor>,
    cursor: Option<usize>,
}

impl TableAccessorIterator for VecTableAccessorIterator {
    fn seek(&mut self, key: &Bytes) {
        let idx = crate::util::search(self.inner.tables.len(), |idx| {
            COMPARATOR.compare_key(self.inner.tables[idx].largest(), key) != cmp::Ordering::Less
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
            COMPARATOR.compare_key(self.inner.tables[n - 1 - idx].smallest(), key)
                != cmp::Ordering::Greater
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

    fn create(mut tables: Vec<Table>) -> Arc<Self> {
        let mut total_size = 0;
        for table in &tables {
            total_size += table.size();
        }

        tables.sort_by(|x, y| COMPARATOR.compare_key(x.smallest(), y.smallest()));
        Arc::new(VecTableAccessor { tables, total_size })
    }

    fn get(&self, key: &Bytes) -> Option<Table> {
        let idx = crate::util::search(self.tables.len(), |idx| {
            COMPARATOR.compare_key(self.tables[idx].largest(), key) != cmp::Ordering::Less
        });
        if idx >= self.tables.len() {
            None
        } else {
            Some(self.tables[idx].clone())
        }
    }

    fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    fn len(&self) -> usize {
        self.tables.len()
    }

    fn total_size(&self) -> u64 {
        self.total_size
    }

    fn new_iterator(inner: Arc<Self>) -> Self::Iter {
        VecTableAccessorIterator {
            inner,
            cursor: None,
        }
    }

    fn replace_tables(&self, to_del: &[Table], to_add: &[Table]) -> Arc<Self> {
        // TODO: handle deletion
        let mut to_del_map = HashSet::new();
        for table in to_del {
            to_del_map.insert(table.id());
        }

        let mut tables = Vec::with_capacity(self.tables.len() + to_add.len());
        let mut total_size = self.total_size;

        for table in &self.tables {
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

        tables.sort_by(|x, y| COMPARATOR.compare_key(x.smallest(), y.smallest()));
        Arc::new(VecTableAccessor { tables, total_size })
    }

    fn delete_tables(&self, to_del: &[Table]) -> Arc<Self> {
        let mut to_del_map = HashSet::new();

        for table in to_del {
            to_del_map.insert(table.id());
        }

        let mut tables = Vec::with_capacity(self.tables.len());
        let mut total_size = self.total_size;

        for table in &self.tables {
            if !to_del_map.contains(&table.id()) {
                tables.push(table.clone());
                continue;
            }
            total_size = total_size.saturating_sub(table.size());
        }

        Arc::new(VecTableAccessor { tables, total_size })
    }
}
