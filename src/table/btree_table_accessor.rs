use super::Table;
use crate::table::{TableAccessor, TableAccessorIterator};
use crate::util::{BTree, BTreeIterator, PageIterator};
use bytes::Bytes;

const MAX_LEAF_PAGE_SIZE: usize = 128;
const MAX_TREE_PAGE_SIZE: usize = 64;

pub struct BTreeTableAccessor {
    inner: BTree<Table>,
    total_size: u64,
}
pub struct BTreeTableAccessorIterator {
    inner: BTreeIterator<Table>,
}

impl TableAccessorIterator for BTreeTableAccessorIterator {
    fn seek(&mut self, key: &Bytes) {
        self.inner.seek(key);
    }

    fn seek_for_previous(&mut self, key: &Bytes) {
        self.inner.seek_for_previous(key);
    }

    fn seek_first(&mut self) {
        self.inner.seek_to_first();
    }

    fn seek_last(&mut self) {
        self.inner.seek_to_last();
    }

    fn prev(&mut self) {
        self.inner.prev();
    }

    fn next(&mut self) {
        self.inner.next();
    }

    fn table(&self) -> Option<Table> {
        self.inner.record()
    }

    fn valid(&self) -> bool {
        self.inner.valid()
    }
}

impl TableAccessor for BTreeTableAccessor {
    type Iter = BTreeTableAccessorIterator;

    fn create(tables: Vec<Table>) -> Self {
        let mut total_size = 0;
        let tree = BTree::new(MAX_TREE_PAGE_SIZE, MAX_LEAF_PAGE_SIZE);
        for t in &tables {
            total_size += t.size();
        }
        let inner = tree.replace(vec![], tables);
        BTreeTableAccessor { inner, total_size }
    }

    fn get(&self, key: &Bytes) -> Option<Table> {
        self.inner.get(key)
    }

    fn is_empty(&self) -> bool {
        self.inner.size() > 0
    }

    fn len(&self) -> usize {
        self.inner.size()
    }

    fn total_size(&self) -> u64 {
        self.total_size
    }

    fn new_iterator(&self) -> Self::Iter {
        BTreeTableAccessorIterator {
            inner: self.inner.new_iterator(),
        }
    }

    fn replace_tables(&self, to_del: &[Table], to_add: &[Table]) -> Self {
        let mut total_size = self.total_size;
        for t in to_add {
            total_size += t.size();
        }
        for t in to_del {
            total_size -= t.size();
        }
        let inner = self.inner.replace(to_del.to_vec(), to_add.to_vec());
        BTreeTableAccessor { inner, total_size }
    }
}
