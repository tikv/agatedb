use super::Table;
use bytes::Bytes;
use std::sync::Arc;

pub trait TableAccessorIterator {
    fn seek(&mut self, key: &Bytes);
    fn seek_for_previous(&mut self, key: &Bytes);
    fn seek_first(&mut self);
    fn seek_last(&mut self);
    fn prev(&mut self);
    fn next(&mut self);
    fn table(&self) -> Option<Table>;
    fn valid(&self) -> bool;
}

pub trait TableAccessor: Send + Sync {
    type Iter: TableAccessorIterator;

    fn create(tables: Vec<Table>) -> Arc<Self>;
    fn get(&self, key: &Bytes) -> Option<Table>;
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
    fn total_size(&self) -> u64;
    fn new_iterator(acessor: Arc<Self>) -> Self::Iter;
    fn replace_tables(&self, to_del: &[Table], to_add: &[Table]) -> Arc<Self>;
}
