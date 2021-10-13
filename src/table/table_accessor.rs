use super::Table;
use bytes::Bytes;

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

    fn create(tables: Vec<Table>) -> Self;
    fn get(&self, key: &Bytes) -> Option<Table>;
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
    fn total_size(&self) -> u64;
    fn new_iterator(&self) -> Self::Iter;
    fn replace_tables(&self, to_del: &[Table], to_add: &[Table]) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::tests::build_table_data;
    use crate::table::{BTreeTableAccessor, Table, VecTableAccessor};
    use crate::util::ComparableRecord;
    use crate::TableOptions;

    fn test_table_accessor<A: TableAccessor>(accessor: A) {
        let gap = 10;
        let mut tables = vec![];
        for i in 1000..3000 {
            let smallest = i * gap;
            let largest = (i + 1) * gap;
            let mut bopts = TableOptions::default();
            bopts.block_size = 1024;
            bopts.table_capacity = 2048;
            bopts.table_size = 4096;
            let mut kvs = vec![];
            for j in smallest..largest {
                if j % 2 == 0 {
                    kvs.push((Bytes::from(j.to_string()), Bytes::from(j.to_string())));
                }
            }
            let data = build_table_data(kvs, bopts.clone());
            let table = Table::open_in_memory(data, i, bopts).unwrap();
            tables.push(table);
        }
        let accessor = accessor.replace_tables(&[], &tables);
        let mut iter = accessor.new_iterator();
        iter.seek(&Bytes::from(20005.to_string()));
        let mut expected = 2000;
        while iter.valid() {
            let table = iter.table().unwrap();
            assert_eq!(expected, table.id());
            expected += 1;
            iter.next();
        }
        let mut iter = accessor.new_iterator();
        iter.seek_for_previous(&Bytes::from(20005.to_string()));
        let mut expected = 2000;
        while iter.valid() {
            let table = iter.table().unwrap();
            assert_eq!(expected, table.id());
            expected -= 1;
            iter.prev();
        }
    }

    #[test]
    fn test_vec_accessor() {
        let accessor = VecTableAccessor::create(vec![]);
        test_table_accessor(accessor);
    }

    #[test]
    fn test_btree_accessor() {
        let accessor = BTreeTableAccessor::create(vec![]);
        test_table_accessor(accessor);
    }
}
