#![allow(dead_code)]
#![allow(unused_variables)]

use super::KeyRange;
use crate::value::Value;
use crate::Result;
use crate::{iterator::IteratorOptions, table::TableIterators};
use crate::{AgateOptions, Table};
use bytes::Bytes;

pub struct LevelHandler {
    opts: AgateOptions,
    pub level: usize,
    pub tables: Vec<Table>,
    pub total_size: u64,
}

impl Drop for LevelHandler {
    fn drop(&mut self) {
        for table in self.tables.drain(..) {
            // After calling `mark_save`, the SST file will be retained on disk
            // and won't be deleted when being dropped.
            table.mark_save();
        }
    }
}

impl LevelHandler {
    pub fn new(opts: AgateOptions, level: usize) -> Self {
        Self {
            opts,
            level,
            tables: vec![],
            total_size: 0,
        }
    }

    pub fn try_add_l0_table(&mut self, table: Table) -> bool {
        unimplemented!()
    }

    pub fn num_tables(&self) -> usize {
        self.tables.len()
    }

    pub fn get_table_for_key(&self, key: &Bytes) -> Vec<Table> {
        unimplemented!()
    }

    pub fn get(&self, key: &Bytes) -> Result<Value> {
        unimplemented!()
    }

    pub fn overlapping_tables(&self, kr: &KeyRange) -> (usize, usize) {
        unimplemented!()
    }

    pub fn replace_tables(&mut self, to_del: &[Table], to_add: &[Table]) -> Result<()> {
        unimplemented!()
    }

    pub fn delete_tables(&mut self, to_del: &[Table]) -> Result<()> {
        unimplemented!()
    }

    pub fn init_tables(&mut self, tables: Vec<Table>) {
        unimplemented!()
    }

    pub(crate) fn append_iterators(&self, iters: &mut Vec<TableIterators>, opts: &IteratorOptions) {
        unimplemented!()
    }
}
