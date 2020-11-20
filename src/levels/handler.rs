use crate::structs::AgateIterator;
use crate::table::{MergeIterator, TableIterators};
use crate::util::{KeyComparator, COMPARATOR};
use crate::value::Value;
use crate::Result;
use crate::{AgateOptions, Table};

use super::KeyRange;

use bytes::Bytes;

use std::collections::HashSet;

pub struct LevelHandler {
    opts: AgateOptions,
    pub level: usize,
    pub tables: Vec<Table>,
    pub total_size: u64,
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
        assert_eq!(self.level, 0);
        if self.tables.len() >= self.opts.num_level_zero_tables_stall {
            return false;
        }

        self.total_size += table.size();
        self.tables.push(table);

        true
    }

    pub fn num_tables(&self) -> usize {
        self.tables.len()
    }

    pub fn get(&self, key: &Bytes) -> Result<Option<Value>> {
        // TODO: Add binary search logic. For now we just merge iterate all tables.
        // TODO: fix wrong logic. This function now just checks if we found the correct key,
        // regardless of their version.

        if self.tables.is_empty() {
            return Ok(None);
        }

        let iters: Vec<Box<TableIterators>> = self
            .tables
            .iter()
            .map(|x| x.new_iterator(0))
            .map(|x| Box::new(TableIterators::from(x)))
            .collect();
        let mut iter = MergeIterator::from_iterators(iters, false);

        iter.seek(key);

        if !iter.valid() {
            return Ok(None);
        }

        if !crate::util::same_key(&key, iter.key()) {
            return Ok(None);
        }

        Ok(Some(iter.value()))
    }

    pub fn overlapping_tables(&self, kr: &KeyRange) -> (usize, usize) {
        use std::cmp::Ordering::*;

        if kr.left.is_empty() || kr.right.is_empty() {
            return (0, 0);
        }
        let left = crate::util::search(self.tables.len(), |i| {
            match COMPARATOR.compare_key(&kr.left, self.tables[i].biggest()) {
                Less | Equal => true,
                _ => false,
            }
        });
        let right = crate::util::search(self.tables.len(), |i| {
            match COMPARATOR.compare_key(&kr.right, self.tables[i].smallest()) {
                Less => true,
                _ => false,
            }
        });
        (left, right)
    }

    pub fn replace_tables(&mut self, to_del: &[Table], to_add: &[Table]) -> Result<()> {
        // TODO: handle deletion
        let mut to_del_map = HashSet::new();

        for table in to_del {
            to_del_map.insert(table.id());
        }

        let mut new_tables = vec![];

        for table in &self.tables {
            if to_del_map.get(&table.id()).is_none() {
                new_tables.push(table.clone());
                continue;
            }
            self.total_size -= table.size();
        }

        for table in to_add {
            self.total_size += table.size();
            new_tables.push(table.clone());
        }

        new_tables.sort_by(|x, y| COMPARATOR.compare_key(x.smallest(), y.smallest()));

        self.tables = new_tables;

        Ok(())
    }

    pub fn delete_tables(&mut self, to_del: &[Table]) -> Result<()> {
        let mut to_del_map = HashSet::new();

        for table in to_del {
            to_del_map.insert(table.id());
        }

        let mut new_tables = vec![];

        for table in &self.tables {
            if to_del_map.get(&table.id()).is_none() {
                new_tables.push(table.clone());
                continue;
            }
            self.total_size -= table.size();
        }

        self.tables = new_tables;

        Ok(())
    }
}
