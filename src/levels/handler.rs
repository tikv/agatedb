use std::collections::HashSet;
use std::sync::Arc;

use crate::table::{get_key_range_single, TableAccessor, TableAccessorIterator};
use crate::value::Value;
use crate::{format::user_key, get_ts, iterator_trait::AgateIterator};
use crate::{iterator::IteratorOptions, table::TableIterators};
use crate::{
    table::ConcatIterator,
    util::{KeyComparator, COMPARATOR},
};
use crate::{AgateOptions, Table};
use crate::{Error, Result};

use super::KeyRange;

use super::compaction::{get_key_range, CompactDef, CompactStatus};
use crate::table::merge_iterator::Iterators;
use crate::util::ComparableRecord;
use bytes::Bytes;

pub trait LevelHandler: Send + Sync {
    fn level(&self) -> usize;
    fn num_tables(&self) -> usize;
    fn total_size(&self) -> u64;

    fn get(&self, key: &Bytes) -> Result<Value>;

    fn append_iterators(&self, iters: &mut Vec<TableIterators>, opts: &IteratorOptions);
    fn overlapping_tables(&self, kr: &KeyRange) -> Vec<Table>;
    fn replace_tables(&mut self, to_del: &[Table], to_add: &[Table]) -> bool;
    fn select_table_range(
        &self,
        other: &dyn LevelHandler,
        compact_def: &mut CompactDef,
        status: &mut CompactStatus,
    ) -> Result<()>;

    fn pick_all_tables(&self, max_file_size: u64, exists: &HashSet<u64>) -> Vec<Table>;
}

pub struct HandlerBaseLevel<T: TableAccessor> {
    opts: AgateOptions,
    level: usize,
    table_acessor: Arc<T>,
}

impl<T: TableAccessor> Drop for HandlerBaseLevel<T> {
    fn drop(&mut self) {
        let mut iter = T::new_iterator(self.table_acessor.clone());
        iter.seek_first();
        while iter.valid() {
            iter.table().unwrap().mark_save();
            iter.next();
        }
    }
}

impl<T: TableAccessor> HandlerBaseLevel<T> {
    pub fn new(tables: Vec<Table>, opts: AgateOptions, level: usize) -> Self {
        let table_acessor = T::create(tables);
        Self {
            opts,
            level,
            table_acessor,
        }
    }
}

impl<T: 'static + TableAccessor> LevelHandler for HandlerBaseLevel<T> {
    fn level(&self) -> usize {
        self.level
    }

    fn num_tables(&self) -> usize {
        self.table_acessor.len()
    }

    fn total_size(&self) -> u64 {
        self.table_acessor.total_size()
    }

    fn get(&self, key: &Bytes) -> Result<Value> {
        let key_no_ts = user_key(key);
        let hash = farmhash::fingerprint32(key_no_ts);
        let mut max_vs = Value::default();

        if let Some(table) = self.table_acessor.get(key) {
            if !table.does_not_have(hash) {
                let mut it = table.new_iterator(0);
                it.seek(key);
                if it.valid() {
                    if crate::util::same_key(key, it.key()) {
                        let version = get_ts(it.key());
                        max_vs = it.value();
                        max_vs.version = version;
                    }
                }
            }
        }
        Ok(max_vs)
    }

    fn append_iterators(&self, iters: &mut Vec<TableIterators>, _: &IteratorOptions) {
        if !self.table_acessor.is_empty() {
            let acessor = self.table_acessor.clone();
            let iter = ConcatIterator::from_tables(Box::new(T::new_iterator(acessor)), 0);
            iters.push(TableIterators::from(iter));
        }
    }

    fn overlapping_tables(&self, kr: &KeyRange) -> Vec<Table> {
        use std::cmp::Ordering::*;

        if kr.left.is_empty() || kr.right.is_empty() {
            return vec![];
        }
        let mut ret = vec![];
        let mut iter = T::new_iterator(self.table_acessor.clone());
        iter.seek(&kr.left);
        while let Some(table) = iter.table() {
            if COMPARATOR.compare_key(&kr.right, table.smallest()) == Less {
                break;
            }
            ret.push(table);
            iter.next();
        }
        ret
    }

    fn replace_tables(&mut self, to_del: &[Table], to_add: &[Table]) -> bool {
        let acessor = self.table_acessor.replace_tables(to_del, to_add);
        self.table_acessor = acessor;
        true
    }

    fn select_table_range(
        &self,
        next_level: &dyn LevelHandler,
        compact_def: &mut CompactDef,
        status: &mut CompactStatus,
    ) -> Result<()> {
        let mut it = T::new_iterator(self.table_acessor.clone());
        it.seek_first();
        while let Some(table) = it.table() {
            if select_table_range(&table, next_level, compact_def, status) {
                return Ok(());
            }
            it.next();
        }
        Err(Error::CustomError("no table to fill".to_string()))
    }

    fn pick_all_tables(&self, max_file_size: u64, tables: &HashSet<u64>) -> Vec<Table> {
        let mut ret = vec![];
        let mut it = T::new_iterator(self.table_acessor.clone());
        it.seek_first();
        while let Some(table) = it.table() {
            if table.size() <= max_file_size && !tables.contains(&table.id()) {
                ret.push(table);
            }
            it.next();
        }
        ret
    }
}

pub struct HandlerLevel0 {
    tables: Vec<Table>,
    opts: AgateOptions,
    level: usize,
    total_size: u64,
}

impl HandlerLevel0 {
    pub fn new(mut tables: Vec<Table>, opts: AgateOptions, level: usize) -> Self {
        tables.sort_by(|x, y| x.id().cmp(&y.id()));
        let mut total_size = 0;
        for table in &tables {
            total_size += table.size();
        }
        Self {
            opts,
            level,
            tables,
            total_size,
        }
    }
}

impl LevelHandler for HandlerLevel0 {
    fn level(&self) -> usize {
        self.level
    }

    fn num_tables(&self) -> usize {
        self.tables.len()
    }

    fn total_size(&self) -> u64 {
        self.total_size
    }

    fn get(&self, key: &Bytes) -> Result<Value> {
        let key_no_ts = user_key(key);
        let hash = farmhash::fingerprint32(key_no_ts);
        let mut max_vs = Value::default();

        for table in self.tables.iter() {
            if table.does_not_have(hash) {
                continue;
            }

            let mut it = table.new_iterator(0);
            it.seek(key);
            if !it.valid() {
                continue;
            }

            if crate::util::same_key(key, it.key()) {
                let version = get_ts(it.key());
                if max_vs.version < version {
                    let mut vs = it.value();
                    vs.version = version;
                    max_vs = vs;
                }
            }
        }
        Ok(max_vs)
    }

    fn append_iterators(&self, iters: &mut Vec<Iterators>, opts: &IteratorOptions) {
        for table in self.tables.iter().rev() {
            if opts.pick_table(table) {
                iters.push(TableIterators::from(table.new_iterator(0)));
            }
        }
    }

    fn overlapping_tables(&self, _: &KeyRange) -> Vec<Table> {
        let mut out = vec![];
        let mut kr = KeyRange::default();
        for table in self.tables.iter() {
            let dkr = get_key_range_single(table);
            if kr.overlaps_with(&dkr) {
                out.push(table.clone());
                kr.extend(&dkr);
            } else {
                break;
            }
        }
        out
    }

    fn replace_tables(&mut self, to_del: &[Table], to_add: &[Table]) -> bool {
        assert_eq!(self.level, 0);
        if self.tables.len() >= self.opts.num_level_zero_tables_stall {
            return false;
        }
        let mut to_del_map = HashSet::new();

        for table in to_del {
            to_del_map.insert(table.id());
        }

        let mut new_tables = vec![];

        for table in &self.tables {
            if !to_del_map.contains(&table.id()) {
                new_tables.push(table.clone());
                continue;
            }
            self.total_size = self.total_size.saturating_sub(table.size());
        }
        for t in to_add {
            self.total_size += t.size();
            new_tables.push(t.clone());
        }
        new_tables.sort_by(|a, b| a.id().cmp(&b.id()));
        self.tables = new_tables;
        true
    }

    fn select_table_range(
        &self,
        next_level: &dyn LevelHandler,
        compact_def: &mut CompactDef,
        status: &mut CompactStatus,
    ) -> Result<()> {
        for table in self.tables.iter() {
            if select_table_range(&table, next_level, compact_def, status) {
                return Ok(());
            }
        }
        Err(Error::CustomError("no table to fill".to_string()))
    }

    fn pick_all_tables(&self, max_file_size: u64, tables: &HashSet<u64>) -> Vec<Table> {
        let mut out = vec![];
        for table in self.tables.iter() {
            if table.size() > max_file_size {
                // file already big, don't include it
                continue;
            }
            // TODO: created at logic
            if tables.contains(&table.id()) {
                continue;
            }
            out.push(table.clone());
        }
        out
    }
}

fn select_table_range(
    table: &Table,
    next_level: &dyn LevelHandler,
    compact_def: &mut CompactDef,
    cpt_status: &mut CompactStatus,
) -> bool {
    compact_def.this_size = table.size();
    compact_def.this_range = get_key_range_single(table);
    // if we're already compacting this range, don't do anything
    if cpt_status.overlaps_with(compact_def.this_level_id, &compact_def.this_range) {
        return false;
    }
    compact_def.top = vec![table.clone()];
    compact_def.bot = next_level.overlapping_tables(&compact_def.this_range);

    if compact_def.bot.is_empty() {
        compact_def.bot = vec![];
        compact_def.next_range = compact_def.this_range.clone();
        if let Err(_) = cpt_status.compare_and_add(compact_def) {
            return false;
        }
        return true;
    }

    compact_def.next_range = get_key_range(&compact_def.bot);

    if cpt_status.overlaps_with(compact_def.next_level_id, &compact_def.next_range) {
        return false;
    }

    if let Err(_) = cpt_status.compare_and_add(compact_def) {
        return false;
    }
    return true;
}
