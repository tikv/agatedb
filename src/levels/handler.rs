use std::collections::HashSet;

use bytes::Bytes;

use super::KeyRange;
use crate::{
    format::user_key,
    get_ts,
    iterator::IteratorOptions,
    iterator_trait::AgateIterator,
    table::{ConcatIterator, TableIterators, ITERATOR_REVERSED},
    util::{KeyComparator, COMPARATOR},
    value::Value,
    AgateOptions, Result, Table,
};

pub struct LevelHandler {
    opts: AgateOptions,
    pub level: usize,

    // For level >= 1, tables are sorted by key ranges, which do not overlap.
    // For level 0, tables are sorted by time.
    // For level 0, newest table are at the back. Compact the oldest one first, which is at the front.
    pub tables: Vec<Table>,
    pub total_size: u64,
}

impl Drop for LevelHandler {
    fn drop(&mut self) {
        for table in self.tables.drain(..) {
            // TODO: simply forget table instance would cause memory leak. Should find
            // a better way to handle this. For example, `table.close_and_save()`, which
            // consumes table instance without deleting the files.
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

    /// Replaces tables with given tables. This is done during loading.
    pub fn init_tables(&mut self, tables: Vec<Table>) {
        self.tables = tables;
        self.total_size = 0;
        for table in &self.tables {
            self.total_size += table.size();
        }

        if self.level == 0 {
            // Key range will overlap. Just sort by file id in ascending order
            // because newer tables are at the end of level 0.
            self.tables.sort_by_key(|x| x.id());
        } else {
            // Sort tables by keys.
            self.tables
                .sort_by(|x, y| COMPARATOR.compare_key(x.smallest(), y.smallest()));
        }
    }

    pub fn delete_tables(&mut self, to_del: &[Table]) {
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

        self.tables = new_tables;
    }

    pub fn replace_tables(&mut self, to_del: &[Table], to_add: &[Table]) -> Result<()> {
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

        // Increase totalSize first.
        for table in to_add {
            self.total_size += table.size();
            new_tables.push(table.clone());
        }

        new_tables.sort_by(|x, y| COMPARATOR.compare_key(x.smallest(), y.smallest()));

        self.tables = new_tables;

        Ok(())
    }

    /// Returns true if ok and no stalling.
    pub fn try_add_l0_table(&mut self, table: Table) -> bool {
        assert_eq!(self.level, 0);

        // Stall (by returning false) if we are above the specified stall setting for L0.
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

    /// Returns a list of table associated with the `key`.
    pub fn get_table_for_key(&self, key: &Bytes) -> Vec<Table> {
        if self.level == 0 {
            // For level 0, we need to iterate every table.
            let mut out = self.tables.clone();
            // CAUTION: Reverse the tables.
            out.reverse();

            out
        } else {
            let idx = crate::util::search(self.tables.len(), |idx| {
                COMPARATOR.compare_key(self.tables[idx].biggest(), key) != std::cmp::Ordering::Less
            });
            if idx >= self.tables.len() {
                // Given key is strictly > than every element we have.
                vec![]
            } else {
                vec![self.tables[idx].clone()]
            }
        }
    }

    /// Returns value for a given key or the key after that.
    pub fn get(&self, key: &Bytes) -> Result<Value> {
        let tables = self.get_table_for_key(key);
        let key_no_ts = user_key(key);
        let hash = farmhash::fingerprint32(key_no_ts);
        let mut max_vs = Value::default();

        for table in tables {
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

    /// Appends iterators to an array of iterators, for merging.
    pub(crate) fn append_iterators(&self, iters: &mut Vec<TableIterators>, opts: &IteratorOptions) {
        let topt = if opts.reverse { ITERATOR_REVERSED } else { 0 };

        if self.level == 0 {
            // Remember to add in reverse order!
            for table in self.tables.iter().rev() {
                if opts.pick_table(table) {
                    iters.push(TableIterators::from(table.new_iterator(topt)));
                }
            }
            return;
        }

        let mut tables = self.tables.clone();
        opts.pick_tables(&mut tables);
        if !tables.is_empty() {
            let iter = ConcatIterator::from_tables(tables, topt);
            iters.push(TableIterators::from(iter));
        }
    }

    /// Returns the tables that intersect with key range. Returns a half-interval.
    pub fn overlapping_tables(&self, kr: &KeyRange) -> (usize, usize) {
        use std::cmp::Ordering::*;

        match kr {
            KeyRange::Range { left, right } => {
                let left = crate::util::search(self.tables.len(), |i| {
                    COMPARATOR.compare_key(left, self.tables[i].biggest()) != Greater
                });
                let right = crate::util::search(self.tables.len(), |i| {
                    COMPARATOR.compare_key(right, self.tables[i].smallest()) == Less
                });
                (left, right)
            }
            KeyRange::Inf => unreachable!(),
            KeyRange::Empty => (0, 0),
        }
    }
}
