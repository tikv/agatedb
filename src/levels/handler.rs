use crate::util::{KeyComparator, COMPARATOR};
use crate::value::Value;
use crate::Result;
use crate::{format::user_key, get_ts, structs::AgateIterator};
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

impl Drop for LevelHandler {
    fn drop(&mut self) {
        for table in self.tables.drain(..) {
            // TODO: simply forget table instance would cause memory leak. Should find
            // a better way to handle this. For example, `table.close_and_save()`, which
            // consumes table instance without deleting the files.
            std::mem::forget(table);
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

    pub fn get_table_for_key(&self, key: &Bytes) -> Vec<Table> {
        if self.level == 0 {
            // for level 0, we need to iterate every table.

            let mut out = self.tables.clone();
            out.reverse();

            out
        } else {
            let idx = crate::util::search(self.tables.len(), |idx| {
                COMPARATOR.compare_key(self.tables[idx].biggest(), key) != std::cmp::Ordering::Less
            });
            if idx >= self.tables.len() {
                vec![]
            } else {
                vec![self.tables[idx].clone()]
            }
        }
    }

    pub fn get(&self, key: &Bytes) -> Result<Option<Value>> {
        let tables = self.get_table_for_key(key);
        let key_no_ts = user_key(key);
        let hash = farmhash::fingerprint32(key_no_ts);
        let mut max_vs: Option<Value> = None;

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
                if let Some(ref max_vs) = max_vs {
                    if !max_vs.version < version {
                        continue;
                    }
                }

                let mut vs = it.value();
                vs.version = version;
                max_vs = Some(vs)
            }
        }

        Ok(max_vs)
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
