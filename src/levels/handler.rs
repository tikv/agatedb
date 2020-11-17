use crate::closer::Closer;
use crate::format::get_ts;
use crate::structs::AgateIterator;
use crate::table::{MergeIterator, TableIterators};
use crate::value::Value;
use crate::{AgateOptions, Table};
use crate::{Error, Result};

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use crossbeam_channel::{select, tick};
use yatp::task::callback::Handle;

pub struct LevelHandler {
    opts: AgateOptions,
    level: usize,
    tables: Vec<Table>,
    total_size: u64,
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
}
