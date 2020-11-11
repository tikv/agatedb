use crate::format::get_ts;
use crate::structs::AgateIterator;
use crate::table::{MergeIterator, TableIterators};
use crate::value::Value;
use crate::{AgateOptions, Table};
use crate::{Error, Result};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{RwLock, Arc};

use bytes::Bytes;

#[derive(Default)]
struct LevelCompactStatus {
    ranges: (),
    del_size: u64,
}

struct CompactStatus {
    levels: Vec<LevelCompactStatus>,
    tables: HashMap<u64, ()>,
}
struct LevelHandler {
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

struct Core {
    next_file_id: AtomicU64,
    // `levels[i].level == i` should be ensured
    levels: Vec<RwLock<LevelHandler>>,
    opts: AgateOptions,
    // TODO: agate oracle, manifest should be added here
    cpt_status: RwLock<CompactStatus>,
}

pub struct LevelsController {
    core: Arc<Core>
}

impl Core {
    fn new(opts: AgateOptions) -> Result<Self> {
        let mut levels = vec![];
        let mut cpt_status_levels = vec![];
        for i in 0..opts.max_levels {
            levels.push(RwLock::new(LevelHandler::new(opts.clone(), i)));
            cpt_status_levels.push(LevelCompactStatus::default());
        }

        let lvctl = Self {
            next_file_id: AtomicU64::new(0),
            levels,
            opts: opts.clone(),
            cpt_status: RwLock::new(CompactStatus {
                levels: cpt_status_levels,
                tables: HashMap::new(),
            }),
        };

        // TODO: load levels from disk

        Ok(lvctl)
    }

    fn reserve_file_id(&self) -> u64 {
        self.next_file_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn add_l0_table(&self, table: Table) -> Result<()> {
        if !self.opts.in_memory {
            // TODO: update manifest
        }

        while !self.levels[0].write()?.try_add_l0_table(table.clone()) {
            println!("L0 stalled");
            // TODO: enhance stall logic
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        Ok(())
    }

    fn get(&self, key: &Bytes, max_value: Value, start_level: usize) -> Result<Value> {
        // TODO: check is_closed

        let version = get_ts(key);

        for (level, handler) in self.levels.iter().enumerate() {
            if level < start_level {
                continue;
            }
            match handler.read()?.get(key) {
                Ok(Some(value)) => {
                    if value.value.is_empty() && value.meta == 0 {
                        continue;
                    }
                    if value.version == version {
                        return Ok(value);
                    }
                }
                Ok(None) => {
                    continue;
                }
                Err(err) => {
                    return Err(Error::CustomError(
                        format!("get key: {:?}, {:?}", Bytes::copy_from_slice(key), err)
                            .to_string(),
                    ))
                }
            }
        }

        Ok(max_value)
    }
}

impl LevelsController {
    pub fn new(opts: AgateOptions) -> Result<Self> {
        Ok(Self {
            core: Arc::new(Core::new(opts)?)
        })
    }

    pub fn add_l0_table(&self, table: Table) -> Result<()> {
        self.core.add_l0_table(table)
    }

    pub fn get(&self, key: &Bytes, max_value: Value, start_level: usize) -> Result<Value> {
        self.core.get(key, max_value, start_level)
    }

    pub fn reserve_file_id(&self) -> u64 {
        self.core.reserve_file_id()
    }

    fn run_compactor(&self, idx: usize, pool: &yatp::ThreadPool<yatp::task::callback::TaskCell>) {
        
    }

    pub fn start_compact(&self, pool: &yatp::ThreadPool<yatp::task::callback::TaskCell>) {
        for i in 0..self.core.opts.num_compactors {
            self.run_compactor(i, pool);
        }
    }
}
