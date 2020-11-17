mod compaction;
mod handler;

use compaction::{CompactDef, CompactStatus, CompactionPriority, LevelCompactStatus};
use handler::LevelHandler;

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

struct Core {
    next_file_id: AtomicU64,
    // `levels[i].level == i` should be ensured
    levels: Vec<Arc<RwLock<LevelHandler>>>,
    opts: AgateOptions,
    // TODO: agate oracle, manifest should be added here
    cpt_status: RwLock<CompactStatus>,
}

pub struct LevelsController {
    core: Arc<Core>,
}

impl Core {
    fn new(opts: AgateOptions) -> Result<Self> {
        let mut levels = vec![];
        let mut cpt_status_levels = vec![];
        for i in 0..opts.max_levels {
            levels.push(Arc::new(RwLock::new(LevelHandler::new(opts.clone(), i))));
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

    fn fill_tables_l0(&self) -> Result<()> {
        unimplemented!()
    }

    fn fill_tables(&self) -> Result<()> {
        unimplemented!()
    }

    // pick some tables on that level and compact it to next level
    fn do_compact(&self, idx: usize, level: usize) -> Result<()> {
        assert!(level < self.opts.max_levels);

        let next_level;

        if level == 0 {
            self.fill_tables_l0()?;
            next_level = self.levels[1].clone();
        } else {
            self.fill_tables()?;
            next_level = self.levels[level + 1].clone();
        };

        let compact_def = CompactDef {
            compactor_id: idx,
            this_level: self.levels[level].clone(),
            next_level,
        };

        Ok(())
    }

    fn level_targets(&self) -> Targets {
        let adjust = |size| {
            if size < self.opts.base_level_size {
                self.opts.base_level_size
            } else {
                size
            }
        };

        let mut targets = Targets {
            base_level: 0,
            target_size: vec![0; self.levels.len()],
            file_size: vec![0; self.levels.len()],
        };

        let mut db_size = self.last_level().read().unwrap().total_size;

        for i in self.levels.len() - 1..0 {
            let ltarget = adjust(db_size);
            targets.target_size[i] = ltarget;
            if targets.base_level == 0 && ltarget <= self.opts.base_level_size {
                targets.base_level = i;
            }
            db_size /= self.opts.level_size_multiplier as u64;
        }

        let mut tsz = self.opts.base_level_size;

        for i in 0..self.levels.len() {
            if i == 0 {
                targets.file_size[i] = self.opts.mem_table_size;
            } else if i <= targets.base_level {
                targets.file_size[i] = tsz;
            } else {
                tsz *= self.opts.table_size_multiplier as u64;
                targets.file_size[i] = tsz;
            }
        }

        targets
    }

    fn last_level(&self) -> &Arc<RwLock<LevelHandler>> {
        self.levels.last().unwrap()
    }

    fn pick_compact_levels(&self) -> Vec<CompactionPriority> {
        let targets = Arc::new(self.level_targets());
        let mut prios = vec![];

        let mut add_priority = |level, score| {
            let pri = CompactionPriority {
                level,
                score,
                adjusted: score,
                targets: targets.clone(),
                drop_prefixes: vec![],
            };
            prios.push(pri);
        };

        add_priority(
            0,
            self.levels[0].read().unwrap().num_tables() as f64
                / self.opts.num_level_zero_tables as f64,
        );

        let cpt_status = self.cpt_status.read().unwrap();

        for i in 1..self.levels.len() {
            let del_size = cpt_status.levels[i].del_size;
            let level = self.levels[i].read().unwrap();
            let size = level.total_size - del_size;
            add_priority(i, size as f64 / targets.target_size[i] as f64);
        }

        assert_eq!(prios.len(), self.levels.len());

        // TODO: adjust score

        let mut x: Vec<CompactionPriority> = prios.into_iter().filter(|x| x.score > 1.0).collect();
        x.sort_by(|x, y| x.adjusted.partial_cmp(&y.adjusted).unwrap());
        x.reverse();
        x
    }
}

struct Targets {
    base_level: usize,
    target_size: Vec<u64>,
    file_size: Vec<u64>,
}

impl LevelsController {
    pub fn new(opts: AgateOptions) -> Result<Self> {
        Ok(Self {
            core: Arc::new(Core::new(opts)?),
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

    fn run_compactor(
        &self,
        idx: usize,
        closer: Closer,
        pool: &yatp::ThreadPool<yatp::task::callback::TaskCell>,
    ) {
        let max_levels = self.core.opts.max_levels;
        let core = self.core.clone();
        pool.spawn(move |_: &mut Handle<'_>| {
            let run_once = || {
                // TODO: automatically determine compact prio,
                // now we always compact from L0 to Ln
                for level in 0..(max_levels - 1) {
                    if let Err(err) = core.do_compact(idx, level) {
                        println!("error while compaction: {:?}", err);
                    }
                }
            };
            let ticker = tick(Duration::from_millis(50));
            select! {
                recv(ticker) -> _ => return,
                recv(closer.has_been_closed()) -> _ => run_once()
            }
        });
    }

    pub fn start_compact(
        &self,
        closer: Closer,
        pool: &yatp::ThreadPool<yatp::task::callback::TaskCell>,
    ) {
        for i in 0..self.core.opts.num_compactors {
            self.run_compactor(i, closer.clone(), pool);
        }
    }
}
