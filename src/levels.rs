mod compaction;
mod handler;

use compaction::{
    get_key_range, get_key_range_single, CompactDef, CompactStatus, CompactionPriority, KeyRange,
    LevelCompactStatus, Targets,
};
use handler::LevelHandler;

use crate::closer::Closer;
use crate::format::{get_ts, key_with_ts, user_key};
use crate::value::Value;
use crate::{AgateOptions, Table};
use crate::{Error, Result};

use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
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
                tables: HashSet::new(),
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
            std::thread::sleep(std::time::Duration::from_millis(1000));
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

    fn fill_tables_l0_to_lbase(&self, compact_def: &mut CompactDef) -> Result<()> {
        let this_level = compact_def.this_level.write().unwrap();
        let next_level = compact_def.next_level.write().unwrap();
        let mut cpt_status = self.cpt_status.write().unwrap();

        if next_level.level == 0 {
            panic!("base level can't be zero");
        }

        if compact_def.prios.adjusted > 0.0 && compact_def.prios.adjusted < 1.0 {
            return Err(Error::CustomError(
                "score less than 1.0, not compact to Lbase".to_string(),
            ));
        }

        if this_level.tables.is_empty() {
            return Err(Error::CustomError("not table in this level".to_string()));
        }

        if compact_def.drop_prefixes.is_empty() {
            let mut out = vec![];
            let mut kr = KeyRange::default();
            for table in this_level.tables.iter() {
                let dkr = get_key_range_single(table);
                if kr.overlaps_with(&dkr) {
                    out.push(table.clone());
                    kr.extend(&dkr);
                } else {
                    break;
                }
            }
            compact_def.top = out;
        }

        compact_def.this_range = get_key_range(&compact_def.top);

        let (left, right) = next_level.overlapping_tables(&compact_def.this_range);

        compact_def.bot = next_level.tables[left..right].to_vec();

        if compact_def.bot.is_empty() {
            compact_def.next_range = compact_def.this_range.clone();
        } else {
            compact_def.next_range = get_key_range(&compact_def.bot);
        }

        cpt_status.compare_and_add(&compact_def, this_level.level, next_level.level)?;

        Ok(())
    }

    fn fill_tables_l0_to_l0(&self, compact_def: &mut CompactDef) -> Result<()> {
        if compact_def.compactor_id != 0 {
            return Err(Error::CustomError(
                "only compactor zero can compact L0 to L0".to_string(),
            ));
        }
        // TODO: should compact_def be mutable?
        compact_def.next_level = self.levels[0].clone();
        compact_def.next_range = KeyRange::default();
        compact_def.bot = vec![];

        let this_level = compact_def.this_level.write().unwrap();
        let next_level = compact_def.next_level.write().unwrap();
        let mut cpt_status = self.cpt_status.write().unwrap();

        let mut out = vec![];
        // let now = std::time::Instant::now();

        for table in this_level.tables.iter() {
            if table.size() > 2 * compact_def.targets.file_size[0] {
                // file already big, don't include it
                continue;
            }
            // TODO: created at logic
            if cpt_status.tables.get(&table.id()).is_some() {
                continue;
            }
            out.push(table.clone());
        }

        if out.len() < 4 {
            return Err(Error::CustomError("not enough table to merge".to_string()));
        }

        compact_def.this_range = KeyRange::inf();
        compact_def.top = out;

        cpt_status.levels[this_level.level]
            .ranges
            .push(KeyRange::inf());

        for table in compact_def.top.iter() {
            assert!(cpt_status.tables.insert(table.id()), false);
        }

        compact_def.targets.file_size[0] = std::u64::MAX;

        Ok(())
    }

    fn fill_tables_l0(&self, compact_def: &mut CompactDef) -> Result<()> {
        if let Err(err) = self.fill_tables_l0_to_lbase(compact_def) {
            println!("error when fill L0 to Lbase {:?}", err);
            return self.fill_tables_l0_to_l0(compact_def);
        }
        Ok(())
    }

    fn fill_tables(&self, compact_def: &CompactDef) -> Result<()> {
        unimplemented!()
        // Ok(())
    }

    fn run_compact_def(
        &self,
        idx: usize,
        level: usize,
        compact_def: &mut CompactDef,
    ) -> Result<()> {
        if compact_def.targets.file_size.len() == 0 {
            return Err(Error::CustomError("targets not set".to_string()));
        }

        let this_level = compact_def.this_level.clone();
        let next_level = compact_def.next_level.clone();
        let this_level_id = this_level.read().unwrap().level;
        let next_level_id = next_level.read().unwrap().level;

        assert_eq!(compact_def.splits.len(), 0);

        if this_level_id == 0 && next_level_id == 0 {
        } else {
            self.add_splits(compact_def);
        }

        if compact_def.splits.len() == 0 {
            compact_def.splits.push(KeyRange::default());
        }

        let new_tables = self.compact_build_tables(level, compact_def)?;

        // TODO: add change to manifest

        let mut this_level = this_level.write().unwrap();
        let mut next_level = next_level.write().unwrap();

        next_level.replace_tables(&compact_def.bot, &new_tables)?;
        this_level.delete_tables(&compact_def.top)?;

        // TODO: logging

        Ok(())
    }

    fn compact_build_tables(&self, level: usize, compact_def: &CompactDef) -> Result<Vec<Table>> {
        unimplemented!()
    }

    fn add_splits(&self, compact_def: &mut CompactDef) {
        const N: usize = 3;
        // assume this_range is never inf
        let mut skr = compact_def.this_range.clone();
        skr.extend(&compact_def.next_range);

        let mut add_range = |splits: &mut Vec<KeyRange>, right| {
            skr.right = right;
            splits.push(skr.clone());
            skr.left = skr.right.clone();
        };

        for (i, table) in compact_def.bot.iter().enumerate() {
            if i == compact_def.bot.len() - 1 {
                add_range(&mut compact_def.splits, Bytes::new());
                return;
            }

            if i % N == N - 1 {
                let biggest = table.biggest();
                let mut buf = BytesMut::with_capacity(biggest.len() + 8);
                buf.put(user_key(&biggest));
                let right = key_with_ts(buf, std::u64::MAX);
                add_range(&mut compact_def.splits, right);
            }
        }
    }

    // pick some tables on that level and compact it to next level
    fn do_compact(&self, idx: usize, mut cpt_prio: CompactionPriority) -> Result<()> {
        let level = cpt_prio.level;
        assert!(level + 1 < self.opts.max_levels);

        if cpt_prio.targets.base_level == 0 {
            cpt_prio.targets = self.level_targets();
        }

        println!("compact #{} on level {}", idx, level);

        let mut compact_def;

        if level == 0 {
            let next_level = self.levels[1].clone();
            let targets = cpt_prio.targets.clone();
            compact_def = CompactDef::new(
                idx,
                self.levels[level].clone(),
                next_level,
                cpt_prio,
                targets,
            );
            self.fill_tables_l0(&mut compact_def)?;
        } else {
            let next_level = self.levels[level + 1].clone();
            let targets = cpt_prio.targets.clone();
            compact_def = CompactDef::new(
                idx,
                self.levels[level].clone(),
                next_level,
                cpt_prio,
                targets,
            );
            self.fill_tables(&compact_def)?;
        };

        if let Err(err) = self.run_compact_def(idx, level, &mut compact_def) {
            println!("failed on compaction {:?}", err);
            self.cpt_status.write().unwrap().delete(&compact_def);
        }

        // TODO: will compact_def be used now?

        println!("compaction success");
        self.cpt_status.write().unwrap().delete(&compact_def);

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
        let targets = self.level_targets();
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
            let move_l0_to_front =
                |prios: Vec<CompactionPriority>| match prios.iter().position(|x| x.level == 0) {
                    Some(pos) => {
                        let mut result = vec![];
                        result.push(prios[pos].clone());
                        result.extend_from_slice(&prios[..idx]);
                        result.extend_from_slice(&prios[idx + 1..]);
                        result
                    }
                    _ => prios,
                };

            let run_once = || {
                let mut prios = core.pick_compact_levels();
                if idx == 0 {
                    prios = move_l0_to_front(prios);
                }
                println!("{:?}", prios);

                for p in prios {
                    if idx == 0 && p.level == 0 {
                        // allow worker zero to run level 0
                    } else if p.adjusted < 1.0 {
                        break;
                    }

                    // TODO: handle error
                    if let Err(err) = core.do_compact(idx, p) {
                        println!("error while compaction: {:?}", err);
                    }
                }
            };

            let ticker = tick(Duration::from_millis(50));

            select! {
                recv(ticker) -> _ => run_once(),
                recv(closer.has_been_closed()) -> _ => return
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
