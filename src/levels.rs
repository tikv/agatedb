mod compaction;
mod handler;
#[cfg(test)]
pub(crate) mod tests;

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};
use compaction::{
    get_key_range, get_key_range_single, CompactDef, CompactStatus, CompactionPriority, KeyRange,
    LevelCompactStatus, Targets,
};
use crossbeam_channel::{select, tick, unbounded};
use handler::LevelHandler;
use proto::meta::ManifestChangeSet;
use yatp::task::callback::Handle;

use crate::{
    closer::Closer,
    format::{get_ts, key_with_ts, user_key},
    iterator::{is_deleted_or_expired, IteratorOptions},
    manifest::{new_create_change, new_delete_change, ManifestFile},
    ops::oracle::Oracle,
    opt::build_table_options,
    table::{MergeIterator, TableIterators},
    util::{self, has_any_prefixes, same_key, KeyComparator, COMPARATOR},
    value::{Value, ValuePointer},
    AgateIterator, AgateOptions, Error, Result, Table, TableBuilder,
};

pub(crate) struct Core {
    next_file_id: AtomicU64,

    // `levels[i].level == i` should be ensured
    pub(crate) levels: Vec<Arc<RwLock<LevelHandler>>>,
    opts: AgateOptions,
    orc: Arc<Oracle>,

    cpt_status: RwLock<CompactStatus>,
    manifest: Arc<ManifestFile>,
    //TODO: Add l0_stalls_ms
}

pub struct LevelsController {
    pub(crate) core: Arc<Core>,
}

impl Core {
    fn new(opts: AgateOptions, manifest: Arc<ManifestFile>, orc: Arc<Oracle>) -> Result<Self> {
        assert!(opts.num_level_zero_tables_stall > opts.num_level_zero_tables);

        let mut core = Core {
            next_file_id: AtomicU64::new(0),
            levels: Vec::with_capacity(opts.max_levels),
            opts: opts.clone(),
            orc,
            cpt_status: RwLock::new(CompactStatus::default()),
            manifest: manifest.clone(),
        };

        {
            let mut cpt_status_lock = core.cpt_status.write().unwrap();
            cpt_status_lock.levels.reserve(opts.max_levels);

            for i in 0..opts.max_levels {
                let level_handler = LevelHandler::new(opts.clone(), i);
                core.levels.push(Arc::new(RwLock::new(level_handler)));
                cpt_status_lock.levels.push(LevelCompactStatus::default());
            }
        }

        if opts.in_memory {
            return Ok(core);
        }

        // TODO: Revert to manifest.

        let manifest_data = manifest.manifest_cloned();

        let mut max_file_id = 0;
        let mut tables: Vec<Vec<Table>> = Vec::new();
        tables.resize(opts.max_levels, vec![]);

        // TODO: Parallelly open tables.
        for (id, table_manifest) in manifest_data.tables {
            if id > max_file_id {
                max_file_id = id;
            }

            let table_opts = build_table_options(&opts);
            // TODO: Set compression, data_key, cache.

            let filename = crate::table::new_filename(id, &opts.dir);
            let table = Table::open(&filename, table_opts)?;
            // TODO: Allow checksum mismatch tables.

            tables[table_manifest.level as usize].push(table);
        }

        core.next_file_id.store(max_file_id + 1, Ordering::SeqCst);

        for (i, tables) in tables.into_iter().enumerate() {
            core.levels[i].write().unwrap().init_tables(tables);
        }

        // TODO: Validate core.

        util::sync_dir(&opts.dir)?;

        Ok(core)
    }

    /// Calculates the [`Targets`] for levels in the LSM tree.
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

        // db_size is the size of the last level.
        let mut db_size = self.last_level().read().unwrap().total_size;

        for i in (1..self.levels.len()).rev() {
            let ltarget = adjust(db_size);
            targets.target_size[i] = ltarget;
            if targets.base_level == 0 && ltarget <= self.opts.base_level_size {
                targets.base_level = i;
            }
            db_size /= self.opts.level_size_multiplier as u64;
        }

        let mut tsz = self.opts.base_level_size;

        // Use mem_table_size for Level 0. Because at Level 0, we stop
        // compactions based on the number of tables, not the size of
        // the level. So, having a 1:1 size ratio between memtable size
        // and the size of L0 files is better than churning out 32 files
        // per memtable (assuming 64MB mem_table_size and 2MB base_table_size).
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

    /// Returns [`LevelHandler`] of bottom level.
    fn last_level(&self) -> &Arc<RwLock<LevelHandler>> {
        self.levels.last().unwrap()
    }

    /// Determines which level to compact.
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

        // Add L0 priority based on the number of tables.
        add_priority(
            0,
            self.levels[0].read().unwrap().num_tables() as f64
                / self.opts.num_level_zero_tables as f64,
        );

        for i in 1..self.levels.len() {
            // We must ensure lock order, by only obtain one lock at a time.
            let del_size = {
                let cpt_status = self.cpt_status.read().unwrap();
                cpt_status.levels[i].del_size
            };
            let level = self.levels[i].read().unwrap();
            // There could be inconsistency data which causes `size < 0`.
            // We may safely ignore this situation.
            // TODO: Check if we could make it more stable.
            let size = if del_size <= level.total_size {
                level.total_size - del_size
            } else {
                0
            };
            add_priority(i, size as f64 / targets.target_size[i] as f64);
        }

        assert_eq!(prios.len(), self.levels.len());

        // TODO: Adjust score.

        prios.pop(); // remove last level
        let mut x: Vec<CompactionPriority> = prios.into_iter().filter(|x| x.score > 1.0).collect();
        x.sort_by(|x, y| x.adjusted.partial_cmp(&y.adjusted).unwrap());
        x.reverse();
        x
    }

    /// Checks if the given `tables` overlap with any level from the given `level` onwards.
    fn check_overlap(&self, tables: &[Table], level: usize) -> bool {
        let kr = get_key_range(tables);

        for (idx, lh) in self.levels.iter().enumerate() {
            if idx < level {
                continue;
            }

            let lvl = lh.read().unwrap();
            let (left, right) = lvl.overlapping_tables(&kr);
            drop(lvl);
            if right - left > 0 {
                return true;
            }
        }
        false
    }

    /// Runs a single sub-compaction, iterating over the specified key-range only.
    fn sub_compact(
        self: &Arc<Self>,
        mut iter: Box<TableIterators>,
        kr: &KeyRange,
        compact_def: &CompactDef,
    ) -> Result<Vec<Table>> {
        let has_overlap =
            self.check_overlap(&compact_def.all_tables(), compact_def.next_level_id + 1);

        let discard_ts = self.orc.discard_at_or_below();

        // TODO: Collect stats for GC.

        // TODO: Check if the given key-range overlaps with next_level too much.

        let start_time = std::time::Instant::now();

        // For return.
        let mut tables = vec![];

        let mut last_key = BytesMut::new();
        let mut skip_key = BytesMut::new();
        let mut num_versions = 0;

        let mut num_keys: u64 = 0;
        let mut num_skips: u64 = 0;

        match kr {
            KeyRange::Range { left, right: _ } => {
                if !left.is_empty() {
                    iter.seek(left);
                } else {
                    iter.rewind();
                }
            }
            _ => iter.rewind(),
        }

        while iter.valid() {
            if let KeyRange::Range { left: _, right } = kr {
                if !right.is_empty()
                    && COMPARATOR.compare_key(iter.key(), right) != std::cmp::Ordering::Less
                {
                    break;
                }
            }

            let mut bopts = crate::opt::build_table_options(&self.opts);
            // TODO: Add key registry and cache.

            bopts.table_size = compact_def.targets.file_size[compact_def.next_level_id];
            let mut builder = TableBuilder::new(bopts.clone());
            let mut table_kr = KeyRange::default();
            let mut vp = ValuePointer::default();

            // Do the iteration and add keys to builder.
            while iter.valid() {
                let iter_key = Bytes::copy_from_slice(iter.key());

                // See if we need to skip the prefix.
                if !compact_def.drop_prefixes.is_empty()
                    && has_any_prefixes(&iter_key, &compact_def.drop_prefixes)
                {
                    num_skips += 1;
                    // TODO: update stats of vlog

                    iter.next();
                    continue;
                }

                // See if we need to skip this key.
                if !skip_key.is_empty() {
                    if same_key(&iter_key, &skip_key) {
                        num_skips += 1;
                        // TODO: update stats of vlog

                        iter.next();
                        continue;
                    } else {
                        skip_key.clear();
                    }
                }

                if !same_key(&iter_key, &last_key) {
                    if let KeyRange::Range { left: _, right } = kr {
                        if !right.is_empty()
                            && COMPARATOR.compare_key(&iter_key, right) != std::cmp::Ordering::Less
                        {
                            break;
                        }
                    }

                    // Ensure that all versions of the key are stored in the same
                    // sstable, and not divided across multiple tables at the same
                    // level.
                    if builder.reach_capacity() {
                        break;
                    }

                    last_key.clear();
                    last_key.extend_from_slice(iter.key());
                    num_versions = 0;

                    match &mut table_kr {
                        KeyRange::Range { left, right } => {
                            if left.is_empty() {
                                *left = Bytes::copy_from_slice(&iter_key);
                            }
                            *right = Bytes::copy_from_slice(&last_key);
                        }
                        KeyRange::Inf => todo!(),
                        KeyRange::Empty => {
                            table_kr = KeyRange::Range {
                                left: Bytes::copy_from_slice(&iter_key),
                                right: Bytes::copy_from_slice(&last_key),
                            }
                        }
                    }

                    // TODO: Do range check.
                }

                let vs = iter.value();
                let version = get_ts(&iter_key);

                if version <= discard_ts && vs.meta & crate::value::VALUE_MERGE_ENTRY == 0 {
                    num_versions += 1;

                    let last_valid_version = vs.meta & crate::value::VALUE_DISCARD_EARLIER_VERSIONS
                        != 0
                        || num_versions == self.opts.num_versions_to_keep;

                    let is_expired = is_deleted_or_expired(vs.meta, vs.expires_at);

                    if is_expired || last_valid_version {
                        skip_key = BytesMut::from(&iter_key[..]);
                        #[allow(clippy::if_same_then_else)]
                        if !is_expired && last_valid_version {
                            // do nothing
                        } else if has_overlap {
                            // do nothing
                        } else {
                            num_skips += 1;
                            // TODO: update stats of vlog

                            iter.next();
                            continue;
                        }
                    }
                }

                num_keys += 1;

                if vs.meta & crate::value::VALUE_POINTER != 0 {
                    vp.decode(&vs.value);
                }

                builder.add(&iter_key, &vs, vp.len);

                iter.next();
            } // Finish iteration.

            if builder.is_empty() {
                continue;
            }

            let file_id = self.reserve_file_id();

            let table = if self.opts.in_memory {
                Table::open_in_memory(builder.finish(), file_id, bopts)?
            } else {
                let filename = crate::table::new_filename(file_id, &self.opts.dir);
                Table::create(&filename, builder.finish(), bopts)?
            };

            tables.push(table);
        }

        eprintln!(
            "compactor {}, sub_compact took {} mills, produce {} tables, added {} keys, skipped {} keys",
            compact_def.compactor_id,
            std::time::Instant::now()
                .duration_since(start_time)
                .as_millis(),
            tables.len(),
            num_keys,
            num_skips
        );

        Ok(tables)
    }

    /// Merges top tables and bot tables to form a list of new tables.
    fn compact_build_tables(
        self: &Arc<Self>,
        level: usize,
        compact_def: &CompactDef,
        _pool: &yatp::ThreadPool<yatp::task::callback::TaskCell>,
    ) -> Result<Vec<Table>> {
        // TODO: this implementation is very very trivial

        let mut valid = vec![];

        for table in &compact_def.bot {
            // TODO: Check valid.
            valid.push(table.clone());
        }

        let valid = Arc::new(valid);

        let make_iterator = move |compact_def: &CompactDef, valid: &[Table]| {
            let mut iters = vec![];

            if level == 0 {
                for table in compact_def.top.iter().rev() {
                    iters.push(TableIterators::from(
                        table.new_iterator(crate::table::ITERATOR_NOCACHE),
                    ));
                }
            } else if !compact_def.top.is_empty() {
                assert_eq!(compact_def.top.len(), 1);
                iters.push(TableIterators::from(
                    compact_def.top[0].new_iterator(crate::table::ITERATOR_NOCACHE),
                ));
            }

            // TODO: Use ConcatIterator.
            for table in valid {
                iters.push(TableIterators::from(
                    table.new_iterator(crate::table::ITERATOR_NOCACHE),
                ));
            }
            iters
        };

        let mut new_tables = vec![];

        // TODO: Improve it.

        let (tx, rx) = unbounded();
        let compact_def = Arc::new(compact_def.clone());
        for kr in &compact_def.splits {
            let kr = kr.clone();
            let compact_def = compact_def.clone();
            let this = self.clone();
            let valid = valid.clone();
            let tx = tx.clone();
            // TODO: currently, the thread will never wake up when using yatp. So we use std::thread instead.
            std::thread::spawn(move || {
                let iters = make_iterator(&compact_def, &valid);
                if iters.is_empty() {
                    // TODO: iters should not be empty
                    tx.send(None).ok();
                }
                tx.send(Some(this.sub_compact(
                    MergeIterator::from_iterators(iters, false),
                    &kr,
                    &compact_def,
                )))
                .ok();
            });
        }

        for table in rx.iter().take(compact_def.splits.len()).flatten() {
            new_tables.append(&mut table?);
        }

        util::sync_dir(&self.opts.dir)?;

        new_tables.sort_by(|x, y| COMPARATOR.compare_key(x.biggest(), y.biggest()));

        Ok(new_tables)
    }

    /// Can allow us to run multiple sub-compactions in parallel across the split key ranges.
    fn add_splits(&self, compact_def: &mut CompactDef) {
        compact_def.splits.clear();

        // Pick one every 3 tables.
        const N: usize = 3;

        // Assume this_range is never inf.
        let mut skr = compact_def.this_range.clone();
        skr = skr.extend(&compact_def.next_range);

        let mut add_range = |splits: &mut Vec<KeyRange>, right| {
            match &mut skr {
                KeyRange::Range {
                    left: _,
                    right: skr_right,
                } => {
                    *skr_right = right;
                }
                KeyRange::Inf => {
                    unreachable!()
                }
                KeyRange::Empty => {
                    skr = KeyRange::Range {
                        left: Bytes::new(),
                        right,
                    }
                }
            }

            splits.push(skr.clone());

            match &mut skr {
                KeyRange::Range { left, right } => {
                    *left = right.clone();
                }
                _ => unreachable!(),
            }
        };

        for (i, table) in compact_def.bot.iter().enumerate() {
            if i == compact_def.bot.len() - 1 {
                add_range(&mut compact_def.splits, Bytes::new());
                return;
            }

            if i % N == N - 1 {
                let biggest = table.biggest();
                // TODO: Check this.
                let mut buf = BytesMut::with_capacity(biggest.len() + 8);
                buf.put(user_key(biggest));
                let right = key_with_ts(buf, std::u64::MAX);
                add_range(&mut compact_def.splits, right);
            }
        }
    }

    fn fill_tables_l0_to_l0(&self, compact_def: &mut CompactDef) -> Result<()> {
        if compact_def.compactor_id != 0 {
            return Err(Error::CustomError(
                "only compactor zero can compact L0 to L0".to_string(),
            ));
        }

        // TODO: should compact_def be mutable?
        compact_def.next_level = self.levels[0].clone();
        compact_def.next_level_id = 0;
        compact_def.next_range = KeyRange::default();
        compact_def.bot = vec![];

        let this_level = compact_def.this_level.read().unwrap();
        // Since next_level and this_level are same L0, do not need to acquire lock of next level.
        // TODO: Don't lock cpt_status through this function.
        let mut cpt_status = self.cpt_status.write().unwrap();

        let mut out = vec![];

        for table in this_level.tables.iter() {
            if table.size() >= 2 * compact_def.targets.file_size[0] {
                // file already big, don't include it
                continue;
            }

            // TODO: Add created at logic.

            if cpt_status.tables.contains(&table.id()) {
                continue;
            }
            out.push(table.clone());
        }

        if out.len() < 4 {
            return Err(Error::CustomError("not enough table to merge".to_string()));
        }

        compact_def.this_range = KeyRange::Inf;
        compact_def.top = out;

        // Avoid any other L0 -> Lbase from happening, while this is going on.
        // TODO: ?
        cpt_status.levels[this_level.level]
            .ranges
            .push(KeyRange::Inf);
        for table in compact_def.top.iter() {
            assert!(!cpt_status.tables.insert(table.id()));
        }

        // Make the output always one file to decreases the L0 table stalls and
        // improves the performance.
        compact_def.targets.file_size[0] = std::u32::MAX as u64;

        Ok(())
    }

    fn fill_tables_l0_to_lbase(&self, compact_def: &mut CompactDef) -> Result<()> {
        if compact_def.next_level_id == 0 {
            panic!("base level can't be zero");
        }

        let this_level = compact_def.this_level.read().unwrap();
        let next_level = compact_def.next_level.read().unwrap();

        if compact_def.prios.adjusted > 0.0 && compact_def.prios.adjusted < 1.0 {
            return Err(Error::CustomError(
                "score less than 1.0, not compact to Lbase".to_string(),
            ));
        }

        if this_level.tables.is_empty() {
            return Err(Error::CustomError("no table in this level".to_string()));
        }

        let mut out = vec![];

        if !compact_def.drop_prefixes.is_empty() {
            out = this_level.tables.clone();
        } else {
            let mut kr = KeyRange::default();
            // Start from the oldest file first.
            for table in this_level.tables.iter() {
                let dkr = get_key_range_single(table);
                if kr.overlaps_with(&dkr) {
                    out.push(table.clone());
                    kr = kr.extend(&dkr);
                } else {
                    break;
                }
            }
        }
        compact_def.this_range = get_key_range(&compact_def.top);
        compact_def.top = out;

        let (left, right) = next_level.overlapping_tables(&compact_def.this_range);

        compact_def.bot = next_level.tables[left..right].to_vec();

        if compact_def.bot.is_empty() {
            compact_def.next_range = compact_def.this_range.clone();
        } else {
            compact_def.next_range = get_key_range(&compact_def.bot);
        }

        self.cpt_status
            .write()
            .unwrap()
            .compare_and_add(compact_def)?;

        Ok(())
    }

    /// Try to fill tables from L0 to be compacted with Lbase. If it can not
    /// do that, it would try to compact tables from L0 -> L0.
    fn fill_tables_l0(&self, compact_def: &mut CompactDef) -> Result<()> {
        if self.fill_tables_l0_to_lbase(compact_def).is_err() {
            return self.fill_tables_l0_to_l0(compact_def);
        }
        Ok(())
    }

    fn fill_tables(&self, compact_def: &mut CompactDef) -> Result<()> {
        let this_level = compact_def.this_level.read().unwrap();
        let next_level = compact_def.next_level.read().unwrap();

        let tables = &this_level.tables;

        if tables.is_empty() {
            return Err(Error::CustomError("no tables to compact".to_string()));
        }

        // TODO: Sort tables by heuristic.

        // TODO: Don't hold cpt_status write lock for long time.
        let mut cpt_status = self.cpt_status.write().unwrap();

        for table in tables {
            compact_def.this_size = table.size();
            compact_def.this_range = get_key_range_single(table);
            // If we're already compacting this range, don't do anything.
            // TODO: ?
            if cpt_status.overlaps_with(compact_def.this_level_id, &compact_def.this_range) {
                continue;
            }
            compact_def.top = vec![table.clone()];
            let (left, right) = next_level.overlapping_tables(&compact_def.this_range);

            // TODO: Try to fix it.
            if right < left {
                eprintln!(
                    "right {} is less than left {} in overlapping_tables for current level {}, next level {}, key_range {:?}",
                    right,
                    left,
                    compact_def.this_level_id,
                    compact_def.next_level_id,
                    compact_def.this_range
                );
                continue;
            }

            compact_def.bot = next_level.tables[left..right].to_vec();

            if compact_def.bot.is_empty() {
                compact_def.bot = vec![];
                compact_def.next_range = compact_def.this_range.clone();
                if cpt_status.compare_and_add(compact_def).is_err() {
                    continue;
                }
                return Ok(());
            }

            compact_def.next_range = get_key_range(&compact_def.bot);

            if cpt_status.overlaps_with(compact_def.next_level_id, &compact_def.next_range) {
                continue;
            }

            if cpt_status.compare_and_add(compact_def).is_err() {
                continue;
            }

            return Ok(());
        }

        Err(Error::CustomError("no table to fill".to_string()))
    }

    fn run_compact_def(
        self: &Arc<Self>,
        _idx: usize,
        level: usize,
        compact_def: &mut CompactDef,
        pool: Arc<yatp::ThreadPool<yatp::task::callback::TaskCell>>,
    ) -> Result<()> {
        if compact_def.targets.file_size.is_empty() {
            return Err(Error::CustomError("targets not set".to_string()));
        }

        let this_level = compact_def.this_level.clone();
        let next_level = compact_def.next_level.clone();
        let this_level_id = compact_def.this_level_id;
        let next_level_id = compact_def.next_level_id;

        assert_eq!(compact_def.splits.len(), 0);

        if this_level_id == 0 && next_level_id == 0 {
            // Don't do anything for L0 -> L0.
        } else {
            self.add_splits(compact_def);
        }

        if compact_def.splits.is_empty() {
            compact_def.splits.push(KeyRange::default());
        }

        let new_tables = self.compact_build_tables(level, compact_def, &pool)?;

        let change_set = build_change_set(compact_def, &new_tables);

        // We write to the manifest _before_ we delete files (and _after_ we created files)
        self.manifest.add_changes(change_set.changes)?;

        // TODO: Re-examine this.
        if this_level_id != next_level_id {
            let mut this_level = this_level.write().unwrap();
            let mut next_level = next_level.write().unwrap();
            this_level.delete_tables(&compact_def.top)?;
            next_level.replace_tables(&compact_def.bot, &new_tables)?;
        } else {
            let mut this_level = this_level.write().unwrap();
            this_level.delete_tables(&compact_def.top)?;
            this_level.replace_tables(&compact_def.bot, &new_tables)?;
        }

        // TODO: Add log.

        Ok(())
    }

    // Picks some tables on that level and compact it to next level.
    fn do_compact(
        self: &Arc<Self>,
        idx: usize,
        mut cpt_prio: CompactionPriority,
        pool: Arc<yatp::ThreadPool<yatp::task::callback::TaskCell>>,
    ) -> Result<()> {
        let level = cpt_prio.level;

        assert!(level + 1 < self.opts.max_levels);

        if cpt_prio.targets.base_level == 0 {
            cpt_prio.targets = self.level_targets();
        }

        let mut compact_def;

        if level == 0 {
            let targets = cpt_prio.targets.clone();
            let base_level = targets.base_level;
            let next_level = self.levels[base_level].clone();
            compact_def = CompactDef::new(
                idx,
                self.levels[level].clone(),
                level,
                next_level,
                base_level,
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
                level,
                next_level,
                level + 1,
                cpt_prio,
                targets,
            );
            self.fill_tables(&mut compact_def)?;
        };
        if let Err(err) = self.run_compact_def(idx, level, &mut compact_def, pool) {
            eprintln!("failed on compaction {:?}", err);
            self.cpt_status.write().unwrap().delete(&compact_def);
        }

        // TODO: will compact_def be used now?

        eprintln!(
            "compactor #{} on level {} success",
            idx, compact_def.this_level_id
        );
        self.cpt_status.write().unwrap().delete(&compact_def);

        Ok(())
    }

    fn add_l0_table(&self, table: Table) -> Result<()> {
        if !self.opts.in_memory {
            // Update the manifest _before_ the table becomes part of a level_handler.
            self.manifest
                .add_changes(vec![new_create_change(table.id(), 0, 0)])?;
        }

        while !self.levels[0].write()?.try_add_l0_table(table.clone()) {
            let time_start = std::time::Instant::now();

            while self.levels[0].read()?.num_tables() >= self.opts.num_level_zero_tables_stall {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }

            let current = std::time::Instant::now();
            let duration = current.duration_since(time_start);

            if duration.as_millis() > 1000 {
                eprintln!("L0 was stalled for {} ms", duration.as_millis());
            }

            // TODO: Update l0_stalls_ms.
        }

        Ok(())
    }

    /// Searches for a given key in all the levels of the LSM tree.
    fn get(&self, key: &Bytes, mut max_value: Value, start_level: usize) -> Result<Value> {
        // TODO: Check if is closed.

        let version = get_ts(key);

        for (level, handler) in self.levels.iter().enumerate() {
            if level < start_level {
                continue;
            }
            match handler.read()?.get(key) {
                Ok(value) => {
                    if value.value.is_empty() && value.meta == 0 {
                        continue;
                    }
                    if value.version == version {
                        return Ok(value);
                    }
                    if max_value.version < value.version {
                        max_value = value;
                    }
                }
                Err(err) => {
                    return Err(Error::CustomError(format!(
                        "get key: {:?}, {:?}",
                        Bytes::copy_from_slice(key),
                        err
                    )));
                }
            }
        }

        Ok(max_value)
    }

    fn reserve_file_id(&self) -> u64 {
        self.next_file_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

impl LevelsController {
    pub fn new(opts: AgateOptions, manifest: Arc<ManifestFile>, orc: Arc<Oracle>) -> Result<Self> {
        Ok(Self {
            core: Arc::new(Core::new(opts, manifest, orc)?),
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

    pub fn start_compact(
        &self,
        closer: Closer,
        pool: Arc<yatp::ThreadPool<yatp::task::callback::TaskCell>>,
    ) {
        for i in 0..self.core.opts.num_compactors {
            self.run_compactor(i, closer.clone(), pool.clone());
        }
    }

    fn run_compactor(
        &self,
        idx: usize,
        closer: Closer,
        pool: Arc<yatp::ThreadPool<yatp::task::callback::TaskCell>>,
    ) {
        let core = self.core.clone();
        let pool_c = pool.clone();

        pool.spawn(move |_: &mut Handle<'_>| {
            let move_l0_to_front =
                |prios: Vec<CompactionPriority>| match prios.iter().position(|x| x.level == 0) {
                    Some(pos) => {
                        let mut result = vec![prios[pos].clone()];
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

                for p in prios {
                    if idx == 0 && p.level == 0 {
                        // Allow worker zero to run level 0.
                    } else if p.adjusted < 1.0 {
                        break;
                    }

                    if let Err(_err) = core.do_compact(idx, p, pool_c.clone()) {
                        // TODO: Handle error.
                    }
                }
            };

            let ticker = tick(Duration::from_millis(50));

            loop {
                select! {
                    recv(ticker) -> _ => run_once(),
                    recv(closer.get_receiver()) -> _ => return
                }
            }
        });
    }

    /// Appends iterators to an array of iterators, for merging.
    pub(crate) fn append_iterators(&self, iters: &mut Vec<TableIterators>, opts: &IteratorOptions) {
        // Iterate the levels from 0 on upward, to avoid missing
        // data when there's a compaction.
        for level in &self.core.levels {
            level.read().unwrap().append_iterators(iters, opts);
        }
    }
}

fn build_change_set(compact_def: &CompactDef, new_tables: &[Table]) -> ManifestChangeSet {
    let mut changes = vec![];

    for table in new_tables {
        // TODO: data key id
        changes.push(new_create_change(table.id(), compact_def.next_level_id, 0));
    }
    for table in &compact_def.top {
        if !table.is_in_memory() {
            changes.push(new_delete_change(table.id()));
        }
    }
    for table in &compact_def.bot {
        changes.push(new_delete_change(table.id()));
    }

    ManifestChangeSet { changes }
}
