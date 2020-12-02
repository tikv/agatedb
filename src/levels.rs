mod compaction;
mod handler;

use compaction::{
    get_key_range, get_key_range_single, CompactDef, CompactStatus, CompactionPriority, KeyRange,
    LevelCompactStatus, Targets,
};
use handler::LevelHandler;

use proto::meta::ManifestChangeSet;

use crate::format::{get_ts, key_with_ts, user_key};
use crate::manifest::{new_create_change, new_delete_change, ManifestFile};
use crate::opt::build_table_options;
use crate::table::{MergeIterator, TableIterators};
use crate::util::{has_any_prefixes, same_key, KeyComparator, COMPARATOR};
use crate::value::{Value, ValuePointer};
use crate::AgateIterator;
use crate::TableBuilder;
use crate::{closer::Closer, iterator::IteratorOptions};
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
    manifest: Arc<ManifestFile>,
}

pub struct LevelsController {
    core: Arc<Core>,
}

impl Core {
    fn new(opts: AgateOptions, manifest: Arc<ManifestFile>) -> Result<Self> {
        let mut levels = vec![];
        let mut cpt_status_levels = vec![];

        // TODO: revert to manifest

        let manifest_data = manifest.manifest_cloned();

        let mut max_file_id = 0;
        let mut tables: Vec<Vec<Table>> = vec![];
        let mut num_opened = 0;
        tables.resize(opts.max_levels, vec![]);

        println!("{:?}", manifest_data);

        // TODO: parallel open tables
        for (id, table_manifest) in manifest_data.tables {
            if id > max_file_id {
                max_file_id = id;
            }
            let table_opts = build_table_options(&opts);
            // TODO: set compression, data_key, cache
            let filename = crate::table::new_filename(id, &opts.path);
            let table = Table::open(&filename, table_opts)?;
            // TODO: allow checksum mismatch tables
            tables[table_manifest.level as usize].push(table);
            num_opened += 1;
        }

        println!("{} tables opened", num_opened);

        for (i, tables) in tables.into_iter().enumerate() {
            let mut level = LevelHandler::new(opts.clone(), i);
            level.init_tables(tables);
            levels.push(Arc::new(RwLock::new(level)));

            cpt_status_levels.push(LevelCompactStatus::default());
        }

        let lvctl = Self {
            next_file_id: AtomicU64::new(max_file_id + 1),
            levels,
            opts: opts.clone(),
            cpt_status: RwLock::new(CompactStatus {
                levels: cpt_status_levels,
                tables: HashSet::new(),
            }),
            manifest,
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
            self.manifest
                .add_changes(vec![new_create_change(table.id(), 0, 0)])?;
        }

        while !self.levels[0].write()?.try_add_l0_table(table.clone()) {
            println!("L0 stalled");
            // TODO: enhance stall logic
            std::thread::yield_now();
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

        self.cpt_status
            .write()
            .unwrap()
            .compare_and_add(&compact_def)?;

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
        compact_def.next_level_id = 0;
        compact_def.next_range = KeyRange::default();
        compact_def.bot = vec![];

        let this_level = compact_def.this_level.read().unwrap();
        // next_level and this_level is the same L0, do not need to acquire lock of next level
        // TODO: don't hold cpt_status through this function
        let mut cpt_status = self.cpt_status.write().unwrap();

        let mut out = vec![];
        // let now = std::time::Instant::now();

        for table in this_level.tables.iter() {
            if table.size() > 2 * compact_def.targets.file_size[0] {
                // file already big, don't include it
                continue;
            }
            // TODO: created at logic
            if cpt_status.tables.contains(&table.id()) {
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

        compact_def.targets.file_size[0] = std::u32::MAX as u64;

        Ok(())
    }

    fn fill_tables_l0(&self, compact_def: &mut CompactDef) -> Result<()> {
        if let Err(err) = self.fill_tables_l0_to_lbase(compact_def) {
            println!("error when fill L0 to Lbase {:?}", err);
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

        // TODO: sort tables by heuristic

        // TODO: don't hold cpt_status write lock for long time
        let mut cpt_status = self.cpt_status.write().unwrap();

        for table in tables {
            compact_def.this_size = table.size();
            compact_def.this_range = get_key_range_single(table);
            // if we're already compacting this range, don't do anything
            if cpt_status.overlaps_with(compact_def.this_level_id, &compact_def.this_range) {
                continue;
            }
            compact_def.top = vec![table.clone()];
            let (left, right) = next_level.overlapping_tables(&compact_def.this_range);

            if right < left {
                println!("right {} is less than left {} in overlapping_tables for current level {}, next level {}, key_range {:?}",
                    right, left, compact_def.this_level_id,
                    compact_def.next_level_id, compact_def.this_range);
                continue;
            }

            compact_def.bot = next_level.tables[left..right].to_vec();

            if compact_def.bot.is_empty() {
                compact_def.bot = vec![];
                compact_def.next_range = compact_def.this_range.clone();
                if let Err(_) = cpt_status.compare_and_add(compact_def) {
                    continue;
                }
                return Ok(());
            }

            compact_def.next_range = get_key_range(&compact_def.bot);

            if cpt_status.overlaps_with(compact_def.next_level_id, &compact_def.next_range) {
                continue;
            }

            if let Err(_) = cpt_status.compare_and_add(compact_def) {
                continue;
            }

            return Ok(());
        }

        Err(Error::CustomError("no table to fill".to_string()))
    }

    fn run_compact_def(
        &self,
        idx: usize,
        level: usize,
        compact_def: &mut CompactDef,
    ) -> Result<()> {
        println!("compact def {} running...", idx);

        if compact_def.targets.file_size.len() == 0 {
            return Err(Error::CustomError("targets not set".to_string()));
        }

        let this_level = compact_def.this_level.clone();
        let next_level = compact_def.next_level.clone();
        let this_level_id = compact_def.this_level_id;
        let next_level_id = compact_def.next_level_id;

        assert_eq!(compact_def.splits.len(), 0);

        if this_level_id == 0 && next_level_id == 0 {
        } else {
            self.add_splits(compact_def);
        }

        if compact_def.splits.is_empty() {
            compact_def.splits.push(KeyRange::default());
        }

        let new_tables = self.compact_build_tables(level, compact_def)?;

        let change_set = build_change_set(&compact_def, &new_tables);
        self.manifest.add_changes(change_set.changes)?;

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

        // TODO: logging

        Ok(())
    }

    fn compact_build_tables(&self, level: usize, compact_def: &CompactDef) -> Result<Vec<Table>> {
        // TODO: this implementation is very very trivial

        // TODO: check prefix

        let mut valid = vec![];

        for table in &compact_def.bot {
            // TODO: check valid
            valid.push(table.clone());
        }

        let make_iterator = || {
            let mut iters = vec![];

            if level == 0 {
                for table in compact_def.top.iter().rev() {
                    iters.push(Box::new(TableIterators::from(
                        table.new_iterator(crate::table::ITERATOR_NOCACHE),
                    )));
                }
            } else if compact_def.top.len() > 0 {
                assert_eq!(compact_def.top.len(), 1);
                iters.push(Box::new(TableIterators::from(
                    compact_def.top[0].new_iterator(crate::table::ITERATOR_NOCACHE),
                )));
            }
            for table in &valid {
                iters.push(Box::new(TableIterators::from(
                    table.new_iterator(crate::table::ITERATOR_NOCACHE),
                )));
            }
            iters
        };

        let mut new_tables = vec![];

        // TODO: multi-thread compaction

        for kr in &compact_def.splits {
            let iters = make_iterator();
            if iters.is_empty() {
                // TODO: iters should not be empty
                continue;
            }
            new_tables.append(&mut self.sub_compact(
                MergeIterator::from_iterators(iters, false),
                kr,
                compact_def,
            )?);
        }

        // TODO: sync dir
        new_tables.sort_by(|x, y| COMPARATOR.compare_key(x.biggest(), y.biggest()));

        Ok(new_tables)
    }

    fn sub_compact(
        &self,
        mut iter: Box<TableIterators>,
        kr: &KeyRange,
        compact_def: &CompactDef,
    ) -> Result<Vec<Table>> {
        // TODO: check overlap and process transaction
        let mut tables = vec![];

        if kr.left.len() > 0 {
            iter.seek(&kr.left);
        } else {
            iter.rewind();
        }

        let mut skip_key = BytesMut::new();
        let mut last_key = BytesMut::new();
        let mut num_builds = 0;
        let mut num_versions = 0;

        while iter.valid() {
            if kr.right.len() > 0 {
                if COMPARATOR.compare_key(iter.key(), &kr.right) != std::cmp::Ordering::Less {
                    break;
                }
            }

            let mut bopts = crate::opt::build_table_options(&self.opts);
            bopts.table_size = compact_def.targets.file_size[compact_def.next_level_id];
            let mut builder = TableBuilder::new(bopts.clone());
            let mut table_kr = KeyRange::default();
            let mut vp = ValuePointer::default();

            while iter.valid() {
                if !compact_def.drop_prefixes.is_empty()
                    && has_any_prefixes(iter.key(), &compact_def.drop_prefixes)
                {
                    // TODO: update stats of vlog
                }

                if !skip_key.is_empty() {
                    if same_key(iter.key(), &skip_key) {
                        // update stats of vlog
                    } else {
                        skip_key.clear();
                    }
                }

                if !same_key(iter.key(), &last_key) {
                    if !kr.right.is_empty()
                        && COMPARATOR.compare_key(iter.key(), &kr.right) != std::cmp::Ordering::Less
                    {
                        break;
                    }

                    if builder.reach_capacity() {
                        break;
                    }

                    last_key.clear();
                    last_key.extend_from_slice(iter.key());

                    num_versions = 0;

                    if table_kr.left.is_empty() {
                        // TODO: use bytes mut and assemble it later
                        table_kr.left = Bytes::copy_from_slice(iter.key());
                    }
                    table_kr.right = Bytes::copy_from_slice(&last_key);

                    // TODO: range check
                }

                let vs = iter.value();
                let version = get_ts(iter.key());

                // TODO: check ts-related properties

                if vs.meta & crate::value::VALUE_POINTER != 0 {
                    vp.decode(&vs.value);
                }

                builder.add(&Bytes::copy_from_slice(iter.key()), vs, vp.len);

                iter.next();
            }

            if builder.is_empty() {
                continue;
            }

            let table;
            let file_id = self.reserve_file_id();

            if self.opts.in_memory {
                table = Table::open_in_memory(builder.finish(), file_id, bopts)?;
            } else {
                let filename = crate::table::new_filename(file_id, &self.opts.path);
                table = Table::create(&filename, builder.finish(), bopts)?;
            }

            tables.push(table);
        }

        Ok(tables)
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
        if let Err(err) = self.run_compact_def(idx, level, &mut compact_def) {
            println!("failed on compaction {:?}", err);
            self.cpt_status.write().unwrap().delete(&compact_def);
        }

        // TODO: will compact_def be used now?

        println!("compaction #{} success", idx);
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

        for i in (1..self.levels.len()).rev() {
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

        // println!("{:?}", targets);

        for i in 1..self.levels.len() {
            let del_size = cpt_status.levels[i].del_size;
            let level = self.levels[i].read().unwrap();
            let size = level.total_size - del_size;
            add_priority(i, size as f64 / targets.target_size[i] as f64);
        }

        assert_eq!(prios.len(), self.levels.len());

        // TODO: adjust score

        prios.pop(); // remove last level
        let mut x: Vec<CompactionPriority> = prios.into_iter().filter(|x| x.score > 1.0).collect();
        x.sort_by(|x, y| x.adjusted.partial_cmp(&y.adjusted).unwrap());
        x.reverse();
        x
    }
}

impl LevelsController {
    pub fn new(opts: AgateOptions, manifest: Arc<ManifestFile>) -> Result<Self> {
        Ok(Self {
            core: Arc::new(Core::new(opts, manifest)?),
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

                // println!("{:?}", prios);

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

            loop {
                select! {
                    recv(ticker) -> _ => run_once(),
                    recv(closer.has_been_closed()) -> _ => return
                }
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

    // TODO: add iterator options
    pub(crate) fn append_iterators(&self, iters: &mut Vec<TableIterators>, opts: &IteratorOptions) {
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

#[cfg(test)]
pub(crate) mod tests {
    use super::LevelsController;

    pub fn helper_dump_levels(lvctl: &LevelsController) {
        for level in &lvctl.core.levels {
            let level = level.read().unwrap();
            println!("--- Level {} ---", level.level);
            for table in &level.tables {
                println!(
                    "#{} ({:?} - {:?}, {})",
                    table.id(),
                    table.smallest(),
                    table.biggest(),
                    table.size()
                );
            }
        }
    }
}
