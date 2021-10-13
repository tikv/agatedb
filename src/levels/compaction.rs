use std::collections::HashSet;

use std::sync::{Arc, RwLock};

use bytes::{Bytes, BytesMut};

use crate::format::{key_with_ts, user_key};
use crate::levels::handler::LevelHandler;
use crate::util::{ComparableRecord, KeyComparator, KeyRange, COMPARATOR};
use crate::{Error, Result, Table};

#[derive(Default)]
pub struct LevelCompactStatus {
    pub ranges: Vec<KeyRange>,
    pub del_size: u64,
}

impl LevelCompactStatus {
    pub fn remove(&mut self, dst: &KeyRange) -> bool {
        let prev_ranges_len = self.ranges.len();
        // TODO: remove in place
        self.ranges = self.ranges.iter().filter(|x| x != &dst).cloned().collect();

        prev_ranges_len != self.ranges.len()
    }

    pub fn overlaps_with(&self, dst: &KeyRange) -> bool {
        for r in self.ranges.iter() {
            if r.overlaps_with(dst) {
                return true;
            }
        }
        false
    }
}

pub struct CompactStatus {
    pub levels: Vec<LevelCompactStatus>,
    pub tables: HashSet<u64>,
}

#[derive(Clone)]
pub struct CompactDef {
    pub compactor_id: usize,
    pub this_level: Arc<RwLock<Box<dyn LevelHandler>>>,
    pub next_level: Arc<RwLock<Box<dyn LevelHandler>>>,
    pub this_level_id: usize,
    pub next_level_id: usize,
    pub targets: Targets,
    pub prios: CompactionPriority,

    pub this_range: KeyRange,
    pub next_range: KeyRange,
    pub splits: Vec<KeyRange>,

    pub top: Vec<Table>,
    pub bot: Vec<Table>,

    pub this_size: u64,

    pub drop_prefixes: Vec<Bytes>,
}

impl CompactDef {
    pub fn new(
        compactor_id: usize,
        this_level: Arc<RwLock<Box<dyn LevelHandler>>>,
        this_level_id: usize,
        next_level: Arc<RwLock<Box<dyn LevelHandler>>>,
        next_level_id: usize,
        prios: CompactionPriority,
        targets: Targets,
    ) -> Self {
        Self {
            compactor_id,
            this_level,
            next_level,
            this_level_id,
            next_level_id,
            this_range: KeyRange::default(),
            next_range: KeyRange::default(),
            splits: vec![],
            this_size: 0,
            drop_prefixes: vec![],
            top: vec![],
            bot: vec![],
            targets,
            prios,
        }
    }

    pub fn all_tables(&self) -> Vec<Table> {
        let mut tables = self.top.clone();
        tables.append(&mut self.bot.clone());
        tables
    }
}

impl CompactStatus {
    pub fn delete(&mut self, compact_def: &CompactDef) {
        // TODO: level is immutable, we could access it without read
        let this_level_id = compact_def.this_level_id;
        assert!(this_level_id < self.levels.len() - 1);

        let this_level = &mut self.levels[this_level_id];
        let next_level_id = compact_def.next_level_id;

        this_level.del_size -= compact_def.this_size;
        let mut found = this_level.remove(&compact_def.this_range);
        drop(this_level);
        if !compact_def.next_range.is_empty() {
            let next_level = &mut self.levels[next_level_id];
            found = next_level.remove(&compact_def.next_range) && found;
        }

        if !found {
            let this = compact_def.this_range.clone();
            let next = compact_def.next_range.clone();
            eprintln!("looking for {:?} in this level", this);
            eprintln!("looking for {:?} in next level", next);
            panic!("key range not found");
        }

        for table in compact_def.top.iter() {
            assert!(self.tables.get(&table.id()).is_some());
            // TODO: delete table
        }
    }

    pub fn compare_and_add(&mut self, compact_def: &CompactDef) -> Result<()> {
        let tl = compact_def.this_level_id;
        assert!(tl < self.levels.len() - 1);
        let this_level = compact_def.this_level_id;
        let next_level = compact_def.next_level_id;
        if self.levels[this_level].overlaps_with(&compact_def.this_range) {
            return Err(Error::CustomError(format!(
                "{:?} overlap with this level {} {:?}",
                compact_def.this_range, compact_def.this_level_id, self.levels[this_level].ranges
            )));
        }
        if self.levels[next_level].overlaps_with(&compact_def.next_range) {
            return Err(Error::CustomError(format!(
                "{:?} overlap with next level {} {:?}",
                compact_def.next_range, compact_def.next_level_id, self.levels[next_level].ranges
            )));
        }

        self.levels[this_level]
            .ranges
            .push(compact_def.this_range.clone());
        self.levels[next_level]
            .ranges
            .push(compact_def.next_range.clone());

        self.levels[this_level].del_size += compact_def.this_size;

        for table in compact_def.top.iter() {
            assert!(self.tables.insert(table.id()));
        }

        for table in compact_def.bot.iter() {
            assert!(self.tables.insert(table.id()));
        }

        Ok(())
    }

    pub fn overlaps_with(&self, level: usize, this: &KeyRange) -> bool {
        let this_level = &self.levels[level];
        this_level.overlaps_with(this)
    }
}

#[derive(Clone, Debug)]
pub struct CompactionPriority {
    pub level: usize,
    pub score: f64,
    pub adjusted: f64,
    pub drop_prefixes: Vec<Bytes>,
    pub targets: Targets,
}

#[derive(Clone, Debug)]
pub struct Targets {
    pub base_level: usize,
    pub target_size: Vec<u64>,
    pub file_size: Vec<u64>,
}

impl Targets {
    pub fn new() -> Self {
        Self {
            base_level: 0,
            target_size: vec![],
            file_size: vec![],
        }
    }
}

pub fn get_key_range(tables: &[Table]) -> KeyRange {
    if tables.is_empty() {
        return KeyRange::default();
    }

    let mut smallest = tables[0].smallest().clone();
    let mut biggest = tables[0].largest().clone();

    for i in 1..tables.len() {
        if COMPARATOR.compare_key(tables[i].smallest(), &smallest) == std::cmp::Ordering::Less {
            smallest = tables[i].smallest().clone();
        }
        if COMPARATOR.compare_key(tables[i].largest(), &biggest) == std::cmp::Ordering::Greater {
            biggest = tables[i].largest().clone();
        }
    }
    let mut smallest_buf = BytesMut::with_capacity(smallest.len() + 8);
    let mut biggest_buf = BytesMut::with_capacity(biggest.len() + 8);
    smallest_buf.extend_from_slice(user_key(&smallest));
    biggest_buf.extend_from_slice(user_key(&biggest));
    return KeyRange::new(
        key_with_ts(smallest_buf, std::u64::MAX),
        key_with_ts(biggest_buf, 0),
    );
}
