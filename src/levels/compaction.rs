use std::collections::HashMap;

use std::sync::{Arc, RwLock};

use bytes::Bytes;

use super::LevelHandler;
use crate::Table;

#[derive(PartialEq, Clone)]
pub enum KeyRange {
    Bound { left: Bytes, right: Bytes },
    Inf,
}

impl KeyRange {
    pub fn is_empty(&self) -> bool {
        match self {
            KeyRange::Inf => false,
            KeyRange::Bound { left, right } => left.is_empty() && right.is_empty(),
        }
    }

    pub fn inf() -> Self {
        KeyRange::Inf
    }

    pub fn new(left: Bytes, right: Bytes) -> Self {
        KeyRange::Bound { left, right }
    }
}

impl Default for KeyRange {
    fn default() -> Self {
        KeyRange::Bound {
            left: Bytes::new(),
            right: Bytes::new(),
        }
    }
}

#[derive(Default)]
pub struct LevelCompactStatus {
    pub ranges: Vec<KeyRange>,
    pub del_size: u64,
}

impl LevelCompactStatus {
    pub fn remove(&mut self, dst: KeyRange) -> bool {
        let prev_ranges_len = self.ranges.len();
        self.ranges = self.ranges.into_iter().filter(|x| x != &dst).collect();

        prev_ranges_len != self.ranges.len()
    }
}

pub struct CompactStatus {
    pub levels: Vec<LevelCompactStatus>,
    pub tables: HashMap<u64, ()>,
}

pub struct CompactDef {
    pub compactor_id: usize,
    pub this_level: Arc<RwLock<LevelHandler>>,
    pub next_level: Arc<RwLock<LevelHandler>>,

    pub this_range: KeyRange,
    pub next_range: KeyRange,
    pub splits: Vec<KeyRange>,

    pub top: Vec<Table>,
    pub bot: Vec<Table>,

    pub this_size: u64,

    pub drop_prefixes: Vec<Bytes>,

    pub targets: Targets
}

impl CompactDef {
    pub fn new(
        compactor_id: usize,
        this_level: Arc<RwLock<LevelHandler>>,
        next_level: Arc<RwLock<LevelHandler>>,
    ) -> Self {
        Self {
            compactor_id,
            this_level,
            next_level,
            this_range: KeyRange::default(),
            next_range: KeyRange::default(),
            splits: vec![],
            this_size: 0,
            drop_prefixes: vec![],
            top: vec![],
            bot: vec![],
            targets: Targets::new()
        }
    }
}

impl CompactStatus {
    pub fn delete(&mut self, compact_def: CompactDef) {
        // TODO: level is immutable, we could access it without read
        let tl = compact_def.this_level.read().unwrap().level;
        assert!(tl < self.levels.len() - 1);

        let this_level_id = compact_def.this_level.read().unwrap().level;
        let this_level = &mut self.levels[this_level_id];
        let next_level_id = compact_def.next_level.read().unwrap().level;

        this_level.del_size -= compact_def.this_size;
        let mut found = this_level.remove(compact_def.this_range);
        drop(this_level);
        if !compact_def.next_range.is_empty() {
            let next_level = &mut self.levels[next_level_id];
            found = next_level.remove(compact_def.next_range) && found;
        }

        if !found {
            let this = compact_def.this_range;
            let next = compact_def.next_range;

            panic!("key range not found");
        }

        for table in compact_def.top {
            assert!(self.tables.get(&table.id()).is_some());
            // TODO: delete table
        }
    }
}

#[derive(Clone)]
pub struct CompactionPriority {
    pub level: usize,
    pub score: f64,
    pub adjusted: f64,
    pub drop_prefixes: Vec<Bytes>,
    pub targets: Arc<Targets>,
}

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
            file_size: vec![]
        }
    }
}