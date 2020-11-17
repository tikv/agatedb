use std::collections::HashMap;

use std::sync::{Arc, RwLock};

use bytes::Bytes;

use super::LevelHandler;

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
    pub ranges: (),
    pub del_size: u64,
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

    this_size: u64,

    drop_prefixes: Vec<Bytes>,
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
        }
    }
}

impl CompactStatus {
    pub fn delete(&mut self, compact_def: CompactDef) {
        // TODO: level is immutable, we could access it without read
        let tl = compact_def.this_level.read().unwrap().level;
        assert!(tl < self.levels.len() - 1);

        let this_level = &mut self.levels[compact_def.this_level.read().unwrap().level];
        let next_level_id = compact_def.next_level.read().unwrap().level;

        this_level.del_size -= compact_def.this_size;
        let found = this_level.remove(compact_def.this_range);
        if !compact_def.next_range.is_empty() {}
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
