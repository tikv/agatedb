use std::collections::HashMap;

use std::sync::{Arc, RwLock};

use bytes::Bytes;

use super::LevelHandler;

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
}

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
