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

use super::LevelHandler;

#[derive(Default)]
pub struct LevelCompactStatus {
    ranges: (),
    del_size: u64,
}

pub struct CompactStatus {
    levels: Vec<LevelCompactStatus>,
    tables: HashMap<u64, ()>,
}

pub struct CompactDef {
    compactor_id: usize,
    this_level: Arc<RwLock<LevelHandler>>,
    next_level: Arc<RwLock<LevelHandler>>,
}

pub struct CompactionPriority {
    level: usize,
    score: f64,
    adjusted: f64,
    drop_prefixes: Vec<Bytes>,
    targets: Arc<Targets>,
}
