use crate::{Table, AgateOptions};
use crate::db::Core as AgateCore;
use std::sync::atomic::AtomicU64;
use crate::Result;

struct CompactStatus {

}
struct LevelHandler {

}

pub struct LevelsController {
    next_file_id: AtomicU64,

    levels: Vec<LevelHandler>,
    opts: AgateOptions,
    // TODO: agate oracle, manifest should be added here

    cpt_status: CompactStatus
}

impl LevelsController {
    pub fn new(db: &AgateCore) -> Result<Self> {
        let lvctl = Self {
            next_file_id: AtomicU64::new(0),
            levels: vec![],
            opts: db.opts.clone(),
            cpt_status: CompactStatus {}
        };

        Ok(lvctl)
    }

    pub fn reserve_file_id(&self) -> u64 {
        0
    }

    pub fn add_l0_table(&self, table: Table) {
        unimplemented!()
    }
}
