use super::*;

#[derive(Clone)]
pub struct AgateOptions {
    pub path: PathBuf,
    // TODO: docs
    pub in_memory: bool,
    pub sync_writes: bool,

    pub create_if_not_exists: bool,
    pub num_memtables: usize,
    pub mem_table_size: u64,

    pub value_threshold: usize,
    pub value_log_file_size: u64,
    pub value_log_max_entries: u32,
}

impl Default for AgateOptions {
    fn default() -> Self {
        Self {
            create_if_not_exists: false,
            path: PathBuf::new(),
            mem_table_size: 64 << 20,
            num_memtables: 20,
            in_memory: false,
            sync_writes: false,
            value_threshold: 1 << 10,
            value_log_file_size: 1 << 30 - 1,
            value_log_max_entries: 1000000,
        }
        // TODO: add other options
    }
}

impl AgateOptions {
    pub(crate) fn fix_options(&mut self) -> Result<()> {
        if self.in_memory {
            // TODO: find a way to check if path is set, if set, then panic with ConfigError
            self.sync_writes = false;
        }

        Ok(())
    }

    fn skip_vlog(&self, entry: &Entry) -> bool {
        entry.value.len() < self.value_threshold
    }

    fn arena_size(&self) -> u64 {
        // TODO: take other options into account
        self.mem_table_size as u64
    }
}
