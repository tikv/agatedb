use super::*;
use crate::opt;

#[derive(Clone)]
pub struct AgateOptions {
    /* Required options. */
    /// The path of the directory where key data will be stored in.
    pub dir: PathBuf,
    /// The path of the directory where value data will be stored in.
    pub value_dir: PathBuf,

    /*  Usually modified options. */
    /// When set to true, the DB would call an additional sync after writes to flush
    /// mmap buffer over to disk to survive hard reboots.
    pub sync_writes: bool,
    /// Sets how many versions to keep per key at most.
    pub num_versions_to_keep: usize,
    /// When set to true, the DB will be opened on read-only mode.
    pub read_only: bool,
    /// When set to true, everything is stored in memory. No value/sst files are
    /// created. In case of a crash all data will be lost.
    pub in_memory: bool,

    /* Fine tuning options. */
    /// Sets the maximum size in bytes for memtable.
    pub mem_table_size: u64,
    /// Sets the maximum size in bytes for LSM table or file in the base level.
    pub base_table_size: u64,
    /// Sets the maximum size target for the base level.
    pub base_level_size: u64,
    /// Sets the ratio between the maximum sizes of contiguous levels in the LSM.
    pub level_size_multiplier: usize,
    /// Sets the ratio between the maximum sizes of table in contiguous levels.
    pub table_size_multiplier: usize,
    /// Sets the maximum number of levels of compaction allowed in the LSM.
    pub max_levels: usize,

    /// Sets the threshold used to decide whether a value is stored directly in the
    /// LSM tree or separately in the log value files.
    pub value_threshold: usize,
    /// Sets the maximum number of tables to keep in memory before stalling.
    pub num_memtables: usize,

    /// Sets the size of any block in SSTable.
    pub block_size: usize,
    /// Sets the false positive probability of the bloom filter in any SSTable.
    pub bloom_false_positive: f64,

    /// Sets the maximum number of Level 0 tables before compaction starts.
    pub num_level_zero_tables: usize,
    /// Sets the number of Level 0 tables that once reached causes the DB to
    /// stall until compaction succeeds.
    pub num_level_zero_tables_stall: usize,

    /// Sets the maximum size of a single value log file.
    pub value_log_file_size: u64,
    /// Sets the maximum number of entries a value log file can hold approximately.
    pub value_log_max_entries: u32,

    /// Sets the number of compaction workers to run concurrently.
    pub num_compactors: usize,
    /// Indicates when the db should verify checksums for SSTable blocks.
    pub checksum_mode: opt::ChecksumVerificationMode,
}

impl Default for AgateOptions {
    fn default() -> Self {
        Self {
            dir: PathBuf::new(),
            value_dir: PathBuf::new(),

            sync_writes: false,
            num_versions_to_keep: 1,
            read_only: false,
            in_memory: false,

            mem_table_size: 64 << 20,
            base_table_size: 2 << 20,
            base_level_size: 10 << 20,
            level_size_multiplier: 10,
            table_size_multiplier: 2,
            max_levels: 7,

            value_threshold: 1 << 10,
            num_memtables: 20,
            block_size: 4 << 10,
            bloom_false_positive: 0.01,

            num_level_zero_tables: 5,
            num_level_zero_tables_stall: 15,

            value_log_file_size: 1 << (30 - 1),
            value_log_max_entries: 1000000,

            num_compactors: 4,
            checksum_mode: opt::ChecksumVerificationMode::NoVerification,
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

    pub fn skip_vlog(&self, entry: &Entry) -> bool {
        entry.value.len() < self.value_threshold
    }

    pub fn arena_size(&self) -> u64 {
        // TODO: take other options into account
        self.mem_table_size as u64
    }
}
