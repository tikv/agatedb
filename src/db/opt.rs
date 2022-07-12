use std::cmp;

use getset::Setters;
use skiplist::MAX_NODE_SIZE;

use super::*;
use crate::{entry::Entry, opt};

#[derive(Clone, Setters)]
pub struct AgateOptions {
    /* Required options. */
    /// The path of the directory where key data will be stored in.
    ///
    /// If it doesn't exist, Agate will try to create it for you.
    pub dir: PathBuf,
    /// The path of the directory where value data will be stored in.
    ///
    /// If it doesn't exist, Agate will try to create it for you.
    pub value_dir: PathBuf,

    /*  Usually modified options. */
    /// When set to true, the DB would call an additional sync after writes to flush
    /// mmap buffer over to disk to survive hard reboots.
    ///
    /// The default value of `sync_writes` is false.
    #[getset(set = "pub")]
    pub sync_writes: bool,
    /// Sets how many versions to keep per key at most.
    ///
    /// The default value of `num_versions_to_keep` is 1.
    pub num_versions_to_keep: usize,
    /// When set to true, the DB will be opened on read-only mode.
    ///
    /// The default value of `read_only` is false.
    pub read_only: bool,
    /// When set to true, everything is stored in memory. No value/sst files are
    /// created. In case of a crash all data will be lost.
    ///
    /// The default value of `in_memory` is false.
    #[getset(set = "pub")]
    pub in_memory: bool,

    /* Fine tuning options. */
    /// Sets the maximum size in bytes for memtable.
    ///
    /// The default value of `mem_table_size` is 64 << 20 bytes.
    #[getset(set = "pub")]
    pub mem_table_size: u64,
    /// Sets the maximum size in bytes for LSM table or file in the base level.
    ///
    /// The default value of `base_table_size` is 2 << 20 bytes.
    pub base_table_size: u64,
    /// Sets the maximum size target for the base level.
    ///
    /// The default value of `base_level_size` is 10 << 20 bytes.
    pub base_level_size: u64,
    /// Sets the ratio between the maximum sizes of contiguous levels in the LSM.
    ///
    /// The default value of `level_size_multiplier` is 10.
    pub level_size_multiplier: usize,
    /// Sets the ratio between the maximum sizes of table in contiguous levels.
    ///
    /// The default value of `table_size_multiplier` is 2.
    pub table_size_multiplier: usize,
    /// Sets the maximum number of levels of compaction allowed in the LSM.
    ///
    /// The default value of `max_levels` is 7.
    pub max_levels: usize,

    /// Sets the threshold used to decide whether a value is stored directly in the
    /// LSM tree or separately in the log value files.
    ///
    /// The default value of `value_threshold` is 1 << 10 bytes.
    pub value_threshold: usize,
    /// Sets the maximum number of tables to keep in memory before stalling.
    ///
    /// The default value of `num_memtables` is 20.
    #[getset(set = "pub")]
    pub num_memtables: usize,

    /// Sets the size of any block in SSTable.
    ///
    /// The default value of `block_size` is 4 << 10 bytes.
    pub block_size: usize,
    /// Sets the false positive probability of the bloom filter in any SSTable.
    ///
    /// The default value of `bloom_false_positive` is 0.01.
    pub bloom_false_positive: f64,

    /// Sets the maximum number of Level 0 tables before compaction starts.
    ///
    /// The default value of `num_level_zero_tables` is 5.
    pub num_level_zero_tables: usize,
    /// Sets the number of Level 0 tables that once reached causes the DB to
    /// stall until compaction succeeds.
    ///
    /// The default value of `num_level_zero_tables_stall` is 15.
    pub num_level_zero_tables_stall: usize,

    /// Sets the maximum size of a single value log file.
    ///
    /// The default value of `value_log_file_size` is 1 << (30 - 1) bytes.
    #[getset(set = "pub")]
    pub value_log_file_size: u64,
    /// Sets the maximum number of entries a value log file can hold approximately.
    ///
    /// The default value of `value_log_max_entries` is 1000000.
    #[getset(set = "pub")]
    pub value_log_max_entries: u32,

    /// Sets the number of compaction workers to run concurrently.
    ///
    /// The default value of `num_compactors` is 4.
    pub num_compactors: usize,
    /// Indicates when the db should verify checksums for SSTable blocks.
    ///
    /// The default value of `checksum_mode` is [`NoVerification`].
    pub checksum_mode: opt::ChecksumVerificationMode,

    /// Determines whether the transactions would be checked for conflicts.
    /// The transactions can be processed at a higher rate when conflict
    /// detection is disabled.
    ///
    /// The default value of `detect_conflicts` is true.
    pub detect_conflicts: bool,

    /// Transaction start and commit timestamps are managed by end-user.
    ///
    /// The default value of `managed_txns` is false.
    pub managed_txns: bool,

    /// Create the directory if the provided open path doesn't exists.
    ///
    /// The default value of `create_if_not_exists` is false
    #[getset(set = "pub")]
    pub create_if_not_exists: bool,

    /// Max entries in batch.
    ///
    /// The default value of `max_batch_count` is `max_batch_size` / `MAX_NODE_SIZE`.
    pub max_batch_count: u64,
    /// Max batch size in bytes.
    ///
    /// The default value of `max_batch_size` is (15 * `mem_table_size`) / 100.
    pub max_batch_size: u64,
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

            detect_conflicts: true,

            managed_txns: false,

            create_if_not_exists: false,

            max_batch_count: 0,
            max_batch_size: 0,
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

        self.max_batch_size = (15 * self.mem_table_size) / 100;
        self.max_batch_count = self.max_batch_size / MAX_NODE_SIZE as u64;

        Ok(())
    }

    pub fn open(&mut self) -> Result<Agate> {
        self.fix_options()?;

        if !self.in_memory {
            if !self.dir.exists() {
                if self.read_only {
                    return Err(Error::Config(format!("{:?} doesn't exist", self.dir)));
                }
                fs::create_dir_all(&self.dir)?;
            }
            if !self.value_dir.exists() {
                if self.read_only {
                    return Err(Error::Config(format!("{:?} doesn't exist", self.value_dir)));
                }
                fs::create_dir_all(&self.value_dir)?;
            }
            // TODO: Acquire database path lock.
        }

        Ok(Agate::new(Arc::new(Core::new(self)?)))
    }

    pub fn skip_vlog(&self, entry: &Entry) -> bool {
        entry.value.len() < self.value_threshold
    }

    pub fn arena_size(&self) -> u64 {
        // When arena reach it's capacity, it will grow, this will copy whole data to the new
        // allocated memory space and the cost is not trivial. So we adjust the arena capacity
        // to a bit of larger than the memtable size to prevent arena's grow in most cases.
        let extra_size = cmp::min((self.mem_table_size as f64 * 0.1) as u64, 1024 * 1024);
        self.mem_table_size + extra_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_options_set() {
        let mut opt = AgateOptions::default();
        opt.set_create_if_not_exists(true)
            .set_in_memory(true)
            .set_value_log_file_size(256)
            .set_num_memtables(3)
            .set_value_log_max_entries(96)
            .set_sync_writes(true)
            .set_mem_table_size(1024);
        assert!(opt.create_if_not_exists);
        assert!(opt.in_memory);
        assert_eq!(opt.value_log_file_size, 256);
        assert_eq!(opt.num_memtables, 3);
        assert_eq!(opt.value_log_max_entries, 96);
        assert!(opt.sync_writes);
        assert_eq!(opt.mem_table_size, 1024);
    }

    #[test]
    fn test_adjust_arena_size() {
        // Arena size is larger than mem_table_size, but the extra size is not larger than 1MB
        let mut opt = AgateOptions::default();
        opt.set_mem_table_size(32 * 1024 * 1024);
        assert!(opt.arena_size() > opt.mem_table_size);
        assert!(opt.arena_size() <= opt.mem_table_size + 1024 * 1024);

        // Arena size is larger than mem_table_size, but the extra size is not larger than 10%
        // memtable size
        opt.set_mem_table_size(4096);
        assert!(opt.arena_size() > opt.mem_table_size);
        assert!(opt.arena_size() <= (opt.mem_table_size as f64 * 1.1) as u64);
    }
}
