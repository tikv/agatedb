use super::TableOptions;

#[derive(Debug, Clone, Default)]
pub struct Options {
    /// size of each block inside SST
    pub table_size: u64,
    /// capacify of table (only useful when building SST, generally 0.95 * table size)
    pub(crate) table_capacity: u64,
    /// size of each block in bytes in SST
    pub block_size: usize,
    /// false positive probability of bloom filter
    pub bloom_false_positive: f64,
    /// checksum mode
    pub checksum_mode: ChecksumVerificationMode,
}

#[derive(Debug, Clone, Copy)]
pub enum ChecksumVerificationMode {
    NoVerification,
    OnTableRead,
    // OnBlockRead indicates checksum should be verified on every SSTable block read.
    OnBlockRead,
    // OnTableAndBlockRead indicates checksum should be verified
    // on SSTable opening and on every block read.
    OnTableAndBlockRead,
}

impl Default for ChecksumVerificationMode {
    fn default() -> Self {
        Self::NoVerification
    }
}

pub fn build_table_options(opt: &crate::AgateOptions) -> TableOptions {
    // get latest data key
    TableOptions {
        table_size: opt.base_table_size,
        block_size: opt.block_size,
        bloom_false_positive: opt.bloom_false_positive,
        checksum_mode: opt.checksum_mode,
        table_capacity: (opt.base_level_size as f64 * 0.95) as u64,
    }
}
