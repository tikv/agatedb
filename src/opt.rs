#[derive(Debug, Clone)]
pub struct Options {
    /// size of each block inside SST
    pub table_size: u64,
    /// size of each block in bytes in SST
    pub block_size: usize,
    /// false positive probability of bloom filter
    pub bloom_false_positive: f64,
    /// checksum mode
    pub checksum_mode: ChecksumVerificationMode,
}
#[derive(Debug, Clone)]
pub enum ChecksumVerificationMode {
    NoVerification,
    OnTableRead,
    // OnBlockRead indicates checksum should be verified on every SSTable block read.
    OnBlockRead,
    // OnTableAndBlockRead indicates checksum should be verified
    // on SSTable opening and on every block read.
    OnTableAndBlockRead,
}
