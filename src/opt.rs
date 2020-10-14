#[derive(Debug, Clone)]
pub struct Options {
    /// size of each block inside SST
    pub table_size: u64,
    /// size of each block in bytes in SST
    pub block_size: usize,
    /// false positive probability of bloom filter
    pub bloom_false_positive: f64,
}
