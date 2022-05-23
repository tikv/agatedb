use bytes::Bytes;

use crate::Table;

#[derive(Default, Clone)]
pub struct IteratorOptions {
    pub prefetch_size: usize,
    pub prefetch_values: bool,
    pub reverse: bool,
    pub all_versions: bool,
    pub internal_access: bool,
    prefix_is_key: bool,
    pub prefix: Bytes,
}

impl IteratorOptions {
    /// Check if a table should be included in iterator
    pub fn pick_table(&self, _table: &Table) -> bool {
        true
        // TODO: implement table selection logic
    }

    /// Remove unnecessary tables
    pub fn pick_tables(&self, _tables: &mut [Table]) {
        // TODO: implement table selection logic
    }
}

pub fn is_deleted_or_expired(meta: u8, expires_at: u64) -> bool {
    if meta & crate::value::VALUE_DELETE != 0 {
        return true;
    }
    if expires_at == 0 {
        return false;
    }
    expires_at <= crate::util::unix_time()
}
