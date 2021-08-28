use crate::Table;
use bytes::Bytes;

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
    pub fn pick_tables(&self, _tables: &mut Vec<Table>) {
        // TODO: implement table selection logic
    }
}
