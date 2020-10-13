use super::Block;
use crate::{Error, Result};
use bytes::Bytes;
use std::sync::Arc;

enum IteratorError {
    EOF
}

struct BlockIterator {
    idx: usize,
    base_key: Bytes,
    key: Bytes,
    val: Bytes,
    // TODO: use `&'a Block` if possible
    block: Arc<Block>,
    perv_overlap: u16,
    err: Option<IteratorError>,
}

impl BlockIterator {
    pub fn new(&mut self, block: Arc<Block>) -> Self {
        Self {
            block,
            err: None,
            base_key: Bytes::new(),
            key: Bytes::new(),
            val: Bytes::new(),
            perv_overlap: 0,
            idx: 0,
        }
    }

    fn data(&self) -> Bytes {
        self.block.data.slice(..self.block.entries_index_start)
    }

    fn entry_offsets(&self) -> &[u32] {
        &self.block.entry_offsets
    }

    fn set_idx(&mut self, i: usize) {
        self.idx = i;
        if i >= self.entry_offsets().len() {
            self.err = Some(IteratorError::EOF);
            return;
        }

        let start_offet = self.entry_offsets()[i] as u32;

        if self.base_key.is_empty() {
            // TODO: decode data
            let base_header = self.data();
        }
    }
}
