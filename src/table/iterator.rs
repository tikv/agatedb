use super::builder::{Header, HEADER_SIZE};
use super::Block;
use crate::{Error, Result};
use bytes::Bytes;
use skiplist::{FixedLengthSuffixComparitor, KeyComparitor};
use std::sync::Arc;

static Comparator: FixedLengthSuffixComparitor = FixedLengthSuffixComparitor::new(8);

#[derive(Clone)]
enum IteratorError {
    EOF,
}

enum SeekPos {
    Origin,
    Current,
}

// TODO: support custom comparator
struct BlockIterator {
    idx: usize,
    base_key: Bytes,
    key: Bytes,
    val: Bytes,
    data: Bytes,
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
            data: self.block.data.slice(..self.block.entries_index_start),
            perv_overlap: 0,
            idx: 0,
        }
    }

    #[inline]
    fn entry_offsets(&self) -> &[u32] {
        &self.block.entry_offsets
    }

    fn set_idx(&mut self, i: usize) {
        self.idx = i;
        if i >= self.entry_offsets().len() {
            self.err = Some(IteratorError::EOF);
            return;
        }

        self.err = None;
        let start_offset = self.entry_offsets()[i] as u32;

        if self.base_key.is_empty() {
            let mut base_header = Header::default();
            base_header.decode(&mut self.data);
            self.base_key = self
                .data
                .slice(HEADER_SIZE..HEADER_SIZE + base_header.diff as usize);
        }

        let end_offset = if self.idx + 1 == self.entry_offsets().len() {
            self.data.len()
        } else {
            self.entry_offsets()[self.idx + 1] as usize
        };

        let mut entry_data = self.data.slice(start_offset as usize..end_offset as usize);
        let mut header = Header::default();
        header.decode(&mut entry_data);

        if header.overlap > self.perv_overlap {
            self.key = Bytes::from(
                [
                    &self.key[..self.perv_overlap as usize],
                    &self.base_key[self.perv_overlap as usize..header.overlap as usize],
                ]
                .concat(),
            );
        }
        self.perv_overlap = header.overlap;
        let value_off = HEADER_SIZE + header.diff as usize;
        let diff_key = &entry_data[HEADER_SIZE..value_off];
        self.key = Bytes::from([&self.key[..header.overlap as usize], diff_key].concat());
        self.val = entry_data.slice(value_off..);
    }

    fn valid(&self) -> bool {
        self.err.is_none()
    }

    fn error(&self) -> Option<IteratorError> {
        self.err.clone()
    }

    // simple rewrite of golang sort.Search
    fn search<F>(n: usize, mut f: F) -> usize
    where
        F: FnMut(usize) -> bool,
    {
        let mut i = 0;
        let mut j = n;
        while i < j {
            let h = (i + j) >> 1;
            if !f(h) {
                i = h + 1;
            } else {
                j = h;
            }
        }
        i
    }

    fn seek(&mut self, key: Bytes, whence: SeekPos) {
        self.err = None;
        let start_index = match whence {
            SeekPos::Origin => 0,
            SeekPos::Current => self.idx,
        };
        let found_entry_idx = Self::search(self.entry_offsets().len(), |idx| {
            use std::cmp::Ordering::*;
            if idx < start_index {
                return false;
            }
            self.set_idx(idx);
            match Comparator.compare_key(&self.key, &key) {
                Less => false,
                _ => true,
            }
        });

        self.set_idx(found_entry_idx);
    }

    fn seek_to_first(&mut self) {
        self.set_idx(0);
    }

    fn seek_to_last(&mut self) {
        self.set_idx(self.entry_offsets().len() - 1);
    }

    fn next(&mut self) {
        self.set_idx(self.idx + 1);
    }

    fn prev(&mut self) {
        self.set_idx(self.idx - 1);
    }
}
