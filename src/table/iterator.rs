use super::builder::{Header, HEADER_SIZE};
use super::{Block, TableInner};
use crate::util::{self, KeyComparitor, COMPARATOR};
use crate::value::Value;
use bytes::Bytes;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum IteratorError {
    NoError,
    EOF,
    // TODO: As we need to clone Error from block iterator to table iterator,
    // we had to save `crate::Error` as String. In the future, we could let all
    // seek-related function to return a `Result<()>` instead of saving an
    // error inside struct.
    Error(String),
}

impl IteratorError {
    pub fn is_err(&self) -> bool {
        match self {
            IteratorError::NoError => false,
            _ => true,
        }
    }

    pub fn is_eof(&self) -> bool {
        match self {
            IteratorError::EOF => true,
            _ => false,
        }
    }
}

enum SeekPos {
    Origin,
    Current,
}

// TODO: support custom comparator
struct BlockIterator {
    idx: isize,
    base_key: Bytes,
    key: Bytes,
    val: Bytes,
    data: Bytes,
    // TODO: use `&'a Block` if possible
    block: Arc<Block>,
    perv_overlap: u16,
    err: IteratorError,
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        let data = block.data.slice(..block.entries_index_start);
        Self {
            block,
            err: IteratorError::NoError,
            base_key: Bytes::new(),
            key: Bytes::new(),
            val: Bytes::new(),
            data,
            perv_overlap: 0,
            idx: 0,
        }
    }

    #[inline]
    fn entry_offsets(&self) -> &[u32] {
        &self.block.entry_offsets
    }

    fn set_idx(&mut self, i: isize) {
        self.idx = i;
        if i >= self.entry_offsets().len() as isize || i < 0 {
            self.err = IteratorError::EOF;
            return;
        }

        self.err = IteratorError::NoError;
        let start_offset = self.entry_offsets()[i as usize] as u32;

        if self.base_key.is_empty() {
            let mut base_header = Header::default();
            base_header.decode(&mut self.data.slice(..));
            // TODO: combine this decode with header decode to avoid slice ptr copy
            self.base_key = self
                .data
                .slice(HEADER_SIZE..HEADER_SIZE + base_header.diff as usize);
        }

        let end_offset = if self.idx + 1 == self.entry_offsets().len() as isize {
            self.data.len()
        } else {
            self.entry_offsets()[self.idx as usize + 1] as usize
        };

        let mut entry_data = self.data.slice(start_offset as usize..end_offset as usize);
        let mut header = Header::default();
        header.decode(&mut entry_data);

        // TODO: optimize base_key copy
        // if header.overlap > self.perv_overlap {
        //     self.key = Bytes::from(
        //         [
        //             &self.key[..self.perv_overlap as usize],
        //             &self.base_key[self.perv_overlap as usize..header.overlap as usize],
        //         ]
        //         .concat(),
        //     );
        // }
        // self.perv_overlap = header.overlap;

        let diff_key = &entry_data[..header.diff as usize];
        self.key = Bytes::from([&self.base_key[..header.overlap as usize], diff_key].concat());
        self.val = entry_data.slice(header.diff as usize..);
    }

    pub fn valid(&self) -> bool {
        !self.err.is_err()
    }

    pub fn error(&self) -> &IteratorError {
        &self.err
    }

    pub fn seek(&mut self, key: &Bytes, whence: SeekPos) {
        self.err = IteratorError::NoError;
        let start_index = match whence {
            SeekPos::Origin => 0,
            SeekPos::Current => self.idx,
        };
        let found_entry_idx = util::search(self.entry_offsets().len(), |idx| {
            use std::cmp::Ordering::*;
            if idx < start_index as usize {
                return false;
            }
            self.set_idx(idx as isize);
            match COMPARATOR.compare_key(&self.key, &key) {
                Less => false,
                _ => true,
            }
        });

        self.set_idx(found_entry_idx as isize);
    }

    pub fn seek_to_first(&mut self) {
        self.set_idx(0);
    }

    pub fn seek_to_last(&mut self) {
        self.set_idx(self.entry_offsets().len() as isize - 1);
    }

    pub fn next(&mut self) {
        self.set_idx(self.idx + 1);
    }

    pub fn prev(&mut self) {
        self.set_idx(self.idx - 1);
    }
}

// TODO: use `bitfield` if there are too many variants
pub const ITERATOR_REVERSED: usize = 1 << 1;
pub const ITERATOR_NOCACHE: usize = 1 << 2;

/// An iterator over SST.
///
/// Here we use generic because when initializaing a
/// table object, we need to get smallest and biggest
/// elements by using an iterator over `&TableInner`.
/// At that time, we could not build an `Arc<TableInner>`.
pub struct Iterator<T: AsRef<TableInner>> {
    table: T,
    bpos: isize,
    block_iterator: Option<BlockIterator>,
    err: IteratorError,
    opt: usize,
}

impl<T: AsRef<TableInner>> Iterator<T> {
    pub fn new(table: T, opt: usize) -> Self {
        Self {
            table,
            bpos: 0,
            block_iterator: None,
            err: IteratorError::NoError,
            opt,
        }
    }

    pub fn reset(&mut self) {
        self.bpos = 0;
        self.err = IteratorError::NoError;
    }

    pub fn valid(&self) -> bool {
        !self.err.is_err()
    }

    pub fn use_cache(&self) -> bool {
        self.opt & ITERATOR_NOCACHE == 0
    }

    pub fn seek_to_first(&mut self) {
        let num_blocks = self.table.as_ref().offsets_length();
        if num_blocks == 0 {
            self.err = IteratorError::EOF;
            return;
        }
        self.bpos = 0;
        match self
            .table
            .as_ref()
            .block(self.bpos as usize, self.use_cache())
        {
            Ok(block) => {
                let mut block_iterator = BlockIterator::new(block);
                block_iterator.seek_to_first();
                self.err = block_iterator.err.clone();
                self.block_iterator = Some(block_iterator);
            }
            Err(err) => self.err = IteratorError::Error(err.to_string()),
        }
    }

    pub fn seek_to_last(&mut self) {
        let num_blocks = self.table.as_ref().offsets_length();
        if num_blocks == 0 {
            self.err = IteratorError::EOF;
            return;
        }
        self.bpos = num_blocks as isize - 1;
        match self
            .table
            .as_ref()
            .block(self.bpos as usize, self.use_cache())
        {
            Ok(block) => {
                let mut block_iterator = BlockIterator::new(block);
                block_iterator.seek_to_last();
                self.err = block_iterator.err.clone();
                self.block_iterator = Some(block_iterator);
            }
            Err(err) => self.err = IteratorError::Error(err.to_string()),
        }
    }

    fn seek_helper(&mut self, block_idx: usize, key: &Bytes) {
        self.bpos = block_idx as isize;
        match self
            .table
            .as_ref()
            .block(self.bpos as usize, self.use_cache())
        {
            Ok(block) => {
                let mut block_iterator = BlockIterator::new(block);
                block_iterator.seek(key, SeekPos::Origin);
                self.err = block_iterator.err.clone();
                self.block_iterator = Some(block_iterator);
            }
            Err(err) => self.err = IteratorError::Error(err.to_string()),
        }
    }

    fn seek_from(&mut self, key: &Bytes, whence: SeekPos) {
        self.err = IteratorError::NoError;
        match whence {
            SeekPos::Origin => self.reset(),
            _ => {}
        }

        let idx = util::search(self.table.as_ref().offsets_length(), |idx| {
            use std::cmp::Ordering::*;
            let block_offset = self.table.as_ref().offsets(idx).unwrap();
            match COMPARATOR.compare_key(&block_offset.key, &key) {
                Less => false,
                _ => true,
            }
        });

        if idx == 0 {
            self.seek_helper(0, key);
            return;
        }

        self.seek_helper(idx - 1, key);
        if self.err.is_eof() {
            if idx == self.table.as_ref().offsets_length() {
                return;
            }
            self.seek_helper(idx, key);
        }
    }

    // seek_inner will reset iterator and seek to >= key.
    fn seek_inner(&mut self, key: &Bytes) {
        self.seek_from(key, SeekPos::Origin);
    }

    // seek_for_prev will reset iterator and seek to <= key.
    pub fn seek_for_prev(&mut self, key: &Bytes) {
        self.seek_from(key, SeekPos::Origin);
        if self.key() != key {
            self.prev_inner();
        }
    }

    fn next_inner(&mut self) {
        self.err = IteratorError::NoError;

        if self.bpos as usize > self.table.as_ref().offsets_length() {
            self.err = IteratorError::EOF;
            return;
        }

        if self.block_iterator.as_ref().unwrap().data.is_empty() {
            match self
                .table
                .as_ref()
                .block(self.bpos as usize, self.use_cache())
            {
                Ok(block) => {
                    let mut block_iterator = BlockIterator::new(block);
                    block_iterator.seek_to_first();
                    self.err = block_iterator.err.clone();
                    self.block_iterator = Some(block_iterator);
                }
                Err(err) => self.err = IteratorError::Error(err.to_string()),
            }
        } else {
            let bi = self.block_iterator.as_mut().unwrap();
            bi.next();
            if !bi.valid() {
                self.bpos += 1;
                bi.data = Bytes::new();
                self.next_inner();
            }
        }
    }

    fn prev_inner(&mut self) {
        self.err = IteratorError::NoError;

        if self.bpos < 0 {
            self.err = IteratorError::EOF;
            return;
        }

        if self.block_iterator.as_ref().unwrap().data.is_empty() {
            match self
                .table
                .as_ref()
                .block(self.bpos as usize, self.use_cache())
            {
                Ok(block) => {
                    let mut block_iterator = BlockIterator::new(block);
                    block_iterator.seek_to_last();
                    self.err = block_iterator.err.clone();
                    self.block_iterator = Some(block_iterator);
                }
                Err(err) => self.err = IteratorError::Error(err.to_string()),
            }
        } else {
            let bi = self.block_iterator.as_mut().unwrap();
            bi.prev();
            if !bi.valid() {
                self.bpos -= 1;
                bi.data = Bytes::new();
                self.prev_inner();
            }
        }
    }

    pub fn key(&self) -> &Bytes {
        &self.block_iterator.as_ref().unwrap().key
    }

    pub fn value(&self) -> Value {
        let mut value = Value::default();
        value.decode(&self.block_iterator.as_ref().unwrap().val);
        value
    }

    pub fn next(&mut self) {
        if self.opt & ITERATOR_REVERSED == 0 {
            self.next_inner();
        } else {
            self.prev_inner();
        }
    }

    pub(crate) fn prev(&mut self) {
        if self.opt & ITERATOR_REVERSED == 0 {
            self.prev_inner();
        } else {
            self.next_inner();
        }
    }

    pub fn rewind(&mut self) {
        if self.opt & ITERATOR_REVERSED == 0 {
            self.seek_to_first();
        } else {
            self.seek_to_last();
        }
    }

    pub fn seek(&mut self, key: &Bytes) {
        if self.opt & ITERATOR_REVERSED == 0 {
            self.seek_inner(key);
        } else {
            self.seek_for_prev(key);
        }
    }

    pub fn error(&self) -> &IteratorError {
        &self.err
    }
}
