use super::builder::{Header, HEADER_SIZE};
use super::{Block, TableInner};
use crate::util::{self, KeyComparator, COMPARATOR};
use crate::value::Value;
use bytes::{Bytes, BytesMut};
use std::sync::Arc;

/// Errors that may encounter during iterator operation
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
    /// Check if iterator has error
    pub fn is_err(&self) -> bool {
        !matches!(self, IteratorError::NoError)
    }

    /// Check if iterator has reached its end
    pub fn is_eof(&self) -> bool {
        matches!(self, IteratorError::EOF)
    }
}

enum SeekPos {
    Origin,
    Current,
}

/// Block iterator iterates on an SST block
// TODO: support custom comparator
struct BlockIterator {
    /// current index of iterator
    idx: isize,
    /// base key of the block
    base_key: Bytes,
    /// key of current entry
    key: BytesMut,
    /// raw value of current entry
    val: Bytes,
    /// block data in bytes
    data: Bytes,
    /// block struct
    // TODO: use `&'a Block` if possible
    block: Arc<Block>,
    /// previous overlap key, used to construct key of current entry from
    /// previous one faster
    perv_overlap: u16,
    /// iterator error in last operation
    err: IteratorError,
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        let data = block.data.slice(..block.entries_index_start);
        Self {
            block,
            err: IteratorError::NoError,
            base_key: Bytes::new(),
            key: BytesMut::new(),
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

        // TODO: merge this truncate with the following key truncate
        if header.overlap > self.perv_overlap {
            self.key.truncate(self.perv_overlap as usize);
            self.key.extend_from_slice(
                &self.base_key[self.perv_overlap as usize..header.overlap as usize],
            );
        }
        self.perv_overlap = header.overlap;

        let diff_key = &entry_data[..header.diff as usize];
        self.key.truncate(header.overlap as usize);
        self.key.extend_from_slice(diff_key);
        self.val = entry_data.slice(header.diff as usize..);
    }
    /// Check if last operation of iterator is error
    /// TODO: use `Result<()>` for all iterator operation and remove this if possible
    pub fn valid(&self) -> bool {
        !self.err.is_err()
    }

    /// Return error of last operation
    pub fn error(&self) -> &IteratorError {
        &self.err
    }

    /// Seek to the first entry that is equal or greater than key
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iterator_error() {
        let ite1 = IteratorError::EOF;
        assert!(ite1.is_eof());
        assert!(ite1.is_err());

        let ite2 = IteratorError::NoError;
        assert!(!ite2.is_eof());
        assert!(!ite2.is_err());

        let ite3 = IteratorError::Error("23333".to_string());
        assert!(!ite3.is_eof());
        assert!(ite3.is_err());
    }
}
