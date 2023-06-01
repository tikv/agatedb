use crate::db::AgateOptions;
use bytes::BufMut;
use memmap2::MmapMut;
use std::mem::ManuallyDrop;

// keeps track of the amount of data that could be discarded for
// a given logfile.
pub(crate) struct DiscardStats {
    mmap_file: ManuallyDrop<MmapMut>,
    opts: AgateOptions,
    next_empty_slot: usize,
}

impl DiscardStats {
    pub(crate) fn get(&self, offset: usize) -> u64 {
        let mut buf = [0; 8];
        buf.copy_from_slice(&self.mmap_file[offset..offset + 8]);
        u64::from_be_bytes(buf)
    }

    // SpadeA(todo): May consider chaning mmap_file to Mutex<..>, so that &mut self can be &self
    pub(crate) fn set(&mut self, offset: usize, val: u64) {
        let mut buf = [0; 8];
        (&mut buf[..]).put_u64(val);
        self.mmap_file[offset..offset + 8].clone_from_slice(&buf);
    }
}
