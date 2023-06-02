use crate::{db::AgateOptions, util, Result};
use bytes::BufMut;
use log::info;
use memmap2::{MmapMut, MmapOptions};
use std::{mem::ManuallyDrop, sync::RwLock};

// keeps track of the amount of data that could be discarded for
// a given logfile.
pub(crate) struct DiscardStats {
    inner: RwLock<DiscardStatsInner>,
    opts: AgateOptions,
}

pub struct DiscardStatsInner {
    mmap_file: ManuallyDrop<MmapMut>,
    next_empty_slot: usize,
}

const DISCARD_FNAME: &str = "DISCARD";

impl DiscardStats {
    pub(crate) fn init_discard_stats(opts: AgateOptions) -> Result<Self> {
        let fname = opts.value_dir.as_path().join(DISCARD_FNAME);

        // 1GB file can store 67M discard entries. Each entry is 16 bytes.
        let mmap_file = ManuallyDrop::new(unsafe { MmapOptions::new().map_mut(&fname)? });
        let mut discard_stats = DiscardStats {
            inner: RwLock::new(DiscardStatsInner {
                mmap_file,
                next_empty_slot: 0,
            }),
            opts,
        };

        for slot in 0..discard_stats.max_slot() {
            if discard_stats.get(16 * slot) == 0 {
                discard_stats.next_empty_slot = slot;
                break;
            }
        }

        // sort
        unimplemented!();

        info!(
            "Discard stats";
            "next_empty_slot" => discard_stats.next_empty_slot,
        );

        Ok(discard_stats)
    }

    pub(crate) fn get(&self, offset: usize) -> u64 {
        let mut buf = [0; 8];
        buf.copy_from_slice(&self.inner.read().unwrap().mmap_file[offset..offset + 8]);
        u64::from_be_bytes(buf)
    }

    // SpadeA(todo): May consider chaning mmap_file to Mutex<..>, so that &mut self can be &self
    pub(crate) fn set(&mut self, offset: usize, val: u64) {
        let mut buf = [0; 8];
        (&mut buf[..]).put_u64(val);
        self.inner.write().unwrap().mmap_file[offset..offset + 8].clone_from_slice(&buf);
    }

    pub(crate) fn max_slot(&self) -> usize {
        self.inner.read().unwrap().mmap_file.len() as usize / 16
    }

    // Update would update the discard stats for the given file id. If discard is
    // 0, it would return the current value of discard for the file. If discard is
    // < 0, it would set the current value of discard to zero for the file.
    pub fn update(&self, fid: u64, discard: isize) -> u64 {
        let inner = self.inner.write().unwrap();
        let idx = util::search(inner.next_empty_slot, |slot| -> bool {
            self.get(slot * 16) >= fid
        });

        if idx < inner.next_empty_slot && self.get(idx * 16) == fid {
            let discard_off = idx * 16 + 8;
            let cur_discard = self.get(discard_off);
            if discard == 0 {
                return cur_discard;
            }
            if discard < 0 {
                self.update(discard_off, 0);
                return 0;
            }
            self.set(discard_off, cur_discard + discard as u64);
            return cur_discard + discard as u64;
        }

        if discard <= 0 {
            // No need to add a new entry.
            return 0;
        }
    }
}
