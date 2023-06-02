use crate::{db::AgateOptions, util, Result};
use bytes::BufMut;
use indexsort::IndexSort;
use log::info;
use memmap2::{MmapMut, MmapOptions};
use std::{fs::OpenOptions, mem::ManuallyDrop, sync::RwLock};

const DISCARD_FNAME: &str = "DISCARD";

// keeps track of the amount of data that could be discarded for
// a given logfile.
pub(crate) struct DiscardStats {
    inner: RwLock<DiscardStatsInner>,
    opts: AgateOptions,
}

impl DiscardStats {
    pub(crate) fn init_discard_stats(opts: AgateOptions) -> Result<Self> {
        let fname = opts.value_dir.as_path().join(DISCARD_FNAME);

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&fname)?;
        // 1GB file can store 67M discard entries. Each entry is 16 bytes.
        let mmap_file = ManuallyDrop::new(unsafe { MmapOptions::new().map_mut(&file)? });
        let discard_stats = DiscardStats {
            inner: RwLock::new(DiscardStatsInner {
                mmap_file,
                next_empty_slot: 0,
            }),
            opts,
        };

        {
            let mut inner = discard_stats.inner.write().unwrap();
            for slot in 0..inner.max_slot() {
                if inner.get(slot * 16) == 0 {
                    inner.next_empty_slot = slot;
                    break;
                }
            }

            inner.sort();
            info!("Discard stats, next_empty_slot {}", inner.next_empty_slot,);
        }

        Ok(discard_stats)
    }

    // Update would update the discard stats for the given file id. If discard is
    // 0, it would return the current value of discard for the file. If discard is
    // < 0, it would set the current value of discard to zero for the file.
    pub fn update(&self, fid: u64, discard: isize) -> u64 {
        let mut inner = self.inner.write().unwrap();
        let idx = util::search(inner.next_empty_slot, |slot| -> bool {
            inner.get(slot * 16) >= fid
        });

        if idx < inner.next_empty_slot && inner.get(idx * 16) == fid {
            let discard_off = idx * 16 + 8;
            let cur_discard = inner.get(discard_off);
            if discard == 0 {
                return cur_discard;
            }
            if discard < 0 {
                inner.set(discard_off, 0);
                return 0;
            }
            inner.set(discard_off, cur_discard + discard as u64);
            return cur_discard + discard as u64;
        }

        if discard <= 0 {
            // No need to add a new entry.
            return 0;
        }

        // Could not find the fid. Add the entry.
        let idx = inner.next_empty_slot;
        inner.set(idx * 16, fid);
        inner.set(idx * 16 + 8, discard as u64);

        inner.next_empty_slot += 1;
        while inner.next_empty_slot >= inner.max_slot() {
            // spadea(todo): remap
            unimplemented!()
        }
        inner.zero_out();
        inner.sort();

        discard as u64
    }

    pub(crate) fn iterate<F>(&self, mut f: F)
    where
        F: FnMut(u64, u64),
    {
        let inner = self.inner.read().unwrap();
        for slot in 0..inner.next_empty_slot {
            let idx = slot * 16;
            f(inner.get(idx), inner.get(idx + 8));
        }
    }

    pub(crate) fn max_discard(&self) -> (u64, u64) {
        let (mut max_fid, mut max_val) = (0, 0);
        self.iterate(|fid, val| {
            if max_val < val {
                max_val = val;
                max_fid = fid;
            }
        });

        (max_fid, max_val)
    }
}

pub struct DiscardStatsInner {
    mmap_file: ManuallyDrop<MmapMut>,
    next_empty_slot: usize,
}

impl DiscardStatsInner {
    fn get(&self, offset: usize) -> u64 {
        let mut buf = [0; 8];
        buf.copy_from_slice(&self.mmap_file[offset..offset + 8]);
        u64::from_be_bytes(buf)
    }

    fn set(&mut self, offset: usize, val: u64) {
        let mut buf = [0; 8];
        (&mut buf[..]).put_u64(val);
        self.mmap_file[offset..offset + 8].clone_from_slice(&buf);
    }

    fn max_slot(&self) -> usize {
        self.mmap_file.len() as usize / 16
    }

    fn zero_out(&mut self) {
        self.set(self.next_empty_slot * 16, 0);
        self.set(self.next_empty_slot * 16 + 8, 0);
    }
}

impl IndexSort for DiscardStatsInner {
    fn len(&self) -> usize {
        self.max_slot()
    }

    fn less(&self, i: usize, j: usize) -> bool {
        self.get(i * 16 + 8) < self.get(j * 16 + 8)
    }

    fn swap(&mut self, i: usize, j: usize) {
        let (i, j) = {
            if i <= j {
                (i, j)
            } else {
                (j, i)
            }
        };

        let (mmap_left, mmap_right) = self.mmap_file.split_at_mut(j * 16);
        let left = &mut mmap_left[i * 16..i * 16 + 16];
        let right = &mut mmap_right[..16];
        let mut tmp = [0; 16];
        tmp.clone_from_slice(&left);
        left.clone_from_slice(&right);
        right.clone_from_slice(&tmp);
    }
}
