use crate::value::{Request, ValuePointer, self};
use crate::wal::Wal;
use crate::AgateOptions;
use crate::{Error, Result};

use bytes::{Bytes, BytesMut};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, RwLock};

fn vlog_file_path(dir: impl AsRef<Path>, fid: u32) -> PathBuf {
    dir.as_ref().join(format!("{:06}.vlog", fid))
}

struct Core {
    files_map: HashMap<u32, Wal>,
    max_fid: u32,
    files_to_delete: Vec<u32>,
    num_entries_written: u32,
}

impl Core {
    fn new() -> Self {
        Self {
            files_map: HashMap::new(),
            max_fid: 0,
            files_to_delete: vec![],
            num_entries_written: 0,
        }
    }
}

pub struct ValueLog {
    dir_path: PathBuf,
    core: Arc<RwLock<Core>>,
    writeable_log_offset: AtomicU32,
    opts: AgateOptions,
}

impl ValueLog {
    pub fn new(opts: AgateOptions) -> Option<Self> {
        if opts.in_memory {
            None
        } else {
            Some(Self {
                core: Arc::new(RwLock::new(Core::new())),
                dir_path: opts.value_dir.clone(),
                opts,
                writeable_log_offset: AtomicU32::new(0),
            })
            // TODO: garbage collection
            // TODO: discard stats
        }
    }

    fn file_path(&self, fid: u32) -> PathBuf {
        vlog_file_path(&self.dir_path, fid)
    }

    fn populate_files_map(&self) -> Result<()> {
        // TODO: implement
        Ok(())
    }

    fn create_vlog_file(&self, core: &mut Core) -> Result<u32> {
        let mut core = self.core.write()?;
        let fid = core.max_fid + 1;
        let path = self.file_path(fid);
        let wal = Wal::open(path, self.opts.clone())?;
        // TODO: only create new files
        assert!(core.files_map.insert(fid, wal).is_none());
        assert!(core.max_fid < fid);
        core.max_fid = fid;
        // TODO: add vlog header
        self.writeable_log_offset
            .store(0, std::sync::atomic::Ordering::SeqCst);
        core.num_entries_written = 0;
        Ok(fid)
    }

    fn sorted_fids(&self) -> Vec<u32> {
        let core = self.core.read().unwrap();
        let mut to_be_deleted = HashSet::new();
        for fid in core.files_to_delete.iter().cloned() {
            to_be_deleted.insert(fid);
        }
        let mut result = vec![];
        for (fid, _) in core.files_map.iter() {
            if to_be_deleted.get(fid).is_none() {
                result.push(*fid);
            }
        }
        result.sort();
        result
    }

    fn open(&self) -> Result<()> {
        self.populate_files_map()?;
        if self.core.read()?.files_map.len() == 0 {
            let core_arc = self.core.clone();
            let mut core = core_arc.write()?;
            self.create_vlog_file(&mut core)?;
        }
        // TODO find empty files and iterate vlogs
        Ok(())
    }

    fn w_offset(&self) -> u32 {
        self.writeable_log_offset.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// write is thread-unsafe and should not be called concurrently
    pub fn write(&self, requests: &[Request]) -> Result<()> {
        // TODO: validate writes
        // TODO: refine lock design
        let mut core = self.core.write()?;
        let mut current_log_id = core.max_fid;

        // TODO: sync writes

        let write = |buf: &[u8], current_log: &mut Wal| -> Result<()> {
            if buf.is_empty() {
                return Ok(());
            }
            let n = buf.len() as u32;
            let start = self
                .writeable_log_offset
                .fetch_add(n, std::sync::atomic::Ordering::SeqCst);
            let end_offset = start + n;
            if end_offset >= current_log.size() {
                return Err(Error::TxnTooBig(format!(
                    "end_offset: {}, len: {}",
                    end_offset,
                    current_log.size()
                )));
            }
            (&mut current_log.data()[start as usize..end_offset as usize])
                .clone_from_slice(buf);
            current_log.set_size(end_offset);
            Ok(())
        };

        let mut to_disk = |current_log_id: u32, current_log: &mut Wal| -> Result<u32> {
            if self.w_offset() as u64 > self.opts.value_log_file_size
                || core.num_entries_written > self.opts.value_log_max_entries
            {
                current_log.done_writing(self.w_offset())?;
                Ok(self.create_vlog_file(&mut core)?)
            } else {
                Ok(current_log_id)
            }
        };

        let mut buf = BytesMut::new();
        for req in requests {
            let mut req = req.clone();
            req.ptrs.clear();

            let mut written = 0;
            let current_log = core.files_map.get_mut(&current_log_id).unwrap();

            for mut entry in req.entries {
                buf.clear();

                if self.opts.skip_vlog(&entry) {
                    req.ptrs.push(ValuePointer::default());
                    continue;
                }

                let mut p = ValuePointer::default();

                p.file_id = current_log_id;
                p.offset = self.w_offset();

                let orig_meta = entry.meta;
                entry.meta = entry.meta & (! value::VALUE_FIN_TXN | value::VALUE_TXN);

                let plen = Wal::encode_entry(&mut buf, &entry);
                entry.meta = orig_meta;
                p.len = plen as u32;
                req.ptrs.push(p);
                write(&buf, current_log)?;

                written += 1;
            }

            to_disk(current_log_id, current_log)?;

            core.num_entries_written += written;
        }

        Ok(())
    }
}
