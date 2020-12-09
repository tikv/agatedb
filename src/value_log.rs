use crate::value::{self, Request, ValuePointer};
use crate::wal::{Header, Wal};
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
    // TODO: use scheme like memtable to separate current vLog
    // and previous logs, so as to reduce usage of RwLock.
    files_map: HashMap<u32, Arc<RwLock<Wal>>>,
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

    fn create_vlog_file(&self) -> Result<(u32, Arc<RwLock<Wal>>)> {
        let mut core = self.core.write()?;
        let fid = core.max_fid + 1;
        let path = self.file_path(fid);
        let wal = Wal::open(path, self.opts.clone())?;
        // TODO: only create new files
        let wal = Arc::new(RwLock::new(wal));
        assert!(core.files_map.insert(fid, wal.clone()).is_none());
        assert!(core.max_fid < fid);
        core.max_fid = fid;
        // TODO: add vlog header
        self.writeable_log_offset
            .store(0, std::sync::atomic::Ordering::SeqCst);
        core.num_entries_written = 0;
        Ok((fid, wal))
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

    pub fn open(&self) -> Result<()> {
        self.populate_files_map()?;
        if self.core.read()?.files_map.len() == 0 {
            self.create_vlog_file()?;
        }
        // TODO find empty files and iterate vlogs
        Ok(())
    }

    fn w_offset(&self) -> u32 {
        self.writeable_log_offset
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// write is thread-unsafe and should not be called concurrently
    pub fn write(&self, requests: &mut [Request]) -> Result<()> {
        // TODO: validate writes

        let core = self.core.read()?;
        let mut current_log_id = core.max_fid;
        let mut current_log = core.files_map.get(&current_log_id).unwrap().clone();
        drop(core);

        // TODO: sync writes before return

        let write = |buf: &[u8], current_log: Arc<RwLock<Wal>>| -> Result<()> {
            let mut current_log = current_log.write()?;
            if buf.is_empty() {
                return Ok(());
            }
            let n = buf.len() as u32;
            let start = self
                .writeable_log_offset
                .fetch_add(n, std::sync::atomic::Ordering::SeqCst);
            let end_offset = start + n;

            // expand file size if space is not enough
            // TODO: handle value >= 4GB case
            if end_offset >= current_log.size() {
                current_log.set_len(end_offset as u64)?;
            }
            (&mut current_log.data()[start as usize..end_offset as usize]).clone_from_slice(buf);
            current_log.set_size(end_offset);
            Ok(())
        };

        let to_disk = |current_log: Arc<RwLock<Wal>>| -> Result<bool> {
            let mut current_log = current_log.write()?;
            let core = self.core.read()?;
            if self.w_offset() as u64 > self.opts.value_log_file_size
                || core.num_entries_written > self.opts.value_log_max_entries
            {
                current_log.done_writing(self.w_offset())?;
                Ok(true)
            } else {
                Ok(false)
            }
        };

        let mut buf = BytesMut::new();
        for req in requests.iter_mut() {
            req.ptrs.clear();

            let mut written = 0;

            for mut entry in req.entries.iter_mut() {
                buf.clear();

                if self.opts.skip_vlog(&entry) {
                    req.ptrs.push(ValuePointer::default());
                    continue;
                }

                let mut p = ValuePointer::default();

                p.file_id = current_log_id;
                p.offset = self.w_offset();

                let orig_meta = entry.meta;
                entry.meta = entry.meta & (!value::VALUE_FIN_TXN | value::VALUE_TXN);

                let plen = Wal::encode_entry(&mut buf, &entry);
                entry.meta = orig_meta;
                p.len = plen as u32;
                req.ptrs.push(p);
                write(&buf, current_log.clone())?;

                written += 1;
            }

            if to_disk(current_log.clone())? {
                let (log_id, log) = self.create_vlog_file()?;
                current_log_id = log_id;
                current_log = log;
            }

            let mut core = self.core.write()?;
            core.num_entries_written += written;
        }

        if to_disk(current_log.clone())? {
            self.create_vlog_file()?;
        }
        Ok(())
    }

    fn get_file(&self, value_ptr: &ValuePointer) -> Result<Arc<RwLock<Wal>>> {
        let core = self.core.read()?;
        let file = core.files_map.get(&value_ptr.file_id).cloned();
        if let Some(file) = file {
            let max_fid = core.max_fid;
            // TODO: read-only
            if value_ptr.file_id == max_fid {
                let current_offset = self.w_offset();
                if value_ptr.offset >= current_offset {
                    return Err(Error::CustomError(format!(
                        "invalid offset {} > {}",
                        value_ptr.offset, current_offset
                    )));
                }
            }

            Ok(file)
        } else {
            return Err(Error::CustomError(format!(
                "vlog {} not found",
                value_ptr.file_id
            )));
        }
    }

    /// Read data from vlogs.
    ///
    /// TODO: let user to decide when to unlock
    pub(crate) fn read(&self, value_ptr: ValuePointer) -> Result<Bytes> {
        let log_file = self.get_file(&value_ptr)?;
        let r = log_file.read()?;
        let mut buf = r.read(&value_ptr)?;
        drop(r);

        // TODO: verify checksum

        let mut header = Header::default();
        header.decode(&mut buf)?;
        let kv = buf;

        if (kv.len() as u32) < header.key_len + header.value_len {
            return Err(Error::CustomError(
                format!(
                    "invalud read vp: {:?}, kvlen {}, {}:{}",
                    value_ptr,
                    kv.len(),
                    header.key_len,
                    header.key_len + header.value_len
                )
                .to_string(),
            ));
        }

        Ok(kv)
    }
}
