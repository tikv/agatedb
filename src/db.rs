mod opt;

use std::{
    collections::VecDeque,
    fs,
    path::{Path, PathBuf},
    sync::{atomic::AtomicUsize, Arc, RwLock},
};

use log::debug;
pub use opt::AgateOptions;
use skiplist::Skiplist;

use super::{
    memtable::{MemTable, MemTables},
    Result,
};
use crate::{
    entry::Entry,
    util::make_comparator,
    value::{Request, Value},
    wal::Wal,
};

const MEMTABLE_FILE_EXT: &str = ".mem";

pub struct Core {
    mts: RwLock<MemTables>,
    opts: AgateOptions,
    next_mem_fid: AtomicUsize,
}

#[derive(Clone)]
pub struct Agate {
    core: Arc<Core>,
}

impl Agate {
    /*
    pub fn get_with_ts(&self, key: &[u8], ts: u64) -> Result<Option<Bytes>> {
        let key = format::key_with_ts(key, ts);
        let view = self.core.memtable.view();
        if let Some(value) = view.get(&key) {
            return Ok(Some(value.clone()));
        }
        unimplemented!()
    }
    */
}

impl Core {
    fn new(_opts: AgateOptions) -> Result<Self> {
        unimplemented!()
    }

    fn memtable_file_path(opts: &AgateOptions, file_id: usize) -> PathBuf {
        opts.dir
            .join(format!("{:05}{}", file_id, MEMTABLE_FILE_EXT))
    }

    fn open_mem_table(opts: &AgateOptions, file_id: usize) -> Result<MemTable> {
        let path = Self::memtable_file_path(opts, file_id);
        let c = make_comparator();
        // TODO: refactor skiplist to use `u64`
        let skl = Skiplist::with_capacity(c, opts.arena_size() as usize);

        // We don't need to create the WAL for the skiplist in in-memory mode so return the memtable.
        if opts.in_memory {
            return Ok(MemTable::new(skl, None, opts.clone()));
        }

        let wal = Wal::open(path, opts.clone())?;
        // TODO: delete WAL when skiplist ref count becomes zero

        let mem_table = MemTable::new(skl, Some(wal), opts.clone());

        mem_table.update_skip_list()?;

        Ok(mem_table)
    }

    fn open_mem_tables(opts: &AgateOptions) -> Result<(VecDeque<Arc<MemTable>>, usize)> {
        // We don't need to open any tables in in-memory mode.
        if opts.in_memory {
            return Ok((VecDeque::new(), 0));
        }

        let mut fids = vec![];
        let mut mts = VecDeque::new();

        for file in fs::read_dir(&opts.dir)? {
            let file = file?;
            let filename_ = file.file_name();
            let filename = filename_.to_string_lossy();
            if filename.ends_with(MEMTABLE_FILE_EXT) {
                let end = filename.len() - MEMTABLE_FILE_EXT.len();
                let fid: usize = filename[end - 5..end].parse().unwrap();
                fids.push(fid);
            }
        }

        fids.sort_unstable();

        for fid in &fids {
            let memtable = Self::open_mem_table(opts, *fid)?;
            mts.push_back(Arc::new(memtable));
        }

        let mut next_mem_fid = 0;

        if !fids.is_empty() {
            next_mem_fid = *fids.last().unwrap();
        }

        next_mem_fid += 1;

        Ok((mts, next_mem_fid))
    }

    fn new_mem_table(&self) -> Result<MemTable> {
        let file_id = self
            .next_mem_fid
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mt = Self::open_mem_table(&self.opts, file_id)?;
        Ok(mt)
    }

    pub fn is_closed(&self) -> bool {
        // TODO: check db closed
        false
    }

    pub(crate) fn get(&self, _key: &[u8]) -> Result<Value> {
        unimplemented!()
    }

    /// `write_to_lsm` will only be called in write thread (or write coroutine).
    ///
    /// By using a fine-grained lock approach, writing to LSM tree acquires:
    /// 1. read lock of memtable list (only block flush)
    /// 2. write lock of mutable memtable WAL (won't block mut-table read).
    /// 3. level controller lock (TBD)
    pub(crate) fn write_to_lsm(&self, _request: Request) -> Result<()> {
        unimplemented!()
    }

    /// Calling ensure_room_for_write requires locking whole memtable
    pub fn ensure_room_for_write(&mut self) -> Result<()> {
        // we do not need to force flush memtable in in-memory mode as WAL is None.
        let mut mts = self.mts.write()?;
        let mut force_flush = false;

        if !self.opts.in_memory && mts.table_mut().should_flush_wal()? {
            force_flush = true;
        }

        let mem_size = mts.table_mut().skl.mem_size() as u64;

        if !force_flush && mem_size < self.opts.mem_table_size {
            return Ok(());
        }

        // TODO: use flush channel

        let memtable = self.new_mem_table()?;

        mts.use_new_table(memtable);

        debug!(
            "memtable flushed, total={}, mt.size={}",
            mts.nums_of_memtable(),
            mem_size
        );

        Ok(())
    }
}

impl Agate {
    pub fn get(&self, key: &[u8]) -> Result<Value> {
        self.core.get(key)
    }

    pub fn write_to_lsm(&self, request: Request) -> Result<()> {
        self.core.write_to_lsm(request)
    }

    pub fn open<P: AsRef<Path>>(mut opts: AgateOptions, path: P) -> Result<Self> {
        opts.fix_options()?;

        opts.dir = path.as_ref().to_path_buf();

        if !opts.in_memory && !opts.dir.exists() {
            fs::create_dir_all(&opts.dir)?;
            // TODO: create wal path, acquire database path lock
        }

        // TODO: open or create manifest
        Ok(Agate {
            core: Arc::new(Core::new(opts)?),
        })
    }
}

#[cfg(test)]
pub(crate) mod tests;
