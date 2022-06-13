mod opt;

use std::{
    collections::VecDeque,
    fs,
    path::{Path, PathBuf},
    sync::{atomic::AtomicUsize, Arc, RwLock},
};

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender};
use log::debug;
pub use opt::AgateOptions;
use skiplist::Skiplist;
use yatp::task::callback::Handle;

use super::{
    manifest::ManifestFile,
    memtable::{MemTable, MemTables},
    opt::build_table_options,
    Error, Result,
};
use crate::{
    format::get_ts,
    levels::LevelsController,
    ops::oracle::Oracle,
    util::{has_any_prefixes, make_comparator},
    value::{self, Request, Value},
    value_log::ValueLog,
    wal::Wal,
    Table, TableBuilder, TableOptions,
};

const MEMTABLE_FILE_EXT: &str = ".mem";

pub struct Core {
    mts: RwLock<MemTables>,
    pub(crate) opts: AgateOptions,
    next_mem_fid: AtomicUsize,
    vlog: Arc<Option<ValueLog>>,
    lvctl: LevelsController,
    flush_channel: (Sender<Option<FlushTask>>, Receiver<Option<FlushTask>>),
}

pub struct Agate {
    pub(crate) core: Arc<Core>,
    pool: Arc<yatp::ThreadPool<yatp::task::callback::TaskCell>>,
}

struct FlushTask {
    mt: Arc<MemTable>,
    drop_prefixes: Vec<Bytes>,
}

impl FlushTask {
    pub fn new(mt: Arc<MemTable>) -> Self {
        Self {
            mt,
            drop_prefixes: vec![],
        }
    }
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

    fn new(core: Arc<Core>) -> Self {
        let flush_core = core.clone();

        let agate = Self {
            core,
            pool: Arc::new(yatp::Builder::new("agatedb").build_callback_pool()),
        };

        agate
            .pool
            .spawn(move |_: &mut Handle<'_>| flush_core.flush_memtable().unwrap());

        agate
    }

    fn close(&self) {
        self.core.flush_channel.0.send(None).unwrap();
    }
}

impl Drop for Agate {
    fn drop(&mut self) {
        self.close();
        self.pool.shutdown();
    }
}

impl Core {
    fn new(opts: AgateOptions) -> Result<Self> {
        // create first mem table
        // TODO: let orc = Arc::new(Oracle::new(opts.managed_txns, opts.detect_conflicts));
        let orc = Arc::new(Oracle::default());
        let manifest = Arc::new(ManifestFile::open_or_create_manifest_file(&opts)?);
        let lvctl = LevelsController::new(opts.clone(), manifest, orc)?;

        let (imm_tables, mut next_mem_fid) = Self::open_mem_tables(&opts)?;
        let mt = Self::open_mem_table(&opts, next_mem_fid)?;
        next_mem_fid += 1;

        // create agate core
        let core = Self {
            mts: RwLock::new(MemTables::new(Arc::new(mt), imm_tables)),
            opts: opts.clone(),
            next_mem_fid: AtomicUsize::new(next_mem_fid),
            vlog: Arc::new(ValueLog::new(opts.clone())?),
            lvctl,
            flush_channel: crossbeam_channel::bounded(opts.num_memtables),
        };

        // TODO: initialize other structures

        Ok(core)
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
            return Ok(MemTable::new(file_id, skl, None, opts.clone()));
        }

        let wal = Wal::open(path, opts.clone())?;
        // TODO: delete WAL when skiplist ref count becomes zero

        let mem_table = MemTable::new(file_id, skl, Some(wal), opts.clone());

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

    pub(crate) fn get(&self, key: &Bytes) -> Result<Value> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        let view = self.mts.read()?.view();
        let mut max_value = Value::default();

        let version = get_ts(key);

        for table in view.tables() {
            let mut value = Value::default();

            if let Some(value_data) = table.get(key) {
                value.decode(value_data.clone());
                if value.meta == 0 && value.value.is_empty() {
                    continue;
                }
                if value.version == version {
                    return Ok(value);
                }
                // max_vs.version == 0 means it is not assigned a value yet.
                if max_value.version == 0 || max_value.version < value.version {
                    max_value = value;
                }
            }
        }

        // max_value will be used in level controller
        self.lvctl.get(key, max_value, 0)
    }

    /// `write_to_lsm` will only be called in write thread (or write coroutine).
    ///
    /// By using a fine-grained lock approach, writing to LSM tree acquires:
    /// 1. read lock of memtable list (only block flush)
    /// 2. write lock of mutable memtable WAL (won't block mut-table read).
    /// 3. level controller lock (TBD)
    pub fn write_to_lsm(&self, request: Request) -> Result<()> {
        // TODO: check entries and pointers

        let memtables = self.mts.read()?;
        let mut_table = memtables.mut_table();

        for entry in request.entries.into_iter() {
            if self.opts.skip_vlog(&entry) {
                // deletion, tombstone, and small values
                mut_table.put(
                    entry.key,
                    Value {
                        value: entry.value,
                        meta: entry.meta & (!value::VALUE_POINTER),
                        user_meta: entry.user_meta,
                        expires_at: entry.expires_at,
                        version: 0,
                    },
                )?;
            } else {
                // write pointer to memtable
                mut_table.put(
                    entry.key,
                    Value {
                        value: Bytes::new(),
                        meta: entry.meta | value::VALUE_POINTER,
                        user_meta: entry.user_meta,
                        expires_at: entry.expires_at,
                        version: 0,
                    },
                )?;
                unimplemented!()
            }
        }
        if self.opts.sync_writes {
            mut_table.sync_wal()?;
        }
        Ok(())
    }

    /// Calling ensure_room_for_write requires locking whole memtable
    pub fn ensure_room_for_write(&self) -> Result<()> {
        // we do not need to force flush memtable in in-memory mode as WAL is None
        let mut mts = self.mts.write()?;
        let mut force_flush = false;

        if !force_flush && !self.opts.in_memory && mts.mut_table().should_flush_wal()? {
            force_flush = true;
        }

        let mem_size = mts.mut_table().skl.mem_size();

        if !force_flush && mem_size as u64 >= self.opts.mem_table_size {
            force_flush = true;
        }

        if !force_flush {
            return Ok(());
        }

        match self
            .flush_channel
            .0
            .try_send(Some(FlushTask::new(mts.mut_table())))
        {
            Ok(_) => {
                let memtable = self.new_mem_table()?;
                mts.use_new_table(Arc::new(memtable));

                debug!(
                    "memtable flushed, total={}, mt.size = {}",
                    mts.nums_of_memtable(),
                    mem_size
                );

                Ok(())
            }
            Err(_) => Err(Error::WriteNoRoom(())),
        }
    }

    /// build L0 table from memtable
    fn build_l0_table(ft: FlushTask, table_opts: TableOptions) -> TableBuilder {
        let mut iter = ft.mt.skl.iter_ref();
        let mut builder = TableBuilder::new(table_opts);
        iter.seek_to_first();
        while iter.valid() {
            if !ft.drop_prefixes.is_empty() && has_any_prefixes(iter.key(), &ft.drop_prefixes) {
                continue;
            }
            // TODO: reduce encode / decode by using something like flatbuffer
            let mut vs = Value::default();
            vs.decode(iter.value().clone());
            if vs.meta & value::VALUE_POINTER != 0 {
                panic!("value pointer not supported");
            }
            builder.add(iter.key(), &vs, 0); // TODO: support vlog length
            iter.next();
        }
        builder
    }

    /// handle_flush_task must run serially.
    fn handle_flush_task(&self, ft: FlushTask) -> Result<()> {
        if ft.mt.skl.is_empty() {
            return Ok(());
        }
        let table_opts = build_table_options(&self.opts);
        let builder = Self::build_l0_table(ft, table_opts.clone());

        if builder.is_empty() {
            builder.finish();
            return Ok(());
        }

        let file_id = self.lvctl.reserve_file_id();
        let table = if self.opts.in_memory {
            let data = builder.finish();
            Table::open_in_memory(data, file_id, table_opts)?
        } else {
            Table::create(
                &crate::table::new_filename(file_id, &self.opts.dir),
                builder.finish(),
                table_opts,
            )?
        };

        self.lvctl.add_l0_table(table)?;

        Ok(())
    }

    fn flush_memtable(&self) -> Result<()> {
        for ft in self.flush_channel.1.clone() {
            if let Some(ft) = ft {
                let flush_id = ft.mt.id();
                match self.handle_flush_task(ft) {
                    Ok(_) => {
                        let mut mts = self.mts.write()?;
                        assert_eq!(flush_id, mts.imm_table(0).id());
                        mts.pop_imm();
                    }
                    Err(err) => {
                        eprintln!("error while flushing memtable to disk: {:?}", err);
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Write requests should be only called in one thread. By calling this
    /// function, requests will be written into the LSM tree.
    ///
    /// TODO: ensure only one thread calls this function by using Mutex.
    #[allow(clippy::needless_collect)]
    pub fn write_requests(&self, mut requests: Vec<Request>) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }

        let dones: Vec<_> = requests.iter().map(|x| x.done.clone()).collect();

        let write = || {
            // TODO: process subscriptions

            if let Some(ref vlog) = *self.vlog {
                vlog.write(&mut requests)?;
            }

            let mut cnt = 0;

            // writing to LSM
            for req in requests {
                if req.entries.is_empty() {
                    continue;
                }
                cnt += req.entries.len();

                while self.ensure_room_for_write().is_err() {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    // eprintln!("wait for room... {:?}", err)
                }

                self.write_to_lsm(req)?;
            }

            debug!("{} entries written", cnt);

            Ok(())
        };

        let result = write();

        for done in dones.into_iter().flatten() {
            done.send(result.clone()).unwrap();
        }

        result
    }
}

impl Agate {
    pub fn get(&self, key: &Bytes) -> Result<Value> {
        self.core.get(key)
    }

    pub fn write_to_lsm(&self, request: Request) -> Result<()> {
        self.core.write_to_lsm(request)
    }

    pub fn write_requests(&self, request: Vec<Request>) -> Result<()> {
        self.core.write_requests(request)
    }

    pub fn open<P: AsRef<Path>>(mut opts: AgateOptions, path: P) -> Result<Self> {
        opts.fix_options()?;

        opts.dir = path.as_ref().to_path_buf();

        if !opts.in_memory && !opts.dir.exists() {
            fs::create_dir_all(&opts.dir)?;
            // TODO: create wal path, acquire database path lock
        }

        // TODO: open or create manifest
        Ok(Self::new(Arc::new(Core::new(opts)?)))
    }
}

#[cfg(test)]
pub(crate) mod tests;
