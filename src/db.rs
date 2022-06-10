mod opt;

use std::{
    collections::VecDeque,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc, RwLock,
    },
};

use bytes::{Bytes, BytesMut};
use crossbeam_channel::{bounded, select, Receiver, Sender};
use log::{debug, info};
pub use opt::AgateOptions;
use skiplist::Skiplist;
use yatp::task::callback::Handle;

use super::{
    memtable::{MemTable, MemTables},
    Result,
};
use crate::{
    closer::Closer,
    entry::Entry,
    get_ts,
    levels::LevelsController,
    manifest::ManifestFile,
    ops::oracle::Oracle,
    util::make_comparator,
    value::{self, Request, Value},
    value_log::ValueLog,
    wal::Wal,
    Error,
};

const MEMTABLE_FILE_EXT: &str = ".mem";
const KV_WRITE_CH_CAPACITY: usize = 1000;

struct Closers {
    writes: Closer,
}

pub struct Core {
    closers: Closers,

    mts: RwLock<MemTables>,

    next_mem_fid: AtomicUsize,

    pub(crate) opts: AgateOptions,
    pub(crate) manifest: Arc<ManifestFile>,
    pub(crate) lvctl: LevelsController,
    pub(crate) vlog: Arc<Option<ValueLog>>,
    write_channel: (Sender<Request>, Receiver<Request>),

    block_writes: AtomicBool,
    is_closed: AtomicBool,

    pub(crate) orc: Arc<Oracle>,
}

#[derive(Clone)]
pub struct Agate {
    pub(crate) core: Arc<Core>,
    closer: Closer,
    pub(crate) pool: Arc<yatp::ThreadPool<yatp::task::callback::TaskCell>>,
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
        // TODOL Flush.

        let closer = Closer::new();
        let pool = Arc::new(
            yatp::Builder::new("agatedb")
                .max_thread_count(core.opts.num_compactors * 8 + 2)
                .min_thread_count(core.opts.num_compactors * 5 + 2)
                .build_callback_pool(),
        );

        let agate = Self {
            core,
            closer: closer.clone(),
            pool,
        };

        let core = agate.core.clone();
        let pool = agate.pool.clone();
        agate.pool.spawn(move |_: &mut Handle<'_>| {
            core.do_writes(core.closers.writes.clone(), core.clone(), pool)
                .unwrap()
        });

        agate.core.lvctl.start_compact(closer, agate.pool.clone());

        agate
    }
}

impl Core {
    pub(crate) fn new(opts: &AgateOptions) -> Result<Self> {
        let orc = Arc::new(Oracle::new(opts));
        let manifest = Arc::new(ManifestFile::open_or_create_manifest_file(opts)?);
        let lvctl = LevelsController::new(opts, manifest.clone(), orc.clone())?;

        let (imm_tables, mut next_mem_fid) = Self::open_mem_tables(opts)?;
        let mt = Self::open_mem_table(opts, next_mem_fid)?;
        next_mem_fid += 1;

        let core = Self {
            closers: Closers {
                writes: Closer::new(),
            },
            mts: RwLock::new(MemTables::new(Arc::new(mt), imm_tables)),
            next_mem_fid: AtomicUsize::new(next_mem_fid),
            opts: opts.clone(),
            manifest,
            lvctl,
            vlog: Arc::new(ValueLog::new(opts.clone())?),
            write_channel: bounded(KV_WRITE_CH_CAPACITY),
            block_writes: AtomicBool::new(false),
            is_closed: AtomicBool::new(false),
            orc,
        };

        // TODO: Initialize other structures.
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

    pub(crate) fn get(&self, key: &Bytes) -> Result<Value> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        let view = self.mts.read()?.view();

        let mut max_value = Value::default();
        let version = get_ts(key);

        for table in view.tables() {
            let mut value = Value::default();

            if let Some((key, value_data)) = table.get_with_key(key) {
                value.decode(value_data.clone());
                value.version = get_ts(key);

                if value.meta == 0 && value.value.is_empty() {
                    continue;
                }
                if value.version == version {
                    return Ok(value);
                }
                if max_value.version < value.version {
                    max_value = value;
                }
            }
        }

        // max_value will be used in level controller.
        self.lvctl.get(key, max_value, 0)
    }

    /// `write_to_lsm` will only be called in write thread (or write coroutine).
    ///
    /// By using a fine-grained lock approach, writing to LSM tree acquires:
    /// 1. read lock of memtable list (only block flush)
    /// 2. write lock of mutable memtable WAL (won't block mut-table read).
    /// 3. level controller lock (TBD)
    pub(crate) fn write_to_lsm(&self, request: Request) -> Result<()> {
        // TODO: Check entries and pointers.

        let memtables = self.mts.read()?;
        let mut_table = memtables.table_mut();

        for (idx, entry) in request.entries.into_iter().enumerate() {
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
                let mut vptr_buf = BytesMut::new();
                request.ptrs[idx].encode(&mut vptr_buf);
                // Write pointer to memtable.
                mut_table.put(
                    entry.key,
                    Value {
                        value: vptr_buf.freeze(),
                        meta: entry.meta | value::VALUE_POINTER,
                        user_meta: entry.user_meta,
                        expires_at: entry.expires_at,
                        version: 0,
                    },
                )?;
            }
        }

        if self.opts.sync_writes {
            mut_table.sync_wal()?;
        }
        Ok(())
    }

    /// Calling ensure_room_for_write requires locking whole memtable
    pub fn ensure_room_for_write(&self) -> Result<()> {
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

        mts.use_new_table(Arc::new(memtable));

        debug!(
            "memtable flushed, total={}, mt.size={}",
            mts.nums_of_memtable(),
            mem_size
        );

        Ok(())
    }

    pub(crate) fn get_mem_tables(&self) -> Vec<Arc<MemTable>> {
        unimplemented!()
    }

    /// Write requests should be only called in one thread. By calling this
    /// function, requests will be written into the LSM tree.
    ///
    // TODO: ensure only one thread calls this function by using Mutex.
    pub fn write_requests(&self, mut requests: Vec<Request>) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }

        #[allow(clippy::needless_collect)]
        let dones: Vec<_> = requests.iter().map(|x| x.done.clone()).collect();

        let f = || {
            if let Some(ref vlog) = *self.vlog {
                vlog.write(&mut requests)?;
            }

            // TODO: Process subscriptions.

            let mut cnt = 0;

            // Writing to LSM.
            for req in requests {
                if req.entries.is_empty() {
                    continue;
                }

                cnt += req.entries.len();

                while self.ensure_room_for_write().is_err() {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }

                self.write_to_lsm(req)?;
            }

            debug!("{} entries written", cnt);
            Ok(())
        };

        let result = f();

        for done in dones.into_iter().flatten() {
            done.send(result.clone()).unwrap();
        }

        result
    }

    pub(crate) fn send_to_write_channel(
        &self,
        entries: Vec<Entry>,
    ) -> Result<Receiver<Result<()>>> {
        if self.block_writes.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::CustomError(
                "Writes are blocked, possibly due to dropping all data or close".to_string(),
            ));
        }

        let mut count = 0;
        let mut size = 0;
        for entry in &entries {
            size += entry.estimate_size(self.opts.value_threshold) as u64;
            count += 1;
        }
        if count >= self.opts.max_batch_count || size >= self.opts.max_batch_size {
            return Err(Error::TxnTooBig);
        }

        let (tx, rx) = bounded(1);
        // TODO: Get request from request pool.
        let req = Request {
            entries,
            ptrs: vec![],
            done: Some(tx),
        };
        self.write_channel.0.send(req)?;
        Ok(rx)
    }

    fn do_writes(
        &self,
        closer: Closer,
        core: Arc<Self>,
        pool: Arc<yatp::ThreadPool<yatp::task::callback::TaskCell>>,
    ) -> Result<()> {
        info!("Start doing writes.");

        let (pending_tx, pending_rx) = bounded(1);

        const STATUS_WRITE: usize = 0;
        const STATUS_CLOSED: usize = 1;

        let mut reqs = Vec::with_capacity(10);

        let status = loop {
            let req;

            // We wait until there is at least one request.
            select! {
                recv(core.write_channel.1) -> req_recv => {
                    req = req_recv.unwrap();
                }
                recv(closer.get_receiver()) -> _ => {
                    break STATUS_CLOSED;
                }
            }

            reqs.push(req);

            let status = loop {
                if reqs.len() >= 3 * KV_WRITE_CH_CAPACITY {
                    pending_tx.send(()).unwrap();
                    break STATUS_WRITE;
                }

                select! {
                    // Either push to pending, or continue to pick from write_channel.
                    recv(core.write_channel.1) -> req => {
                        let req = req.unwrap();
                        reqs.push(req);
                    }
                    send(pending_tx, ()) -> _ => {
                        break STATUS_WRITE;
                    }
                    recv(closer.get_receiver()) -> _ => {
                        break STATUS_CLOSED;
                    }
                }
            };

            if status == STATUS_CLOSED {
                break STATUS_CLOSED;
            } else if status == STATUS_WRITE {
                let rx = pending_rx.clone();
                let reqs = std::mem::replace(&mut reqs, Vec::with_capacity(10));
                let core = core.clone();
                pool.spawn(move |_: &mut Handle<'_>| {
                    if let Err(err) = core.write_requests(reqs) {
                        log::error!("failed to write: {:?}", err);
                    }
                    rx.recv().ok();
                })
            }
        };

        if status == STATUS_CLOSED {
            // All the pending request are drained.
            // Don't close the write_channel, because it has be used in several places.
            loop {
                select! {
                    recv(core.write_channel.1) -> req => {
                        reqs.push(req.unwrap());
                    }
                    default => {
                        if let Err(err) = core.write_requests(reqs) {
                            log::error!("failed to write: {:?}", err);
                        }
                        return Ok(());
                    }
                }
            }
        }

        unreachable!()
    }
}

impl Agate {
    pub fn get(&self, key: &Bytes) -> Result<Value> {
        self.core.get(key)
    }

    pub fn write_to_lsm(&self, request: Request) -> Result<()> {
        self.core.write_to_lsm(request)
    }
}

#[cfg(test)]
pub(crate) mod tests;
