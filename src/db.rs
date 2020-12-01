use super::memtable::{MemTable, MemTables};
use super::{Error, Result};
use crate::closer::Closer;
use crate::entry::Entry;
use crate::format::get_ts;
use crate::levels::LevelsController;
use crate::manifest::ManifestFile;
use crate::ops::oracle::Oracle;
use crate::opt;
use crate::util::{has_any_prefixes, make_comparator};
use crate::value::{self, Request, Value};
use crate::value_log::ValueLog;
use crate::wal::Wal;
use crate::{Table, TableBuilder, TableOptions};

use bytes::{Bytes, BytesMut};
use crossbeam_channel::{Receiver, Sender};
use skiplist::{Skiplist, MAX_NODE_SIZE};
use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use yatp::task::callback::Handle;

pub struct Core {
    mts: RwLock<MemTables>,
    pub(crate) opts: AgateOptions,
    next_mem_fid: AtomicUsize,
    vlog: Option<ValueLog>,
    lvctl: LevelsController,
    flush_channel: (Sender<Option<FlushTask>>, Receiver<Option<FlushTask>>),
    manifest: Arc<ManifestFile>,
    pub(crate) orc: Arc<Oracle>,
}

pub struct Agate {
    pub(crate) core: Arc<Core>,
    closer: Closer,
    pool: yatp::ThreadPool<yatp::task::callback::TaskCell>,
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

const MEMTABLE_FILE_EXT: &str = ".mem";

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
        let closer = Closer::new();
        let agate = Self {
            core,
            closer: closer.clone(),
            pool: yatp::Builder::new("agatedb").build_callback_pool(),
        };

        agate
            .pool
            .spawn(move |_: &mut Handle<'_>| flush_core.flush_memtable().unwrap());

        agate
            .core
            .clone()
            .lvctl
            .start_compact(closer.clone(), &agate.pool);

        agate
    }

    fn close(&self) {
        // TODO: use closer for flush channel
        self.core.flush_channel.0.send(None).unwrap();
        self.closer.close();
    }
}

impl Drop for Agate {
    fn drop(&mut self) {
        self.close();
        self.pool.shutdown();
    }
}

#[derive(Clone)]
pub struct AgateOptions {
    pub path: PathBuf,
    pub value_dir: PathBuf,
    // TODO: docs
    pub in_memory: bool,
    pub sync_writes: bool,
    pub create_if_not_exists: bool,

    // Memtable options
    pub mem_table_size: u64,
    pub base_table_size: u64,
    pub base_level_size: u64,
    pub level_size_multiplier: usize,
    pub table_size_multiplier: usize,
    pub max_levels: usize,

    pub value_threshold: usize,
    pub num_memtables: usize,

    pub block_size: usize,
    pub bloom_false_positive: f64,

    pub num_level_zero_tables: usize,
    pub num_level_zero_tables_stall: usize,

    pub value_log_file_size: u64,
    pub value_log_max_entries: u32,

    pub num_compactors: usize,

    pub checksum_mode: opt::ChecksumVerificationMode,

    pub detect_conflicts: bool,

    pub(crate) managed_txns: bool,

    pub(crate) max_batch_count: u64,
    pub(crate) max_batch_size: u64,
}

impl Default for AgateOptions {
    fn default() -> Self {
        Self {
            create_if_not_exists: false,
            path: PathBuf::new(),
            value_dir: PathBuf::new(),
            // memtable options
            mem_table_size: 64 << 20,
            base_table_size: 2 << 20,
            base_level_size: 10 << 20,
            table_size_multiplier: 2,
            level_size_multiplier: 10,
            max_levels: 7,
            // agate options
            // although MEMTABLE_VIEW_MAX is 20, it is possible that
            // during the compaction process, memtable would exceed num_memtables.
            // therefore, set it to 5 for now.
            num_memtables: 5,
            in_memory: false,
            sync_writes: false,
            value_threshold: 1 << 10,
            value_log_file_size: 1 << 30 - 1,
            value_log_max_entries: 1000000,
            checksum_mode: opt::ChecksumVerificationMode::NoVerification,
            block_size: 4 << 10,
            bloom_false_positive: 0.01,
            num_level_zero_tables: 5,
            num_level_zero_tables_stall: 15,
            num_compactors: 4,
            detect_conflicts: true,
            managed_txns: false,
            max_batch_count: 0,
            max_batch_size: 0,
        }
        // TODO: add other options
    }
}

impl AgateOptions {
    pub fn create(&mut self) -> &mut AgateOptions {
        self.create_if_not_exists = true;
        self
    }

    pub fn path<P: Into<PathBuf>>(&mut self, p: P) -> &mut AgateOptions {
        self.path = p.into();
        self
    }

    pub fn num_memtables(&mut self, num_memtables: usize) -> &mut AgateOptions {
        self.num_memtables = num_memtables;
        self
    }

    pub fn in_memory(&mut self, in_memory: bool) -> &mut AgateOptions {
        self.in_memory = in_memory;
        self
    }

    pub fn sync_writes(&mut self, sync_writes: bool) -> &mut AgateOptions {
        self.sync_writes = sync_writes;
        self
    }

    pub fn value_log_file_size(&mut self, value_log_file_size: u64) -> &mut AgateOptions {
        self.value_log_file_size = value_log_file_size;
        self
    }

    pub fn value_log_max_entries(&mut self, value_log_max_entries: u32) -> &mut AgateOptions {
        self.value_log_max_entries = value_log_max_entries;
        self
    }

    fn fix_options(&mut self) -> Result<()> {
        if self.in_memory {
            // TODO: find a way to check if path is set, if set, then panic with ConfigError
            self.sync_writes = false;
        }

        self.max_batch_size = (15 * self.mem_table_size) / 100;
        self.max_batch_count = self.max_batch_size / MAX_NODE_SIZE as u64;

        Ok(())
    }

    // open is by-default OpenManaged
    pub fn open<P: AsRef<Path>>(&mut self, path: P) -> Result<Agate> {
        self.fix_options()?;
        self.managed_txns = true;

        self.path = path.as_ref().to_path_buf();
        // TODO: allow specify value dir
        self.value_dir = path.as_ref().to_path_buf();

        if !self.in_memory {
            if !self.path.exists() {
                if !self.create_if_not_exists {
                    return Err(Error::Config(format!("{:?} doesn't exist", self.path)));
                }
                fs::create_dir_all(&self.path)?;
            }
            // TODO: create wal path, acquire database path lock
        }

        // TODO: open or create manifest
        Ok(Agate::new(Arc::new(Core::new(self.clone())?)))
    }

    pub(crate) fn skip_vlog(&self, entry: &Entry) -> bool {
        entry.value.len() < self.value_threshold
    }

    fn arena_size(&self) -> u64 {
        // TODO: take other options into account
        // TODO: don't just multiply 2
        self.mem_table_size as u64 * 2
    }
}

impl Core {
    fn new(opts: AgateOptions) -> Result<Self> {
        // create first mem table

        let manifest = Arc::new(ManifestFile::open_or_create_manifest_file(&opts)?);
        let lvctl = LevelsController::new(opts.clone(), manifest.clone())?;

        let (imm_tables, mut next_mem_fid) = Self::open_mem_tables(&opts)?;
        let mt = Self::open_mem_table(&opts.path, opts.clone(), next_mem_fid)?;
        next_mem_fid += 1;

        // create agate core
        let mut core = Self {
            mts: RwLock::new(MemTables::new(Arc::new(mt), imm_tables)),
            opts: opts.clone(),
            next_mem_fid: AtomicUsize::new(next_mem_fid),
            vlog: ValueLog::new(opts.clone()),
            lvctl,
            flush_channel: crossbeam_channel::bounded(opts.num_memtables),
            manifest,
            orc: Arc::new(Oracle::new(opts.managed_txns, opts.detect_conflicts)),
        };

        if let Some(ref mut vlog) = core.vlog {
            vlog.open()?
        }

        // TODO: initialize other structures

        Ok(core)
    }

    fn memtable_file_path(base_path: &Path, file_id: usize) -> PathBuf {
        base_path
            .to_path_buf()
            .join(format!("{:05}{}", file_id, MEMTABLE_FILE_EXT))
    }

    fn open_mem_table<P: AsRef<Path>>(
        base_path: P,
        opts: AgateOptions,
        file_id: usize,
    ) -> Result<MemTable> {
        let path = Self::memtable_file_path(base_path.as_ref(), file_id);
        let c = make_comparator();
        // TODO: refactor skiplist to use `u64`
        let skl = Skiplist::with_capacity(c, opts.arena_size() as u32);
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
        if opts.in_memory {
            return Ok((VecDeque::new(), 0));
        }

        let mut fids = vec![];
        let mut mts = VecDeque::new();

        for file in fs::read_dir(&opts.path)? {
            let file = file?;
            let filename_ = file.file_name();
            let filename = filename_.to_string_lossy();
            if filename.ends_with(MEMTABLE_FILE_EXT) {
                let end = filename.len() - MEMTABLE_FILE_EXT.len();
                let fid: usize = filename[end - 5..end].parse().unwrap();
                fids.push(fid);
            }
        }
        fids.sort();

        for fid in &fids {
            let memtable = Self::open_mem_table(&opts.path, opts.clone(), *fid)?;
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
        let fid = self
            .next_mem_fid
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mt = Self::open_mem_table(&self.opts.path, self.opts.clone(), fid)?;
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
                value.decode(value_data);
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

        // max_value will be used in level controller
        self.lvctl.get(&key, max_value, 0)
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
                // write pointer to memtable
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
        // we do not need to force flush memtable in in-memory mode as WAL is None
        let mut mts = self.mts.write()?;
        let mut force_flush = false;

        if !force_flush && !self.opts.in_memory {
            if mts.table_mut().should_flush_wal()? {
                force_flush = true;
            }
        }

        let mem_size = mts.table_mut().skl.mem_size();

        if !force_flush && mts.table_mut().skl.mem_size() as u64 >= self.opts.mem_table_size {
            force_flush = true;
        }

        if !force_flush {
            return Ok(());
        }

        // TODO: use log library

        match self
            .flush_channel
            .0
            .try_send(Some(FlushTask::new(mts.table_mut().clone())))
        {
            Ok(_) => {
                let memtable = self.new_mem_table()?;

                mts.use_new_table(Arc::new(memtable));

                println!(
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
            vs.decode(iter.value());
            if vs.meta & value::VALUE_POINTER != 0 {
                panic!("value pointer not supported");
            }
            builder.add(iter.key(), vs, 0); // TODO: support vlog length
            iter.next();
        }
        builder
    }

    /// handle_flush_task must run serially.
    fn handle_flush_task(&self, ft: FlushTask) -> Result<()> {
        if ft.mt.skl.is_empty() {
            return Ok(());
        }
        let table_opts = opt::build_table_options(&self.opts);
        let mut builder = Self::build_l0_table(ft, table_opts.clone());

        if builder.is_empty() {
            builder.finish();
            return Ok(());
        }

        let file_id = self.lvctl.reserve_file_id();
        let table;

        if self.opts.in_memory {
            let data = builder.finish();
            table = Table::open_in_memory(data, file_id, table_opts)?;
        } else {
            table = Table::create(
                &crate::table::new_filename(file_id, &self.opts.path),
                builder.finish(),
                table_opts,
            )?;
        }

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
                        assert_eq!(flush_id, mts.table_imm(0).id());
                        mts.pop_imm();
                    }
                    Err(err) => {
                        println!("error while flushing memtable to disk: {:?}", err);
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
    pub fn write_requests(&self, mut requests: Vec<Request>) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }

        // TODO: process subscriptions

        if let Some(ref vlog) = self.vlog {
            vlog.write(&mut requests)?;
        }

        let mut cnt = 0;

        // writing to LSM
        for req in requests {
            if req.entries.is_empty() {
                continue;
            }
            cnt += req.entries.len();

            while let Err(_) = self.ensure_room_for_write() {
                std::thread::sleep(std::time::Duration::from_millis(10));
                // println!("wait for room... {:?}", err)
            }

            self.write_to_lsm(req)?;
        }

        // println!("{} entries written", cnt);

        Ok(())
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
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::format::key_with_ts;
    use crate::levels::tests::helper_dump_levels;
    use crate::value::ValuePointer;
    use bytes::BytesMut;
    use rand::prelude::*;
    use tempdir::TempDir;

    #[test]
    fn test_build() {
        with_agate_test(|_| {});
    }

    pub fn helper_dump_dir(path: &Path) {
        let mut result = vec![];
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_file() {
                result.push(path);
            }
        }
        result.sort();

        for path in result {
            println!("{:?}", path);
        }
    }

    pub fn with_agate_test(f: impl FnOnce(&mut Agate) -> () + Send + 'static) {
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let tmp_dir = TempDir::new("agatedb").unwrap();
            let mut options = AgateOptions::default();

            options
                .create()
                .in_memory(false)
                .value_log_file_size(4 << 20);

            options.mem_table_size = 1 << 14;
            // set base level size small enought to make the compactor flush L0 to L5 and L6
            options.base_level_size = 4 << 10;

            let mut agate = options.open(&tmp_dir).unwrap();
            f(&mut agate);
            println!("---agate directory---");
            helper_dump_dir(tmp_dir.path());
            helper_dump_levels(&agate.core.lvctl);
            drop(agate);
            println!("---after close---");
            helper_dump_dir(tmp_dir.path());
            tmp_dir.close().unwrap();
            tx.send(()).expect("failed to complete test");
        });

        match rx.recv_timeout(std::time::Duration::from_secs(60)) {
            Ok(_) => handle.join().expect("thread panic"),
            Err(err) => panic!("error: {:?}", err),
        }
    }

    #[test]
    fn test_simple_get_put() {
        with_agate_test(|agate| {
            let key = key_with_ts(BytesMut::from("2333"), 0);
            let value = Bytes::from("2333333333333333");
            let req = Request {
                entries: vec![Entry::new(key.clone(), value.clone())],
                ptrs: vec![],
            };
            agate.write_to_lsm(req).unwrap();
            let value = agate.get(&key).unwrap();
            assert_eq!(value.value, Bytes::from("2333333333333333"));
        });
    }

    fn with_payload(mut buf: BytesMut, payload: usize, fill_char: u8) -> Bytes {
        let mut payload_buf = vec![];
        payload_buf.resize(payload, fill_char);
        buf.extend_from_slice(&payload_buf);
        buf.freeze()
    }

    pub fn generate_requests(n: usize, payload: usize) -> Vec<Request> {
        let mut requests: Vec<Request> = (0..n)
            .map(|i| Request {
                entries: vec![Entry::new(
                    key_with_ts(BytesMut::from(format!("{:08x}", i).as_str()), 0),
                    with_payload(
                        BytesMut::from(format!("{:08}", i).as_str()),
                        payload,
                        (i % 256) as u8,
                    ),
                )],
                ptrs: vec![],
            })
            .collect();
        let mut rng = rand::thread_rng();
        requests[n / 2..].shuffle(&mut rng);
        requests
    }

    pub fn verify_requests(n: usize, agate: &Agate) {
        for i in 0..n {
            let key = key_with_ts(BytesMut::from(format!("{:08x}", i).as_str()), 0);
            let value = agate.get(&key).unwrap();

            assert!(!value.value.is_empty());

            if value.meta & value::VALUE_POINTER != 0 {
                let vlog = agate.core.vlog.as_ref().unwrap();
                let mut vptr = ValuePointer::default();
                vptr.decode(&value.value);
                let kv = vlog.read(vptr).unwrap();
                let key_length = key.len();
                let v_key = &kv[..key_length];
                let val = &kv[key_length..];
                assert_eq!(key, v_key);
                assert_eq!(&val[..8], format!("{:08}", i).as_bytes());
                for j in &val[8..] {
                    assert_eq!(*j, (i % 256) as u8);
                }
            } else {
                assert_eq!(&value.value[..8], format!("{:08}", i).as_bytes());
            }
        }
    }

    #[test]
    fn test_flush_memtable() {
        with_agate_test(|agate| {
            agate.write_requests(generate_requests(2000, 0)).unwrap();
            verify_requests(2000, &agate);
        });
    }

    #[test]
    fn test_flush_l1() {
        with_agate_test(|agate| {
            let requests = generate_requests(10000, 0);
            for request in requests.chunks(100) {
                agate.write_requests(request.to_vec()).unwrap();
            }
            println!("verifying requests...");
            verify_requests(10000, &agate);
        });
    }

    #[test]
    fn test_flush_memtable_bigvalue() {
        with_agate_test(|agate| {
            let requests = generate_requests(15, 1 << 20);
            for request in requests.chunks(4) {
                agate.write_requests(request.to_vec()).unwrap();
            }
            verify_requests(15, &agate);
        });
    }
}
