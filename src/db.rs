use super::memtable::{MemTable, MemTables};
use super::{Error, Result};
use crate::entry::Entry;
use crate::format::get_ts;
use crate::levels::LevelController;
use crate::opt;
use crate::util::{has_any_prefixes, make_comparator};
use crate::value::{self, Request, Value};
use crate::value_log::ValueLog;
use crate::wal::Wal;
use crate::{Table, TableBuilder, TableOptions};
use bytes::Bytes;
use skiplist::Skiplist;
use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::sync::RwLock;

pub struct Core {
    mts: RwLock<MemTables>,
    pub(crate) opts: AgateOptions,
    next_mem_fid: AtomicUsize,
    vlog: ValueLog,
    lvctl: LevelController,
}

#[derive(Clone)]
pub struct Agate {
    core: Arc<Core>,
}

struct FlushTask {
    mt: MemTable,
    drop_prefixes: Vec<Bytes>,
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
}

#[derive(Clone)]
pub struct AgateOptions {
    pub path: PathBuf,
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

    pub value_log_file_size: u64,
    pub value_log_max_entries: u32,

    pub checksum_mode: opt::ChecksumVerificationMode,
}

impl Default for AgateOptions {
    fn default() -> Self {
        Self {
            create_if_not_exists: false,
            path: PathBuf::new(),
            // memtable options
            mem_table_size: 64 << 20,
            base_table_size: 2 << 20,
            base_level_size: 10 << 20,
            table_size_multiplier: 2,
            level_size_multiplier: 10,
            max_levels: 7,
            // agate options
            num_memtables: 20,
            in_memory: false,
            sync_writes: false,
            value_threshold: 1 << 10,
            value_log_file_size: 1 << 30 - 1,
            value_log_max_entries: 1000000,
            checksum_mode: opt::ChecksumVerificationMode::NoVerification,
            block_size: 4 << 10,
            bloom_false_positive: 0.01,
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

        Ok(())
    }

    pub fn open<P: AsRef<Path>>(&mut self, path: P) -> Result<Agate> {
        self.fix_options()?;

        self.path = path.as_ref().to_path_buf();

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
        Ok(Agate {
            core: Arc::new(Core::new(self.clone())?),
        })
    }

    fn skip_vlog(&self, entry: &Entry) -> bool {
        entry.value.len() < self.value_threshold
    }

    fn arena_size(&self) -> u64 {
        // TODO: take other options into account
        self.mem_table_size as u64
    }
}

impl Core {
    fn new(opts: AgateOptions) -> Result<Self> {
        // create first mem table
        let mt = Self::open_mem_table(&opts.path, opts.clone(), 0)?;

        // create agate core
        let core = Self {
            mts: RwLock::new(MemTables::new(mt, VecDeque::new())),
            opts,
            next_mem_fid: AtomicUsize::new(1),
            vlog: ValueLog::new(),
            lvctl: LevelController {},
        };

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
            return Ok(MemTable::new(skl, None, opts.clone()));
        }
        let wal = Wal::open(path, opts.clone())?;
        // TODO: delete WAL when skiplist ref count becomes zero

        let mem_table = MemTable::new(skl, Some(wal), opts.clone());

        mem_table.update_skip_list()?;

        Ok(mem_table)
    }

    fn open_mem_tables(&mut self) -> Result<()> {
        if self.opts.in_memory {
            return Ok(());
        }
        // TODO: process on-disk structures
        Ok(())
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

    pub(crate) fn get(&self, key: &[u8]) -> Result<Value> {
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
        panic!("value not available in memtable") // Should get from level controller
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
        let mut mt = self.mts.write()?;
        let mut force_flush = false;

        if !force_flush && !self.opts.in_memory {
            if mt.table_mut().should_flush_wal()? {
                force_flush = true;
            }
        }

        let mem_size = mt.table_mut().skl.mem_size();

        if !force_flush && mt.table_mut().skl.mem_size() as u64 >= self.opts.mem_table_size {
            force_flush = true;
        }

        if !force_flush {
            return Ok(());
        }

        // TOO: use log library
        // TODO: use flush channel

        let memtable = self.new_mem_table()?;

        mt.use_new_table(memtable);

        println!(
            "memtable flushed, {}, mt.size = {}",
            mt.nums_of_memtable(),
            mem_size
        );

        Ok(())
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
        let table_opts = opt::build_table_options(&self);
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
            table = Table::create(&PathBuf::new(), builder.finish(), table_opts)?;
        }

        self.lvctl.add_l0_table(table);

        Ok(())
    }

    /// Write requests should be only called in one thread. By calling this
    /// function, requests will be written into the LSM tree.
    ///
    /// TODO: ensure only one thread calls this function by using Mutex.
    pub fn write_requests(&self, requests: Vec<Request>) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }

        // TODO: process subscriptions

        self.vlog.write(&requests)?;

        let mut cnt = 0;

        // writing to LSM
        for req in requests {
            if req.entries.is_empty() {
                continue;
            }
            cnt += req.entries.len();

            while let Err(err) = self.ensure_room_for_write() {
                std::thread::sleep(std::time::Duration::from_millis(10));
                println!("wait for room... {:?}", err)
            }

            self.write_to_lsm(req)?;
        }

        println!("{} entries written", cnt);

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

    pub fn write_requests(&self, request: Vec<Request>) -> Result<()> {
        self.core.write_requests(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::key_with_ts;
    use bytes::BytesMut;
    use tempdir::TempDir;

    #[test]
    fn test_build() {
        with_agate_test(|_| {});
    }

    fn with_agate_test(f: impl FnOnce(Agate) -> ()) {
        let tmp_dir = TempDir::new("agatedb").unwrap();
        let agate = AgateOptions::default()
            .create()
            .in_memory(false)
            .value_log_file_size(4096)
            .open(&tmp_dir)
            .unwrap();
        f(agate);
        tmp_dir.close().unwrap();
    }

    #[test]
    fn test_simple_get_put() {
        with_agate_test(|agate| {
            let key = key_with_ts(BytesMut::from("2333"), 0);
            let value = Bytes::from("2333333333333333");
            let req = Request {
                entries: vec![Entry::new(key.clone(), value.clone())],
            };
            agate.write_to_lsm(req).unwrap();
            let value = agate.get(&key).unwrap();
            assert_eq!(value.value, Bytes::from("2333333333333333"));
        });
    }

    fn generate_requests(n: usize) -> Vec<Request> {
        (0..n)
            .map(|i| Request {
                entries: vec![Entry::new(
                    key_with_ts(BytesMut::from(format!("{:08x}", i).as_str()), 0),
                    Bytes::from(i.to_string()),
                )],
            })
            .collect()
    }

    fn verify_requests(n: usize, agate: &Agate) {
        for i in 0..n {
            let value = agate
                .get(&key_with_ts(
                    BytesMut::from(format!("{:08x}", i).as_str()),
                    0,
                ))
                .unwrap();
            assert_eq!(value.value, i.to_string());
        }
    }

    #[test]
    fn test_flush_memtable() {
        with_agate_test(|agate| {
            agate.write_requests(generate_requests(3000)).unwrap();
            verify_requests(3000, &agate);
        });
    }
}
