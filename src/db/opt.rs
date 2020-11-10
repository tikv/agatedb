use super::*;

#[derive(Clone)]
pub struct AgateOptions {
    pub path: PathBuf,
    // TODO: docs
    pub in_memory: bool,
    pub sync_writes: bool,

    pub create_if_not_exists: bool,
    pub num_memtables: usize,
    pub mem_table_size: u64,

    pub value_threshold: usize,
    pub value_log_file_size: u64,
    pub value_log_max_entries: u32,
}

impl Default for AgateOptions {
    fn default() -> Self {
        Self {
            create_if_not_exists: false,
            path: PathBuf::new(),
            mem_table_size: 64 << 20,
            num_memtables: 20,
            in_memory: false,
            sync_writes: false,
            value_threshold: 1 << 10,
            value_log_file_size: 1 << 30 - 1,
            value_log_max_entries: 1000000,
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
