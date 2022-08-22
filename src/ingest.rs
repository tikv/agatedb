use std::{cmp::Ordering, fs, io, ops::RangeInclusive, path::Path, sync::Arc};

use bytes::Bytes;
use log::warn;
use skiplist::KeyComparator;

use crate::{
    db::Core, error::Result, format::user_key, opt::build_table_options, table, util::COMPARATOR,
    ChecksumVerificationMode, Table,
};

#[derive(Debug, Clone, Copy)]
pub enum PickLevelStrategy {
    BaseLevel,
    BottomLevel,
}

#[derive(Debug)]
pub struct IngestExternalFileOptions {
    pub commit_ts: u64,
    pub move_files: bool,
    pub failed_move_fall_back_to_copy: bool,
    pub verify_checksum: bool,
    pub pick_level_strategy: PickLevelStrategy,
}

impl Default for IngestExternalFileOptions {
    fn default() -> Self {
        Self {
            commit_ts: 0,
            move_files: false,
            failed_move_fall_back_to_copy: true,
            verify_checksum: true,
            pick_level_strategy: PickLevelStrategy::BaseLevel,
        }
    }
}

pub(crate) struct FileContext {
    /// file id alloc by [`LevelController`]
    pub(crate) id: u64,
    pub(crate) input_path: String,
    pub(crate) table: Option<Table>,
    pub(crate) moved_or_copied: bool,
    pub(crate) picked_level: usize,
}

pub(crate) struct IngestExternalFileTask {
    opts: IngestExternalFileOptions,
    core: Arc<Core>,
    files: Vec<FileContext>,
    version: Option<u64>,
    // whether files to ingest overlap with each other
    overlap: bool,
}

impl IngestExternalFileTask {
    pub(crate) fn new(core: Arc<Core>, files: &[&str], opts: IngestExternalFileOptions) -> Self {
        IngestExternalFileTask {
            opts,
            core,
            files: files
                .iter()
                .map(|str| FileContext {
                    id: 0,
                    input_path: str.to_string(),
                    table: None,
                    moved_or_copied: false,
                    picked_level: 0,
                })
                .collect(),
            version: None,
            overlap: false,
        }
    }

    pub(crate) fn run(&mut self) -> Result<()> {
        let res = self.run_inner();
        if let Some(version) = self.version {
            self.core.orc.done_commit(version);
        }
        if res.is_err() {
            self.cleanup_files();
        }
        res
    }

    fn run_inner(&mut self) -> Result<()> {
        // first check all files are valid
        self.check_input_exist()?;
        // alloc file id for each file
        self.reserve_file_id();
        // move or copy file to db dir
        self.ingest_to_dir()?;
        // verify file
        self.verify()?;
        // start to commit
        self.assign_version();
        // ingest to LSM-tree
        self.ingest_to_lsm()?;
        Ok(())
    }

    fn check_input_exist(&self) -> Result<()> {
        for file in self.files.iter() {
            if !Path::new(&file.input_path).is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("file path {} is invalid", file.input_path),
                )
                .into());
            }
        }
        Ok(())
    }

    fn reserve_file_id(&mut self) {
        for file in self.files.iter_mut() {
            file.id = self.core.lvctl.reserve_file_id();
        }
    }

    fn ingest_to_dir(&mut self) -> Result<()> {
        for file in self.files.iter_mut() {
            let out_path = table::new_filename(file.id, &self.core.opts.dir);
            if self.opts.move_files {
                match fs::hard_link(&file.input_path, &out_path) {
                    Ok(_) => {
                        file.moved_or_copied = true;
                        continue;
                    }
                    Err(err) => {
                        warn!(
                            "[ingest task]: failed to move file {} to db dir. {}",
                            file.input_path, err
                        );
                        if !self.opts.failed_move_fall_back_to_copy {
                            return Err(err.into());
                        }
                    }
                }
            }
            // copy file
            match fs::copy(&file.input_path, &out_path) {
                Ok(_) => {
                    file.moved_or_copied = true;
                }
                Err(err) => {
                    warn!(
                        "[ingest task]: failed to copy file {} to db dir. {}",
                        file.input_path, err
                    );
                    return Err(err.into());
                }
            }
        }
        Ok(())
    }

    fn verify(&mut self) -> Result<()> {
        use ChecksumVerificationMode::*;

        for file in self.files.iter_mut() {
            let table = Table::open(
                &table::new_filename(file.id, &self.core.opts.dir),
                build_table_options(&self.core.opts),
            )?;
            // checksum has been checked when open in [`OnTableRead`] or [`OnTableAndBlockRead`] mode
            // avoid double check
            if self.opts.verify_checksum
                && !matches!(
                    self.core.opts.checksum_mode,
                    OnTableRead | OnTableAndBlockRead
                )
            {
                table.verify_checksum()?;
            }
            file.table = Some(table);
        }
        Ok(())
    }

    fn assign_version(&mut self) {
        // collect user key range for oracle to check conflict
        let ranges = self
            .files
            .iter()
            .map(|file| {
                RangeInclusive::new(
                    Bytes::copy_from_slice(user_key(file.table.as_ref().unwrap().smallest())),
                    Bytes::copy_from_slice(user_key(file.table.as_ref().unwrap().biggest())),
                )
            })
            .collect::<Vec<_>>();
        let version = self.core.orc.new_ingest_commit_ts(ranges, &self.opts);
        self.version = Some(version);
        self.files.iter_mut().for_each(|file| {
            file.table.as_mut().unwrap().set_global_version(version);
        });

        // all tables assigned version and open, sort by range
        self.files.sort_unstable_by(|x, y| {
            COMPARATOR.compare_key(
                x.table.as_ref().unwrap().smallest(),
                y.table.as_ref().unwrap().smallest(),
            )
        });
        // check overlap
        if self.files.len() > 1 {
            for i in 0..(self.files.len() - 1) {
                if matches!(
                    COMPARATOR.compare_key(
                        self.files[i].table.as_ref().unwrap().biggest(),
                        self.files[i + 1].table.as_ref().unwrap().smallest()
                    ),
                    Ordering::Equal | Ordering::Greater
                ) {
                    self.overlap = true;
                    break;
                }
            }
        }
    }

    fn ingest_to_lsm(&mut self) -> Result<()> {
        // TODO: will, it's too ugly here
        let ccore = self.core.clone();
        ccore.lvctl.ingest_tables(self)
    }

    fn cleanup_files(&self) {
        // file will be removed when table drop
        self.files
            .iter()
            .filter(|file| file.moved_or_copied && file.table.is_none())
            .for_each(|file| {
                let out_path = table::new_filename(file.id, &self.core.opts.dir);
                if let Err(err) = fs::remove_file(&out_path) {
                    warn!(
                        "[ingest tark]: failed to clean file {} when ingest task failed. {}",
                        out_path.to_string_lossy(),
                        err
                    )
                }
            })
    }

    pub(crate) fn overlap(&self) -> bool {
        self.overlap
    }

    pub(crate) fn files(&self) -> &[FileContext] {
        &self.files
    }

    pub(crate) fn files_mut(&mut self) -> &mut [FileContext] {
        &mut self.files
    }

    pub(crate) fn opts(&self) -> &IngestExternalFileOptions {
        &self.opts
    }
}
