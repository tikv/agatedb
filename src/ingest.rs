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

#[derive(Debug, Clone, Copy)]
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
    success: bool,
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
            success: false,
        }
    }

    pub(crate) fn run(&mut self) -> Result<()> {
        let res = self.run_inner();
        if let Some(version) = self.version {
            self.core.orc.done_commit(version);
        }
        self.success = res.is_ok();
        self.cleanup_files();
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
        if !self.success {
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
        } else if self.opts.move_files {
            // success move and ingest, remove old files
            self.files.iter().for_each(|file| {
                if let Err(err) = fs::remove_file(&file.input_path) {
                    warn!(
                        "failed to remove input file {} after ingest. {}",
                        file.input_path, err
                    );
                }
            })
        }
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, path::Path};

    use bytes::BytesMut;

    use super::IngestExternalFileOptions;
    use crate::{
        db::tests::run_agate_test, error::Result, key_with_ts, opt::build_table_options, Agate,
        AgateOptions, Table, TableBuilder, Value, IteratorOptions,
    };

    const BUILD_TABLE_VERSION: u64 = 10;

    fn build_key(i: usize) -> BytesMut {
        BytesMut::from(format!("key_{:012x}", i).as_bytes())
    }

    fn build_value(i: usize) -> BytesMut {
        BytesMut::from(build_key(i).repeat(4).as_slice())
    }

    fn build_table<P: AsRef<Path>>(
        path: P,
        opts: &AgateOptions,
        f: impl FnOnce(&mut TableBuilder),
    ) -> Result<()> {
        let mut builder = TableBuilder::new(build_table_options(opts));
        f(&mut builder);
        let table = Table::create(path.as_ref(), builder.finish(), build_table_options(opts))?;
        table.mark_save();
        Ok(())
    }

    fn create_external_files_dir<P: AsRef<Path>>(path: P) {
        let _ = fs::remove_dir_all(&path);
        assert!(fs::create_dir_all(&path).is_ok());
    }

    fn ingest(db: &Agate, files: &[&str], move_files: bool, commit_ts: u64) -> Result<()> {
        let mut opts = IngestExternalFileOptions::default();
        opts.move_files = move_files;
        opts.commit_ts = commit_ts;
        db.ingest_external_files(files, &opts)
    }

    #[test]
    fn basic() {
        run_agate_test(None, |db| {
            let external_dir = db.core.opts.dir.join("external_files");
            create_external_files_dir(&external_dir);

            let file1 = external_dir.join("1.sst");

            let res = build_table(&file1, &db.core.opts, |builder| {
                for i in 0..100 {
                    builder.add(
                        &key_with_ts(build_key(i), BUILD_TABLE_VERSION),
                        &Value::new(build_value(i).freeze()),
                        0,
                    );
                }
            });
            assert!(res.is_ok());

            let file2 = external_dir.join("2.sst");

            let res = build_table(&file2, &db.core.opts, |builder| {
                for i in 100..200 {
                    builder.add(
                        &key_with_ts(build_key(i), BUILD_TABLE_VERSION),
                        &Value::new(build_value(i).freeze()),
                        0,
                    );
                }
            });
            assert!(res.is_ok());

            let res = ingest(&db, &[&file1.to_string_lossy()], false, 0);
            assert!(res.is_ok());
            let res = ingest(&db, &[&file2.to_string_lossy()], true, 0);
            assert!(res.is_ok());

            assert!(Path::exists(&file1));
            assert!(!Path::exists(&file2));

            db.view(|txn| {
                for i in 0..200 {
                    let item = txn.get(&build_key(i).freeze()).unwrap();
                    assert_eq!(item.value(), build_value(i));
                }
                Ok(())
            })
            .unwrap();
        })
    }

    #[test]
    fn overlap() {
        let mut db_opts = AgateOptions::default();
        db_opts.managed_txns = true;

        run_agate_test(Some(db_opts), |db| {
            let external_dir = db.core.opts.dir.join("external_files");
            create_external_files_dir(&external_dir);

            let mut data = HashMap::new();

            let file1 = external_dir.join("1.sst");

            let res = build_table(&file1, &db.core.opts, |builder| {
                for i in 0..1000 {
                    let k = build_key(i);
                    let v = build_value(i);
                    builder.add(
                        &key_with_ts(k.clone(), BUILD_TABLE_VERSION),
                        &Value::new(v.clone().freeze()),
                        0,
                    );
                    data.insert(k, v);
                }
            });
            assert!(res.is_ok());
            let res = ingest(&db, &[&file1.to_string_lossy()], true, 1);
            assert!(res.is_ok());

            {
                let mut txn = db.new_transaction_at(1, true);
                for i in 500..1500 {
                    let k = build_key(i);
                    let v = BytesMut::from(&b"in memtable"[..]);
                    assert!(txn.set(k.clone().freeze(), v.clone().freeze()).is_ok());
                    data.insert(k, v);
                }
                assert!(txn.commit_at(2).is_ok());
            }

            let file2 = external_dir.join("2.sst");

            let res = build_table(&file2, &db.core.opts, |builder| {
                for i in 1000..2000 {
                    let k = build_key(i);
                    let v = build_value(i);
                    builder.add(
                        &key_with_ts(k.clone(), BUILD_TABLE_VERSION),
                        &Value::new(v.clone().freeze()),
                        0,
                    );
                    data.insert(k, v);
                }
            });
            assert!(res.is_ok());

            let res = ingest(&db, &[&file2.to_string_lossy()], true, 3);
            assert!(res.is_ok());

            assert!(!Path::exists(&file1));
            assert!(!Path::exists(&file2));

            db.view(|txn| {
                for (k, v) in data.iter() {
                    let item = txn.get(&k.clone().freeze()).unwrap();
                    assert_eq!(item.value(), v)
                }
                Ok(())
            })
            .unwrap();

            {
                let txn = db.new_transaction_at(2, false);
                for i in 500..1500 {
                    let k = build_key(i);
                    let v = BytesMut::from(&b"in memtable"[..]);
                    let item = txn.get(&k.freeze()).unwrap();
                    assert_eq!(item.value(), &v);
                }
            }
        })
    }

    #[test]
    fn conflict_check() {
        run_agate_test(None, |db| {
            assert!(db.update(|txn| {
                for i in 0..500 {
                    txn.set(build_key(i).freeze(), build_value(i).freeze())?;
                }
                Ok(())
            }).is_ok());

            // use update mode to trigger conflict check
            let mut txn1 = db.new_transaction(true);
            let mut txn2 = db.new_transaction(true);
            let mut txn3 = db.new_transaction(true);
            let mut txn4 = db.new_transaction(true);
            assert!(txn1.set(build_key(501).freeze(), build_value(501).freeze()).is_ok());
            assert!(txn2.set(build_key(502).freeze(), build_value(502).freeze()).is_ok());
            assert!(txn3.set(build_key(503).freeze(), build_value(503).freeze()).is_ok());
            assert!(txn4.set(build_key(504).freeze(), build_value(504).freeze()).is_ok());



            let external_dir = db.core.opts.dir.join("external_files");
            create_external_files_dir(&external_dir);
            let file1 = external_dir.join("1.sst");
            let res = build_table(&file1, &db.core.opts, |builder| {
                for i in 200..300 {
                    builder.add(
                        &key_with_ts(build_key(i), BUILD_TABLE_VERSION),
                        &Value::new(build_value(i).freeze()),
                        0,
                    );
                }
            });
            assert!(res.is_ok());
            let res = ingest(&db, &[&file1.to_string_lossy()], true, 0);
            assert!(res.is_ok());

            {
                // [0, 200], should conflict
                let mut iter = txn1.new_iterator(&IteratorOptions::default());
                iter.rewind();
                loop {
                    assert!(iter.valid());
                    let item = iter.item();
                    if &item.key == &build_key(200) {
                        break;
                    }
                    iter.next();
                }
            }

            {
                // [300, 400], not conflict
                let mut iter = txn2.new_iterator(&IteratorOptions::default());
                iter.seek(&build_key(300).freeze());
                loop {
                    assert!(iter.valid());
                    let item = iter.item();
                    if &item.key == &build_key(400) {
                        break;
                    }
                    iter.next();
                }
            }

            {
                // [250, 350], should conflict
                let mut iter = txn3.new_iterator(&IteratorOptions::default());
                iter.seek(&build_key(250).freeze());
                loop {
                    assert!(iter.valid());
                    let item = iter.item();
                    if &item.key == &build_key(350) {
                        break;
                    }
                    iter.next();
                }
            }

            {
                // [50, 60], not conflict
                let mut iter = txn4.new_iterator(&IteratorOptions::default());
                iter.seek(&build_key(50).freeze());
                loop {
                    assert!(iter.valid());
                    let item = iter.item();
                    if &item.key == &build_key(60) {
                        break;
                    }
                    iter.next();
                }
            }

            assert!(txn1.commit().is_err());
            assert!(txn2.commit().is_ok());
            assert!(txn3.commit().is_err());
            assert!(txn4.commit().is_ok());
        })
    }
}
