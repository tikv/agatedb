#![allow(dead_code)]
use agatedb::{
    opt::build_table_options, util::sync_dir, AgateOptions,
    ChecksumVerificationMode::NoVerification, Table, TableBuilder, Value,
};
use bytes::{Bytes, BytesMut};
use rand::{distributions::Alphanumeric, Rng};
use std::{
    fs::{read_dir, remove_file},
    ops::{Deref, DerefMut},
    path::Path,
    time::UNIX_EPOCH,
};
use tempdir::TempDir;

pub fn rand_value() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .collect::<String>()
}

/// TableGuard saves Table and TempDir, so as to ensure
/// temporary directory is removed after table is closed.
/// According to Rust RFC, the drop order is first `table` then
/// `tmp_dir`.
pub struct TableGuard {
    pub table: Table,
    _tmp_dir: TempDir,
}

impl Deref for TableGuard {
    type Target = Table;

    fn deref(&self) -> &Table {
        &self.table
    }
}

impl DerefMut for TableGuard {
    fn deref_mut(&mut self) -> &mut Table {
        &mut self.table
    }
}

pub fn get_table_for_benchmark(count: usize) -> TableGuard {
    let tmp_dir = TempDir::new("agatedb").unwrap();

    let agate_opts = AgateOptions {
        block_size: 4 * 1024,
        bloom_false_positive: 0.01,
        checksum_mode: NoVerification,
        ..Default::default()
    };

    let opts = build_table_options(&agate_opts);

    let mut builder = TableBuilder::new(opts.clone());
    let filename = tmp_dir.path().join("1.sst");

    for i in 0..count {
        let k = Bytes::from(format!("{:016x}", i));
        let v = Bytes::from(i.to_string());
        builder.add(&k, &Value::new(v), 0);
    }

    TableGuard {
        table: Table::create(&filename, builder.finish(), opts).unwrap(),
        _tmp_dir: tmp_dir,
    }
}

pub fn gen_kv_pair(key: u64, value_size: usize) -> (Bytes, Bytes) {
    let key = Bytes::from(format!("vsz={:05}-k={:010}", value_size, key));

    let mut value = BytesMut::with_capacity(value_size);
    value.resize(value_size, 0);

    (key, value.freeze())
}

pub fn unix_time() -> u64 {
    UNIX_EPOCH
        .elapsed()
        .expect("Time went backwards")
        .as_millis() as u64
}

pub fn remove_files(path: &Path) {
    read_dir(path).unwrap().into_iter().for_each(|entry| {
        let entry = entry.unwrap();
        remove_file(entry.path()).unwrap();
    });
    sync_dir(&path).unwrap();
}
