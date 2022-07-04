#![allow(dead_code)]
use agatedb::{
    opt::build_table_options, AgateOptions, ChecksumVerificationMode::NoVerification, Table,
    TableBuilder, Value,
};
use bytes::Bytes;
use rand::{distributions::Alphanumeric, Rng};
use std::ops::{Deref, DerefMut};
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
