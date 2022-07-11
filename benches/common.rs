#![allow(dead_code)]
use std::{
    fs::{read_dir, remove_file},
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
    time::UNIX_EPOCH,
};

use agatedb::{
    opt::build_table_options, util::sync_dir, Agate, AgateOptions,
    ChecksumVerificationMode::NoVerification, IteratorOptions, Table, TableBuilder, Value,
};
use bytes::{Bytes, BytesMut};
use rand::{distributions::Alphanumeric, Rng};
#[cfg(feature = "enable-rocksdb")]
use rocksdb::DB;
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

pub fn agate_populate(
    agate: Arc<Agate>,
    key_nums: u64,
    chunk_size: u64,
    batch_size: u64,
    value_size: usize,
) {
    let mut handles = vec![];

    for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
        let agate = agate.clone();

        handles.push(std::thread::spawn(move || {
            let range = chunk_start..chunk_start + chunk_size;

            for batch_start in range.step_by(batch_size as usize) {
                let mut txn = agate.new_transaction_at(unix_time(), true);

                (batch_start..batch_start + batch_size).for_each(|key| {
                    let (key, value) = gen_kv_pair(key, value_size);
                    txn.set(key, value).unwrap();
                });

                txn.commit_at(unix_time()).unwrap();
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

pub fn agate_randread(agate: Arc<Agate>, key_nums: u64, chunk_size: u64, value_size: usize) {
    let mut handles = vec![];

    for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
        let agate = agate.clone();

        handles.push(std::thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let range = chunk_start..chunk_start + chunk_size;
            let txn = agate.new_transaction_at(unix_time(), false);

            for _ in range {
                let (key, _) = gen_kv_pair(rng.gen_range(0, key_nums), value_size);
                match txn.get(&key) {
                    Ok(item) => {
                        assert_eq!(item.value().len(), value_size);
                    }
                    Err(err) => {
                        if matches!(err, agatedb::Error::KeyNotFound(_)) {
                            continue;
                        } else {
                            panic!("{:?}", err);
                        }
                    }
                }
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

pub fn agate_iterate(agate: Arc<Agate>, key_nums: u64, chunk_size: u64, value_size: usize) {
    let mut handles = vec![];

    for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
        let agate = agate.clone();
        let (key, _) = gen_kv_pair(chunk_start, value_size);

        handles.push(std::thread::spawn(move || {
            let txn = agate.new_transaction_at(unix_time(), false);
            let opts = IteratorOptions::default();
            let mut iter = txn.new_iterator(&opts);
            iter.seek(&key);
            let mut count = 0;

            while iter.valid() {
                let item = iter.item();
                assert_eq!(item.value().len(), value_size);

                iter.next();

                count += 1;
                if count > chunk_size {
                    break;
                }
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

#[cfg(feature = "enable-rocksdb")]
pub fn rocks_populate(
    db: Arc<DB>,
    key_nums: u64,
    chunk_size: u64,
    batch_size: u64,
    value_size: usize,
) {
    let mut write_options = rocksdb::WriteOptions::default();
    write_options.set_sync(true);
    write_options.disable_wal(false);
    let write_options = Arc::new(write_options);

    let mut handles = vec![];

    for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
        let db = db.clone();
        let write_options = write_options.clone();

        handles.push(std::thread::spawn(move || {
            let range = chunk_start..chunk_start + chunk_size;

            for batch_start in range.step_by(batch_size as usize) {
                let mut batch = rocksdb::WriteBatch::default();

                (batch_start..batch_start + batch_size).for_each(|key| {
                    let (key, value) = gen_kv_pair(key, value_size);
                    batch.put(key, value);
                });

                db.write_opt(batch, &write_options).unwrap();
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

#[cfg(feature = "enable-rocksdb")]
pub fn rocks_randread(db: Arc<DB>, key_nums: u64, chunk_size: u64, value_size: usize) {
    let mut handles = vec![];

    for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
        let db = db.clone();

        handles.push(std::thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let range = chunk_start..chunk_start + chunk_size;

            for _ in range {
                let (key, _) = gen_kv_pair(rng.gen_range(0, key_nums), value_size);
                match db.get(key) {
                    Ok(item) => {
                        if item.is_some() {
                            assert_eq!(item.unwrap().len(), value_size);
                        }
                    }
                    Err(err) => {
                        panic!("{:?}", err);
                    }
                }
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

#[cfg(feature = "enable-rocksdb")]
pub fn rocks_iterate(db: Arc<DB>, key_nums: u64, chunk_size: u64, value_size: usize) {
    let mut handles = vec![];

    for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
        let db = db.clone();
        let (key, _) = gen_kv_pair(chunk_start, value_size);

        handles.push(std::thread::spawn(move || {
            let mut iter = db.raw_iterator();
            iter.seek(&key);
            let mut count = 0;

            while iter.valid() {
                assert_eq!(iter.value().unwrap().len(), value_size);

                iter.next();

                count += 1;
                if count > chunk_size {
                    break;
                }
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}
