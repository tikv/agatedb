#![cfg(feature = "enable-rocksdb")]
mod common;

use std::{
    ops::Add,
    sync::Arc,
    time::{Duration, Instant},
};

use agatedb::{Agate, AgateOptions, IteratorOptions};
use common::{gen_kv_pair, remove_files, unix_time};
use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;
use rocksdb::DB;
use tempdir::TempDir;

// We will process `CHUNK_SIZE` items in a thread, and in one certain thread,
// we will process `BATCH_SIZE` items in a transaction or write batch.
const KEY_NUMS: u64 = 160_000;
const CHUNK_SIZE: u64 = 10_000;
const BATCH_SIZE: u64 = 100;

const SMALL_VALUE_SIZE: usize = 32;
const LARGE_VALUE_SIZE: usize = 4096;

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

    for _ in (0..key_nums).step_by(chunk_size as usize) {
        let agate = agate.clone();

        handles.push(std::thread::spawn(move || {
            let txn = agate.new_transaction_at(unix_time(), false);
            let opts = IteratorOptions::default();
            let mut iter = txn.new_iterator(&opts);
            iter.rewind();

            while iter.valid() {
                let item = iter.item();
                assert_eq!(item.value().len(), value_size);

                iter.next();
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

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

pub fn rocks_iterate(db: Arc<DB>, key_nums: u64, chunk_size: u64, value_size: usize) {
    let mut handles = vec![];

    for _ in (0..key_nums).step_by(chunk_size as usize) {
        let db = db.clone();

        handles.push(std::thread::spawn(move || {
            let iter = db.iterator(rocksdb::IteratorMode::Start);

            for (_, value) in iter {
                assert_eq!(value.len(), value_size);
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

fn bench_agate(c: &mut Criterion) {
    let dir = TempDir::new("agatedb-bench-small-value").unwrap();
    let dir_path = dir.path();
    let mut opts = AgateOptions {
        dir: dir_path.to_path_buf(),
        value_dir: dir_path.to_path_buf(),
        sync_writes: true,
        managed_txns: true,
        ..Default::default()
    };

    c.bench_function("agate populate small value", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);

            (0..iters).into_iter().for_each(|_| {
                remove_files(dir_path);
                let agate = Arc::new(opts.open().unwrap());

                let now = Instant::now();
                agate_populate(agate, KEY_NUMS, CHUNK_SIZE, BATCH_SIZE, SMALL_VALUE_SIZE);
                total = total.add(now.elapsed());
            });

            total
        });
    });

    let agate = Arc::new(opts.open().unwrap());

    c.bench_function("agate randread small value", |b| {
        b.iter(|| {
            agate_randread(agate.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE);
        });
    });

    c.bench_function("agate iterate small value", |b| {
        b.iter(|| {
            agate_iterate(agate.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE);
        });
    });

    dir.close().unwrap();
    let dir = TempDir::new("agatedb-bench-large-value").unwrap();
    let dir_path = dir.path();
    opts.dir = dir_path.to_path_buf();
    opts.value_dir = dir_path.to_path_buf();

    c.bench_function("agate populate large value", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);

            (0..iters).into_iter().for_each(|_| {
                remove_files(dir_path);
                let agate = Arc::new(opts.open().unwrap());

                let now = Instant::now();
                agate_populate(agate, KEY_NUMS, CHUNK_SIZE, BATCH_SIZE, LARGE_VALUE_SIZE);
                total = total.add(now.elapsed());
            });

            total
        });
    });

    let agate = Arc::new(opts.open().unwrap());

    c.bench_function("agate randread large value", |b| {
        b.iter(|| {
            agate_randread(agate.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE);
        });
    });

    c.bench_function("agate iterate large value", |b| {
        b.iter(|| {
            agate_iterate(agate.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE);
        });
    });

    dir.close().unwrap();
}

fn bench_rocks(c: &mut Criterion) {
    let dir = TempDir::new("rocks-bench-small-value").unwrap();
    let dir_path = dir.path();
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(rocksdb::DBCompressionType::None);

    c.bench_function("rocks populate small value", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);

            (0..iters).into_iter().for_each(|_| {
                remove_files(dir_path);
                let db = Arc::new(rocksdb::DB::open(&opts, &dir).unwrap());

                let now = Instant::now();
                rocks_populate(db, KEY_NUMS, CHUNK_SIZE, BATCH_SIZE, SMALL_VALUE_SIZE);
                total = total.add(now.elapsed());
            });

            total
        });
    });

    let db = Arc::new(rocksdb::DB::open(&opts, &dir).unwrap());

    c.bench_function("rocks randread small value", |b| {
        b.iter(|| {
            rocks_randread(db.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE);
        });
    });

    c.bench_function("rocks iterate small value", |b| {
        b.iter(|| rocks_iterate(db.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE));
    });

    dir.close().unwrap();
    let dir = TempDir::new("rocks-bench-large-value").unwrap();
    let dir_path = dir.path();

    c.bench_function("rocks populate large value", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);

            (0..iters).into_iter().for_each(|_| {
                remove_files(dir_path);
                let db = Arc::new(rocksdb::DB::open(&opts, &dir).unwrap());

                let now = Instant::now();
                rocks_populate(db, KEY_NUMS, CHUNK_SIZE, BATCH_SIZE, LARGE_VALUE_SIZE);
                total = total.add(now.elapsed());
            });

            total
        });
    });

    let db = Arc::new(rocksdb::DB::open(&opts, &dir).unwrap());

    c.bench_function("rocks randread large value", |b| {
        b.iter(|| {
            rocks_randread(db.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE);
        });
    });

    c.bench_function("rocks iterate large value", |b| {
        b.iter(|| rocks_iterate(db.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE));
    });

    dir.close().unwrap();
}

criterion_group! {
  name = benches_agate_rocks;
  config = Criterion::default();
  targets = bench_agate, bench_rocks
}

criterion_main!(benches_agate_rocks);
