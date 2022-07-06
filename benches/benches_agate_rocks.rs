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
use rand::{prelude::ThreadRng, Rng};
use rocksdb::DB;
use tempdir::TempDir;

const BATCH_SIZE: u64 = 1000;
const SMALL_VALUE_SIZE: usize = 32;
const LARGE_VALUE_SIZE: usize = 102400;

fn agate_populate(agate: Arc<Agate>, value_size: usize) {
    let mut txn = agate.new_transaction_at(unix_time(), true);

    for i in 0..BATCH_SIZE {
        let (key, value) = gen_kv_pair(i, value_size);
        txn.set(key, value).unwrap();
    }

    txn.commit_at(unix_time()).unwrap();
}

fn agate_randread(agate: Arc<Agate>, value_size: usize, rng: &mut ThreadRng) {
    let txn = agate.new_transaction_at(unix_time(), false);

    for _ in 0..BATCH_SIZE {
        let (key, value) = gen_kv_pair(rng.gen_range(0, BATCH_SIZE), value_size);

        let item = txn.get(&key).unwrap();
        assert_eq!(item.value(), value);
    }
}

fn agate_iterate(agate: Arc<Agate>, value_size: usize) {
    let txn = agate.new_transaction_at(unix_time(), false);
    let opts = IteratorOptions::default();
    let mut iter = txn.new_iterator(&opts);
    iter.rewind();

    while iter.valid() {
        let item = iter.item();
        assert_eq!(item.value().len(), value_size);

        iter.next();
    }
}

fn rocks_populate(db: Arc<DB>, value_size: usize) {
    let mut write_options = rocksdb::WriteOptions::default();
    write_options.set_sync(true);
    write_options.disable_wal(false);

    let mut batch = rocksdb::WriteBatch::default();

    for i in 0..BATCH_SIZE {
        let (key, value) = gen_kv_pair(i, value_size);
        batch.put(key, value);
    }

    db.write_opt(batch, &write_options).unwrap();
}

fn rocks_randread(db: Arc<DB>, value_size: usize, rng: &mut ThreadRng) {
    for _ in 0..BATCH_SIZE {
        let (key, value) = gen_kv_pair(rng.gen_range(0, BATCH_SIZE), value_size);

        let find = db.get(key).unwrap();
        assert_eq!(find.unwrap(), value)
    }
}

fn rocks_iterate(db: Arc<DB>, value_size: usize) {
    let iter = db.iterator(rocksdb::IteratorMode::Start);

    for (_, value) in iter {
        assert_eq!(value.len(), value_size);
    }
}

fn bench_agate(c: &mut Criterion) {
    let mut rng = rand::thread_rng();

    let dir = TempDir::new("agatedb-bench-small-value").unwrap();
    let dir_path = dir.path();
    let mut opts = AgateOptions {
        create_if_not_exists: true,
        sync_writes: true,
        dir: dir_path.to_path_buf(),
        value_dir: dir_path.to_path_buf(),
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
                agate_populate(agate, SMALL_VALUE_SIZE);
                total = total.add(now.elapsed());
            });

            total
        });
    });

    let agate = Arc::new(opts.open().unwrap());

    c.bench_function("agate randread small value", |b| {
        b.iter(|| {
            agate_randread(agate.clone(), SMALL_VALUE_SIZE, &mut rng);
        });
    });

    c.bench_function("agate iterate small value", |b| {
        b.iter(|| {
            agate_iterate(agate.clone(), SMALL_VALUE_SIZE);
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
                agate_populate(agate, LARGE_VALUE_SIZE);
                total = total.add(now.elapsed());
            });

            total
        });
    });

    let agate = Arc::new(opts.open().unwrap());

    c.bench_function("agate randread large value", |b| {
        b.iter(|| {
            agate_randread(agate.clone(), LARGE_VALUE_SIZE, &mut rng);
        });
    });

    c.bench_function("agate iterate large value", |b| {
        b.iter(|| {
            agate_iterate(agate.clone(), LARGE_VALUE_SIZE);
        });
    });

    dir.close().unwrap();
}

fn bench_rocks(c: &mut Criterion) {
    let mut rng = rand::thread_rng();

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
                rocks_populate(db, SMALL_VALUE_SIZE);
                total = total.add(now.elapsed());
            });

            total
        });
    });

    let db = Arc::new(rocksdb::DB::open(&opts, &dir).unwrap());

    c.bench_function("rocks randread small value", |b| {
        b.iter(|| {
            rocks_randread(db.clone(), SMALL_VALUE_SIZE, &mut rng);
        });
    });

    c.bench_function("rocks iterate small value", |b| {
        b.iter(|| rocks_iterate(db.clone(), SMALL_VALUE_SIZE));
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
                rocks_populate(db, LARGE_VALUE_SIZE);
                total = total.add(now.elapsed());
            });

            total
        });
    });

    let db = Arc::new(rocksdb::DB::open(&opts, &dir).unwrap());

    c.bench_function("rocks randread large value", |b| {
        b.iter(|| {
            rocks_randread(db.clone(), LARGE_VALUE_SIZE, &mut rng);
        });
    });

    c.bench_function("rocks iterate large value", |b| {
        b.iter(|| rocks_iterate(db.clone(), LARGE_VALUE_SIZE));
    });

    dir.close().unwrap();
}

criterion_group! {
  name = benches_agate_rocks;
  config = Criterion::default();
  targets = bench_agate, bench_rocks
}

criterion_main!(benches_agate_rocks);
