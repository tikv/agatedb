mod common;

use std::{
    ops::Add,
    sync::Arc,
    time::{Duration, Instant},
};

use agatedb::AgateOptions;
use common::{
    agate_iterate, agate_populate, agate_randread, remove_files, rocks_iterate, rocks_populate,
    rocks_randread,
};
use criterion::{criterion_group, criterion_main, Criterion};
use tempdir::TempDir;

// We will process `CHUNK_SIZE` items in a thread, and in one certain thread,
// we will process `BATCH_SIZE` items in a transaction or write batch.
const KEY_NUMS: u64 = 160_000;
const CHUNK_SIZE: u64 = 10_000;
const BATCH_SIZE: u64 = 100;

const SMALL_VALUE_SIZE: usize = 32;
const LARGE_VALUE_SIZE: usize = 4096;

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
