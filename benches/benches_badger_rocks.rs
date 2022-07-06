#![cfg(feature = "enable-rocksdb")]
mod common;

use agatedb::Agate;
use agatedb::AgateOptions;
use agatedb::IteratorOptions;
use common::{gen_kv_pair, unix_time};
use criterion::{criterion_group, criterion_main, Criterion};
use rand::prelude::ThreadRng;
use rand::Rng;
use rocksdb::DB;
use std::sync::Arc;
use tempdir::TempDir;

fn badger_populate(agate: Arc<Agate>, batch_size: u64, value_size: usize) {
    let mut txn = agate.new_transaction_at(unix_time(), true);

    for i in 0..batch_size {
        let (key, value) = gen_kv_pair(i, value_size);
        txn.set(key, value).unwrap();
    }

    txn.commit_at(unix_time()).unwrap();
}

fn badger_randread(agate: Arc<Agate>, batch_size: u64, value_size: usize, rng: &mut ThreadRng) {
    let txn = agate.new_transaction_at(unix_time(), false);

    for _ in 0..batch_size {
        let (key, value) = gen_kv_pair(rng.gen_range(0, batch_size), value_size);

        let item = txn.get(&key).unwrap();
        assert_eq!(item.value(), value);
    }
}

fn badger_iterate(agate: Arc<Agate>, value_size: usize) {
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

fn rocks_populate(db: Arc<DB>, batch_size: u64, value_size: usize) {
    let mut write_options = rocksdb::WriteOptions::default();
    write_options.set_sync(true);
    write_options.disable_wal(false);

    let mut batch = rocksdb::WriteBatch::default();

    for i in 0..batch_size {
        let (key, value) = gen_kv_pair(i, value_size);
        batch.put(key, value);
    }

    db.write_opt(batch, &write_options).unwrap();
}

fn rocks_randread(db: Arc<DB>, batch_size: u64, value_size: usize, rng: &mut ThreadRng) {
    for _ in 0..batch_size {
        let (key, value) = gen_kv_pair(rng.gen_range(0, batch_size), value_size);

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

fn bench_badger(c: &mut Criterion) {
    let batch_size = 1000;
    let mut rng = rand::thread_rng();

    let dir = TempDir::new("agatedb-bench-small-value").unwrap();
    let mut opts = AgateOptions {
        create_if_not_exists: true,
        sync_writes: true,
        dir: dir.as_ref().to_path_buf(),
        value_dir: dir.as_ref().to_path_buf(),
        managed_txns: true,
        ..Default::default()
    };
    let agate = Arc::new(opts.open().unwrap());
    let value_size = 32;

    c.bench_function("badger populate small value", |b| {
        b.iter(|| {
            badger_populate(agate.clone(), batch_size, value_size);
        });
    });

    c.bench_function("badger randread small value", |b| {
        b.iter(|| {
            badger_randread(agate.clone(), batch_size, value_size, &mut rng);
        });
    });

    c.bench_function("badger iterate small value", |b| {
        b.iter(|| {
            badger_iterate(agate.clone(), value_size);
        });
    });

    dir.close().unwrap();
    let dir = TempDir::new("agatedb-bench-large-value").unwrap();
    opts.dir = dir.as_ref().to_path_buf();
    opts.value_dir = dir.as_ref().to_path_buf();
    let agate = Arc::new(opts.open().unwrap());
    let value_size = 102400;

    c.bench_function("badger populate large value", |b| {
        b.iter(|| {
            badger_populate(agate.clone(), batch_size, value_size);
        });
    });

    c.bench_function("badger randread large value", |b| {
        b.iter(|| {
            badger_randread(agate.clone(), batch_size, value_size, &mut rng);
        });
    });

    c.bench_function("badger iterate large value", |b| {
        b.iter(|| {
            badger_iterate(agate.clone(), value_size);
        });
    });

    dir.close().unwrap();
}

fn bench_rocks(c: &mut Criterion) {
    let batch_size = 1000;
    let mut rng = rand::thread_rng();

    let dir = TempDir::new("rocks-bench-small-value").unwrap();
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(rocksdb::DBCompressionType::None);
    let db = Arc::new(rocksdb::DB::open(&opts, &dir).unwrap());
    let value_size = 32;

    c.bench_function("rocks populate small value", |b| {
        b.iter(|| rocks_populate(db.clone(), batch_size, value_size));
    });

    c.bench_function("rocks randread small value", |b| {
        b.iter(|| {
            rocks_randread(db.clone(), batch_size, value_size, &mut rng);
        });
    });

    c.bench_function("rocks iterate small value", |b| {
        b.iter(|| rocks_iterate(db.clone(), value_size));
    });

    dir.close().unwrap();
    let dir = TempDir::new("rocks-bench-large-value").unwrap();
    let db = Arc::new(rocksdb::DB::open(&opts, &dir).unwrap());
    let value_size = 102400;

    c.bench_function("rocks populate large value", |b| {
        b.iter(|| rocks_populate(db.clone(), batch_size, value_size));
    });

    c.bench_function("rocks randread large value", |b| {
        b.iter(|| {
            rocks_randread(db.clone(), batch_size, value_size, &mut rng);
        });
    });

    c.bench_function("rocks iterate large value", |b| {
        b.iter(|| rocks_iterate(db.clone(), value_size));
    });

    dir.close().unwrap();
}

criterion_group! {
  name = benches_badger_rocks;
  config = Criterion::default();
  targets = bench_badger, bench_rocks
}

criterion_main!(benches_badger_rocks);
