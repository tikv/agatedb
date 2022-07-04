mod common;

use agatedb::util::unix_time;
use agatedb::AgateIterator;
use agatedb::AgateOptions;
use agatedb::ConcatIterator;
use agatedb::Iterators;
use agatedb::MergeIterator;

use bytes::Bytes;
use common::get_table_for_benchmark;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{thread_rng, Rng};
use tempdir::TempDir;

fn get_test_options() -> AgateOptions {
    agatedb::AgateOptions {
        mem_table_size: 1 << 15,
        base_table_size: 1 << 15,
        base_level_size: 4 << 15,
        sync_writes: false,
        ..Default::default()
    }
}

fn bench_iterator(c: &mut Criterion) {
    let dir = TempDir::new("agatedb").unwrap();
    let mut opt = get_test_options();
    opt.dir = dir.path().to_path_buf();
    opt.value_dir = dir.path().to_path_buf();
    opt.managed_txns = true;
    let db = opt.open().unwrap();
    const N: usize = 100000; // around 80 SST

    let key = |i| Bytes::from(format!("{:06}", i));
    let val = Bytes::from("ok");

    println!("generating tables...");

    for chunk in (0..N).collect::<Vec<_>>().chunks(10) {
        let mut txn = db.new_transaction_at(unix_time(), true);
        for i in chunk {
            txn.set(key(*i), val.clone()).unwrap();
        }
        txn.commit_at(unix_time()).unwrap();
    }

    std::thread::sleep(std::time::Duration::from_secs(3));

    let lsm_files = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|x| x.ok())
        .filter_map(|x| x.file_name().into_string().ok())
        .filter(|x| x.ends_with(".sst"))
        .count();

    println!("LSM files: {}", lsm_files);

    c.bench_function("iterate noprefix single key", |b| {
        let txn = db.new_transaction_at(unix_time(), false);
        let key_id = thread_rng().gen_range(0, N);
        let seek_key = key(key_id);
        let it_opts = agatedb::IteratorOptions {
            all_versions: true,
            ..Default::default()
        };
        let mut it = txn.new_iterator(&it_opts);

        b.iter(|| {
            it.seek(&seek_key);
            let mut cnt = 0;
            while it.valid_for_prefix(&seek_key) {
                let item = it.item();
                assert_eq!(item.value(), val);
                it.next();
                cnt += 1;
            }
            if cnt != 1 {
                panic!("count must be one key");
            }
        });
    });

    dir.close().unwrap();
}

fn bench_merge_iterator(c: &mut Criterion) {
    let m = 2;
    let n = 5000000 / m;

    let tables = (0..m)
        .map(|_| get_table_for_benchmark(n))
        .collect::<Vec<_>>();
    let its = tables
        .iter()
        .map(|t| Iterators::from(t.new_iterator(0)))
        .collect::<Vec<_>>();
    let mut it = MergeIterator::from_iterators(its, false);

    c.bench_function("merge iterator read", |b| {
        b.iter(|| {
            it.rewind();
            while it.valid() {
                it.next();
            }
        });
    });

    let mut rng = rand::thread_rng();
    c.bench_function("merge iterator random read", |b| {
        b.iter_batched(
            || {
                let i = rng.gen_range(0, n);
                (
                    Bytes::from(format!("{:016x}", i)),
                    Bytes::from(i.to_string()),
                )
            },
            |(k, v)| {
                it.seek(&k);
                assert!(it.valid());
                assert_eq!(it.value().value, v)
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_concat_iterator(c: &mut Criterion) {
    let m = 2;
    let n = 5000000 / m;

    let tables = (0..m)
        .map(|_| get_table_for_benchmark(n))
        .collect::<Vec<_>>();
    let tables = tables.iter().map(|t| t.table.clone()).collect::<Vec<_>>();
    let mut it = ConcatIterator::from_tables(tables, 0);

    c.bench_function("concat iterator read", |b| {
        b.iter(|| {
            it.rewind();
            while it.valid() {
                it.next();
            }
        });
    });

    let mut rng = rand::thread_rng();
    c.bench_function("concat iterator random read", |b| {
        b.iter_batched(
            || {
                let i = rng.gen_range(0, n);
                (
                    Bytes::from(format!("{:016x}", i)),
                    Bytes::from(i.to_string()),
                )
            },
            |(k, v)| {
                it.seek(&k);
                assert!(it.valid());
                assert_eq!(it.value().value, v)
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group! {
    name = benches_iterator;
    config = Criterion::default();
    targets = bench_iterator, bench_merge_iterator, bench_concat_iterator
}

criterion_main!(benches_iterator);
