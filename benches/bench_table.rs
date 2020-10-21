mod common;

use agatedb::{Table, TableBuilder, TableOptions, Value};
use bytes::Bytes;
use common::rand_value;
use criterion::{criterion_group, criterion_main, Criterion};
use tempdir::TempDir;

fn bench_table_builder(c: &mut Criterion) {
    c.bench_function("table builder", |b| {
        let mut key_list = vec![];

        const KEY_COUNT: usize = 1300000; // about 64MB

        for i in 0..KEY_COUNT {
            let k = Bytes::from(format!("{:032}", i));
            key_list.push(k);
        }

        let vs = Value::new(Bytes::from(rand_value()));

        let opt = TableOptions {
            block_size: 4 * 1024,
            bloom_false_positive: 0.01,
            table_size: 5 << 20,
        };

        b.iter(|| {
            let mut builder = TableBuilder::new(opt.clone());
            for j in 0..KEY_COUNT {
                builder.add(&key_list[j], vs.clone(), 0);
            }
            builder.finish()
        });
    });
}

fn get_table_for_benchmark(count: usize) -> Table {
    let tmp_dir = TempDir::new("agatedb").unwrap();

    let opts = TableOptions {
        // TODO: add compression parameter
        block_size: 4 * 1024,
        bloom_false_positive: 0.01,
        table_size: 0,
    };

    let mut builder = TableBuilder::new(opts.clone());
    let filename = tmp_dir.path().join("1.sst".to_string());

    for i in 0..count {
        let k = Bytes::from(format!("{:016x}", i));
        let v = Bytes::from(i.to_string());
        builder.add(&k, Value::new(v), 0);
    }

    Table::create(&filename, builder.finish(), opts).unwrap()
}

fn bench_table(c: &mut Criterion) {
    let n = 5000000;
    let table = get_table_for_benchmark(n);
    c.bench_function("table read", |b| {
        b.iter(|| {
            let mut it = table.new_iterator(0);
            it.seek_to_first();
            while it.valid() {
                it.next();
            }
        });
    });
}

criterion_group! {
    name = benches_table;
    config = Criterion::default().sample_size(20);
    targets = bench_table_builder, bench_table
}

criterion_main!(benches_table);
