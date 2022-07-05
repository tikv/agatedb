mod common;

use agatedb::{
    opt::build_table_options, AgateIterator, AgateOptions,
    ChecksumVerificationMode::NoVerification, TableBuilder, Value,
};
use bytes::Bytes;
use common::{get_table_for_benchmark, rand_value};
use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;

fn bench_table_builder(c: &mut Criterion) {
    c.bench_function("table builder", |b| {
        let mut key_list = vec![];

        const KEY_COUNT: usize = 1300000; // about 64MB

        for i in 0..KEY_COUNT {
            let k = Bytes::from(format!("{:032}", i));
            key_list.push(k);
        }

        let vs = Value::new(Bytes::from(rand_value()));

        let agate_opts = AgateOptions {
            block_size: 4 * 1024,
            base_table_size: 5 << 20,
            bloom_false_positive: 0.01,
            checksum_mode: NoVerification,
            ..Default::default()
        };

        let opts = build_table_options(&agate_opts);

        b.iter(|| {
            let mut builder = TableBuilder::new(opts.clone());
            for key in key_list.iter().take(KEY_COUNT) {
                builder.add(key, &vs, 0);
            }
            builder.finish()
        });
    });
}

fn bench_table(c: &mut Criterion) {
    let n = 5000000;
    c.bench_function("table read", |b| {
        let table = get_table_for_benchmark(n);
        let mut it = table.new_iterator(0);

        b.iter(|| {
            it.seek_to_first();
            while it.valid() {
                it.next();
            }
        });
    });

    let agate_opts = AgateOptions {
        block_size: 4 * 1024,
        bloom_false_positive: 0.01,
        checksum_mode: NoVerification,
        ..Default::default()
    };

    let builder_opts = build_table_options(&agate_opts);

    c.bench_function("table read and build", |b| {
        let table = get_table_for_benchmark(n);
        let mut it = table.new_iterator(0);

        b.iter(|| {
            let mut builder = TableBuilder::new(builder_opts.clone());
            it.seek_to_first();
            while it.valid() {
                builder.add(&Bytes::copy_from_slice(it.key()), &it.value(), 0);
                it.next();
            }
            builder.finish()
        });
    });

    // TODO: table merge read

    let mut rng = rand::thread_rng();
    c.bench_function("table random read", |b| {
        let table = get_table_for_benchmark(n);
        let mut it = table.new_iterator(0);

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
    name = benches_table;
    config = Criterion::default();
    targets = bench_table_builder, bench_table
}

criterion_main!(benches_table);
