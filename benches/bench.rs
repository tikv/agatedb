use agatedb::{get_ts, key_with_ts, TableBuilder, TableOptions, Value};
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{distributions::Alphanumeric, Rng};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn rand_value() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .collect::<String>()
}

fn bench_format(c: &mut Criterion) {
    c.bench_function("format make key with ts", |b| {
        b.iter(|| key_with_ts("aaabbbcccddd", 233))
    });
    let key = key_with_ts("aaabbbcccddd", 233);
    c.bench_function("format get ts", |b| b.iter(|| get_ts(&key)));
}

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
            for key in key_list.iter().take(KEY_COUNT) {
                builder.add(&key, vs.clone(), 0);
            }
            builder.finish()
        });
    });
}

criterion_group!(benches, bench_format, bench_table_builder);
criterion_main!(benches);
