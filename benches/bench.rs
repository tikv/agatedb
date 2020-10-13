use criterion::{criterion_group, criterion_main, Criterion};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("format make key with ts", |b| {
        b.iter(|| key_with_ts("aaabbbcccddd", 233))
    });
    let key = key_with_ts("aaabbbcccddd", 233);
    c.bench_function("format get ts", |b| {
        b.iter(|| get_ts(&key))
    });
    c.bench_function("format make key with ts (legacy)", |b| {
        b.iter(|| agatedb::format::legacy::key_with_ts("aaabbbcccddd", 233))
    });
    let key = agatedb::format::legacy::key_with_ts("aaabbbcccddd", 233);
    c.bench_function("format get ts (legacy)", |b| {
        b.iter(|| agatedb::format::legacy::get_ts(&key))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
