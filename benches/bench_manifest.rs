mod common;

use agatedb::util::{BTree, ComparableRecord};
use rand::Rng;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Instant;

struct FakeTableInner {
    id: u64,
    smallest: Bytes,
    largest: Bytes,
}

#[derive(Clone)]
struct FakeTable {
    inner: Arc<FakeTableInner>,
}

impl ComparableRecord for FakeTable {
    fn smallest(&self) -> &Bytes {
        &self.inner.smallest
    }

    fn largest(&self) -> &Bytes {
        &self.inner.largest
    }

    fn id(&self) -> u64 {
        self.inner.id
    }
}

fn benche_manifest(c: &mut Criterion) {
    const KEY_RANGE_COUNT: u64 = 10000; // about 64MB
    const KEY_BASE: u64 = 1000_1000; // about 64MB

    // let mut rng = rand::thread_rng();
    // let mut tree = LevelTree::<FakeTable>::new(64, 128);

    let mut test_count = 0;
    println!("start bench");
    c.bench_function("table builder", |b| {
        b.iter(|| {
            // let j = rng.gen_range(0, KEY_RANGE_COUNT - 1);
            // let left = KEY_BASE + j as usize * 100;
            // let left = left + 99;
            // let smallest = Bytes::from(left.to_string());
            // let largest = Bytes::from(right.to_string());
            // TODO: check whether key existed and decide delete or insert.
            test_count += 1;
        });
    });
    println!("end bench, {}", test_count);
}

criterion_group! {
    name = benches_manifest;
    config = Criterion::default();
    targets = benche_manifest
}

criterion_main!(benches_manifest);
