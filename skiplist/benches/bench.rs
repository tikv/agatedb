use bytes::*;
use criterion::*;
use rand::prelude::*;
use skiplist::*;
use std::collections::*;
use std::sync::atomic::*;
use std::sync::*;
use std::thread;

fn skiplist_round(l: &Skiplist, case: &(Bytes, bool), exp: &Bytes) {
    if case.1 {
        if let Some(v) = l.get(&case.0) {
            assert_eq!(v, exp);
        }
    } else {
        l.put(case.0.clone(), exp.clone());
    }
}

fn bench_read_write_skiplist_frac(b: &mut Bencher<'_>, frac: &usize) {
    let frac = *frac;
    let value = Bytes::from_static(b"00123");
    let list = Skiplist::with_capacity(512 << 20);
    let l = list.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();
    let v = value.clone();
    let j = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        while !s.load(Ordering::SeqCst) {
            let mut key = vec![0; 8];
            rng.fill_bytes(&mut key);
            let f: usize = rng.gen_range(0, 11);
            let case = (Bytes::from(key), f < frac);
            skiplist_round(&l, &case, &v);
        }
    });
    let mut rng = rand::thread_rng();
    b.iter_batched_ref(
        || {
            let mut key = vec![0; 8];
            let f: usize = rng.gen_range(0, 11);
            rng.fill_bytes(&mut key);
            (Bytes::from(key), f < frac)
        },
        |case| {
            skiplist_round(&list, case, &value);
        },
        BatchSize::SmallInput,
    );
    stop.store(true, Ordering::SeqCst);
    j.join().unwrap();
}

fn bench_read_write_skiplist(c: &mut Criterion) {
    let mut group = c.benchmark_group("skiplist_read_write");
    for i in 0..=10 {
        group.bench_with_input(
            BenchmarkId::from_parameter(i),
            &i,
            bench_read_write_skiplist_frac,
        );
    }
    group.finish();
}

fn map_round(m: &Mutex<HashMap<Bytes, Bytes>>, case: &(Bytes, bool), exp: &Bytes) {
    if case.1 {
        let rm = m.lock().unwrap();
        let value = rm.get(&case.0);
        if let Some(v) = value {
            assert_eq!(v, exp);
        }
    } else {
        let mut rm = m.lock().unwrap();
        rm.insert(case.0.clone(), exp.clone());
    }
}

fn bench_read_write_map_frac(b: &mut Bencher<'_>, frac: &usize) {
    let frac = *frac;
    let value = Bytes::from_static(b"00123");
    let map = Arc::new(Mutex::new(HashMap::with_capacity(512 << 10)));
    let m = map.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();

    let v = value.clone();
    let h = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        while !s.load(Ordering::SeqCst) {
            let mut key = vec![0; 8];
            let f = rng.gen_range(0, 11);
            rng.fill_bytes(&mut key);
            let case = (Bytes::from(key), f < frac);
            map_round(&m, &case, &v);
        }
    });
    let mut rng = rand::thread_rng();
    b.iter_batched_ref(
        || {
            let mut key = vec![0; 8];
            let f = rng.gen_range(0, 11);
            rng.fill_bytes(&mut key);
            (Bytes::from(key), f < frac)
        },
        |case| map_round(&map, case, &value),
        BatchSize::SmallInput,
    );
    stop.store(true, Ordering::SeqCst);
    h.join().unwrap();
}

fn bench_read_write_map(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_read_write");
    for i in 0..=10 {
        group.bench_with_input(
            BenchmarkId::from_parameter(i),
            &i,
            bench_read_write_map_frac,
        );
    }
    group.finish();
}

fn bench_write_skiplist(c: &mut Criterion) {
    let list = Skiplist::with_capacity(512 << 20);
    let value = Bytes::from_static(b"00123");
    let l = list.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();
    let v = value.clone();
    let j = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        while !s.load(Ordering::SeqCst) {
            let mut key = vec![0; 8];
            rng.fill_bytes(&mut key);
            let case = (Bytes::from(key), false);
            skiplist_round(&l, &case, &v);
        }
    });
    let mut rng = rand::thread_rng();
    c.bench_function("skiplist_write", |b| {
        b.iter_batched(
            || {
                let mut key = vec![0; 8];
                rng.fill_bytes(&mut key);
                Bytes::from(key)
            },
            |key| {
                list.put(key, value.clone());
            },
            BatchSize::SmallInput,
        )
    });
    stop.store(true, Ordering::SeqCst);
    j.join().unwrap();
}

criterion_group!(
    benches,
    bench_read_write_skiplist,
    bench_read_write_map,
    bench_write_skiplist
);
criterion_main!(benches);
