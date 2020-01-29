use criterion::*;
use rand::prelude::*;
use skiplist::*;
use std::collections::*;
use std::sync::atomic::*;
use std::sync::*;
use std::thread;

fn skiplist_round(rng: &mut ThreadRng, frac: usize, l: &Skiplist, key: &mut [u8], exp: &[u8]) {
    let f: usize = rng.gen_range(0, 11);
    rng.fill_bytes(key);
    if f < frac {
        if let Some(v) = l.get(&key) {
            assert_eq!(v, exp);
        }
    } else {
        l.put(key, exp);
    }
}

fn bench_read_write_skiplist_frac(b: &mut Bencher<'_>, frac: &usize) {
    let frac = *frac;
    let value = b"00123";
    let list = Skiplist::with_capacity(512 << 20);
    let l = list.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();
    let j = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let mut key = [0; 8];
        while !s.load(Ordering::SeqCst) {
            skiplist_round(&mut rng, frac, &l, &mut key, value);
        }
    });
    let mut rng = rand::thread_rng();
    let mut key = [0; 8];
    b.iter(|| {
        skiplist_round(&mut rng, frac, &list, &mut key, value);
    });
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

fn map_round(
    rng: &mut ThreadRng,
    frac: usize,
    m: &Mutex<HashMap<[u8; 8], [u8; 5]>>,
    key: &mut [u8; 8],
    exp: &[u8; 5],
) {
    let f: usize = rng.gen_range(0, 11);
    rng.fill_bytes(key);
    if f < frac {
        let rm = m.lock().unwrap();
        let value = rm.get(key);
        if let Some(v) = value {
            assert_eq!(v, exp);
        }
    } else {
        let mut rm = m.lock().unwrap();
        rm.insert(key.clone(), exp.clone());
    }
}

fn bench_read_write_map_frac(b: &mut Bencher<'_>, frac: &usize) {
    let frac = *frac;
    let value = b"00123";
    let map = Arc::new(Mutex::new(HashMap::with_capacity(512 << 10)));
    let m = map.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();

    let h = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let mut key = [0; 8];
        while !s.load(Ordering::SeqCst) {
            map_round(&mut rng, frac, &m, &mut key, value);
        }
    });
    let mut rng = rand::thread_rng();
    let mut key = [0; 8];
    b.iter(|| {
        map_round(&mut rng, frac, &map, &mut key, value);
    });
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
    let value = b"00123";
    let l = list.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();
    let j = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let mut key = [0; 8];
        while !s.load(Ordering::SeqCst) {
            skiplist_round(&mut rng, 10, &l, &mut key, value);
        }
    });
    let mut key = [0; 8];
    let mut rng = rand::thread_rng();
    c.bench_function("skiplist_write", |b| {
        b.iter(|| {
            rng.fill_bytes(&mut key);
            list.put(&key, value);
        })
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
