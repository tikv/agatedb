use agatedb::{AgateOptions, IteratorOptions};
use bytes::{Bytes, BytesMut};
use clap::clap_app;
use indicatif::{ProgressBar, ProgressStyle};
use rand::Rng;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use std::{sync::mpsc::channel, time::Duration};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn gen_kv_pair(key: u64, value_size: usize) -> (Bytes, Bytes) {
    let key = Bytes::from(format!("vsz={:05}-k={:010}", value_size, key));

    let mut value = BytesMut::with_capacity(value_size);
    value.resize(value_size, 0);

    (key, value.freeze())
}

pub fn unix_time() -> u64 {
    UNIX_EPOCH
        .elapsed()
        .expect("Time went backwards")
        .as_millis() as u64
}

pub struct Rate {
    pub data: std::sync::Arc<std::sync::atomic::AtomicU64>,
    lst_data: u64,
}

impl Rate {
    pub fn new() -> Self {
        Self {
            data: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            lst_data: 0,
        }
    }

    pub fn now(&self) -> u64 {
        self.data.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn rate(&mut self) -> u64 {
        let now = self.now();
        let delta = now - self.lst_data;
        self.lst_data = now;
        delta
    }
}

impl Default for Rate {
    fn default() -> Self {
        Self::new()
    }
}
fn main() {
    let matches = clap_app!(agate_bench =>
        (version: "1.0")
        (author: "TiKV authors")
        (about: "Benchmark for AgateDB")
        (@arg directory: --directory +takes_value +required "database directory")
        (@arg threads: --threads +takes_value default_value("8") "threads")
        (@subcommand populate =>
            (about: "build a database with given keys")
            (version: "1.0")
            (author: "TiKV authors")
            (@arg key_nums: --key_nums +takes_value default_value("1024") "key numbers")
            (@arg seq: --seq +takes_value default_value("true") "write sequentially")
            (@arg value_size: --value_size +takes_value default_value("1024") "value size")
            (@arg chunk_size: --chunk_size +takes_value default_value("1000") "pairs in one txn")
        )
        (@subcommand randread =>
            (about: "randomly read from database")
            (version: "1.0")
            (author: "TiKV authors")
            (@arg key_nums: --key_nums +takes_value default_value("1024") "key numbers")
            (@arg times: --times +takes_value default_value("5") "read how many times")
            (@arg value_size: --value_size +takes_value default_value("1024") "value size")
            (@arg chunk_size: --chunk_size +takes_value default_value("1000") "pairs in one txn")
        )
        (@subcommand iterate =>
            (about: "iterate database")
            (version: "1.0")
            (author: "TiKV authors")
            (@arg times: --times +takes_value default_value("5") "read how many times")
            (@arg value_size: --value_size +takes_value default_value("1024") "value size")
        )
        (@subcommand rocks_populate =>
            (about: "build a database with given keys")
            (version: "1.0")
            (author: "TiKV authors")
            (@arg key_nums: --key_nums +takes_value default_value("1024") "key numbers")
            (@arg seq: --seq +takes_value default_value("true") "write sequentially")
            (@arg value_size: --value_size +takes_value default_value("1024") "value size")
            (@arg chunk_size: --chunk_size +takes_value default_value("1000") "pairs in one txn")
        )
        (@subcommand rocks_randread =>
            (about: "randomly read from database")
            (version: "1.0")
            (author: "TiKV authors")
            (@arg key_nums: --key_nums +takes_value default_value("1024") "key numbers")
            (@arg times: --times +takes_value default_value("5") "read how many times")
            (@arg value_size: --value_size +takes_value default_value("1024") "value size")
            (@arg chunk_size: --chunk_size +takes_value default_value("1000") "pairs in one txn")
        )
        (@subcommand rocks_iterate =>
            (about: "iterate database")
            (version: "1.0")
            (author: "TiKV authors")
            (@arg times: --times +takes_value default_value("5") "read how many times")
            (@arg value_size: --value_size +takes_value default_value("1024") "value size")
        )
    )
    .get_matches();

    let directory = PathBuf::from(matches.value_of("directory").unwrap());
    let threads: usize = matches.value_of("threads").unwrap().parse().unwrap();
    let pool = threadpool::ThreadPool::new(threads);
    let (tx, rx) = channel();

    match matches.subcommand() {
        ("populate", Some(sub_matches)) => {
            let key_nums: u64 = sub_matches.value_of("key_nums").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();
            let chunk_size: u64 = sub_matches.value_of("chunk_size").unwrap().parse().unwrap();

            let mut options = AgateOptions {
                create_if_not_exists: true,
                dir: directory.clone(),
                value_dir: directory,
                managed_txns: true,
                ..Default::default()
            };
            let agate = Arc::new(options.open().unwrap());
            let mut expected = 0;
            // let pb = ProgressBar::new(key_nums);
            let pb = ProgressBar::hidden();
            pb.set_style(ProgressStyle::default_bar()
            .template(
                "{prefix:.bold.dim} [{elapsed_precise}] [{bar:40}] [{per_sec}] ({pos}/{len}) {msg}",
            )
            .progress_chars("=> "));

            let mut write = Rate::new();
            let mut last_report = std::time::Instant::now();

            let seq: bool = sub_matches.value_of("seq").unwrap().parse().unwrap();

            if seq {
                println!("writing sequentially");
            }

            for i in 0..key_nums / chunk_size {
                let agate = agate.clone();
                let tx = tx.clone();
                let write = write.data.clone();
                pool.execute(move || {
                    let range = (i * chunk_size)..((i + 1) * chunk_size);
                    let mut txn = agate.new_transaction_at(unix_time(), true);
                    let mut rng = rand::thread_rng();
                    for j in range {
                        let (key, value) = if seq {
                            gen_kv_pair(j, value_size)
                        } else {
                            gen_kv_pair(rng.gen_range(0, key_nums), value_size)
                        };
                        txn.set(key, value).unwrap();
                        write.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    txn.commit_at(unix_time()).unwrap();
                    tx.send(()).unwrap();
                });
                expected += 1;
            }

            let begin = std::time::Instant::now();

            for _ in rx.iter().take(expected) {
                pb.inc(chunk_size);
                let now = std::time::Instant::now();
                let delta = now.duration_since(last_report);
                if delta > std::time::Duration::from_secs(1) {
                    last_report = now;
                    println!(
                        "{}, rate: {}, total: {}",
                        now.duration_since(begin).as_secs_f64(),
                        write.rate() as f64 / delta.as_secs_f64(),
                        write.now()
                    );
                }
            }
            pb.finish_with_message("done");
        }
        ("randread", Some(sub_matches)) => {
            let key_nums: u64 = sub_matches.value_of("key_nums").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();
            let chunk_size: u64 = sub_matches.value_of("chunk_size").unwrap().parse().unwrap();
            let times: u64 = sub_matches.value_of("times").unwrap().parse().unwrap();

            let mut options = AgateOptions {
                create_if_not_exists: true,
                sync_writes: true,
                dir: directory.clone(),
                value_dir: directory,
                managed_txns: true,
                ..Default::default()
            };
            let agate = Arc::new(options.open().unwrap());
            let mut expected = 0;
            let pb = ProgressBar::new(key_nums * times);
            pb.set_style(ProgressStyle::default_bar()
            .template(
                "{prefix:.bold.dim} [{elapsed_precise}] [{bar:40}] [{per_sec}] ({pos}/{len}) {msg}",
            )
            .progress_chars("=> "));

            let mut missing = Rate::new();
            let mut found = Rate::new();
            let mut last_report = std::time::Instant::now();

            for _ in 0..times {
                for i in 0..key_nums / chunk_size {
                    let agate = agate.clone();
                    let tx = tx.clone();
                    let missing = missing.data.clone();
                    let found = found.data.clone();
                    pool.execute(move || {
                        let range = (i * chunk_size)..((i + 1) * chunk_size);
                        let txn = agate.new_transaction_at(unix_time(), false);
                        let mut rng = rand::thread_rng();
                        for _ in range {
                            let (key, _) = gen_kv_pair(rng.gen_range(0, key_nums), value_size);
                            match txn.get(&key) {
                                Ok(item) => {
                                    assert_eq!(item.value().len(), value_size);
                                    found.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                }
                                Err(err) => {
                                    if matches!(err, agatedb::Error::KeyNotFound(_)) {
                                        missing.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        continue;
                                    } else {
                                        panic!("{:?}", err);
                                    }
                                }
                            }
                        }
                        tx.send(()).unwrap();
                    });
                    expected += 1;
                }
            }

            let begin = std::time::Instant::now();

            for _ in rx.iter().take(expected) {
                let now = std::time::Instant::now();
                let delta = now.duration_since(last_report);
                last_report = now;
                if delta > std::time::Duration::from_secs(1) {
                    println!(
                        "{}, rate: {}, found: {}, missing: {}",
                        now.duration_since(begin).as_secs_f64(),
                        (found.rate() + missing.rate()) as f64 / delta.as_secs_f64(),
                        found.now(),
                        missing.now()
                    );
                }
            }
            pb.finish_with_message("done");
        }
        ("iterate", Some(sub_matches)) => {
            let times: u64 = sub_matches.value_of("times").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();

            let mut options = AgateOptions {
                create_if_not_exists: true,
                sync_writes: true,
                dir: directory.clone(),
                value_dir: directory,
                managed_txns: true,
                ..Default::default()
            };
            let agate = Arc::new(options.open().unwrap());

            let begin = std::time::Instant::now();
            let mut lst_report = std::time::Instant::now();
            let mut total = Rate::new();

            for _ in 0..times {
                let agate = agate.clone();
                let txn = agate.new_transaction_at(unix_time(), false);
                let opts = IteratorOptions::default();
                let mut iter = txn.new_iterator(&opts);
                iter.rewind();
                while iter.valid() {
                    let item = iter.item();
                    assert_eq!(item.value().len(), value_size);
                    total
                        .data
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    iter.next();
                    let now = std::time::Instant::now();
                    if now.duration_since(lst_report) >= Duration::from_secs(1) {
                        lst_report = now;
                        println!(
                            "{}, rate: {}, total: {}",
                            now.duration_since(begin).as_secs_f64(),
                            total.rate(),
                            total.now()
                        );
                    }
                }
            }

            println!(
                "read total {} keys in {}",
                total.now(),
                begin.elapsed().as_secs_f64()
            )
        }
        #[cfg(feature = "enable-rocksdb")]
        ("rocks_populate", Some(sub_matches)) => {
            let key_nums: u64 = sub_matches.value_of("key_nums").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();
            let chunk_size: u64 = sub_matches.value_of("chunk_size").unwrap().parse().unwrap();
            let mut opts = rocksdb::Options::default();
            opts.create_if_missing(true);
            opts.set_compression_type(rocksdb::DBCompressionType::None);

            let db = Arc::new(rocksdb::DB::open(&opts, directory).unwrap());
            let mut expected = 0;
            // let pb = ProgressBar::new(key_nums);
            let pb = ProgressBar::hidden();
            pb.set_style(ProgressStyle::default_bar()
            .template(
                "{prefix:.bold.dim} [{elapsed_precise}] [{bar:40}] [{per_sec}] ({pos}/{len}) {msg}",
            )
            .progress_chars("=> "));

            let mut write = Rate::new();
            let mut last_report = std::time::Instant::now();

            let seq: bool = sub_matches.value_of("seq").unwrap().parse().unwrap();

            if seq {
                println!("writing sequentially");
            }

            for i in 0..key_nums / chunk_size {
                let db = db.clone();
                let tx = tx.clone();
                let write = write.data.clone();
                pool.execute(move || {
                    let mut write_options = rocksdb::WriteOptions::default();
                    write_options.set_sync(true);
                    write_options.disable_wal(false);

                    let range = (i * chunk_size)..((i + 1) * chunk_size);
                    let mut batch = rocksdb::WriteBatch::default();
                    let mut rng = rand::thread_rng();
                    for j in range {
                        let (key, value) = if seq {
                            gen_kv_pair(j, value_size)
                        } else {
                            gen_kv_pair(rng.gen_range(0, key_nums), value_size)
                        };
                        batch.put(key, value);
                        write.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    db.write_opt(batch, &write_options).unwrap();
                    tx.send(()).unwrap();
                });
                expected += 1;
            }

            let begin = std::time::Instant::now();

            for _ in rx.iter().take(expected) {
                pb.inc(chunk_size);
                let now = std::time::Instant::now();
                let delta = now.duration_since(last_report);
                if delta > std::time::Duration::from_secs(1) {
                    last_report = now;
                    println!(
                        "{}, rate: {}, total: {}",
                        now.duration_since(begin).as_secs_f64(),
                        write.rate() as f64 / delta.as_secs_f64(),
                        write.now()
                    );
                }
            }
            pb.finish_with_message("done");
        }
        #[cfg(feature = "enable-rocksdb")]
        ("rocks_randread", Some(sub_matches)) => {
            let key_nums: u64 = sub_matches.value_of("key_nums").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();
            let chunk_size: u64 = sub_matches.value_of("chunk_size").unwrap().parse().unwrap();
            let times: u64 = sub_matches.value_of("times").unwrap().parse().unwrap();
            let mut opts = rocksdb::Options::default();
            opts.create_if_missing(true);
            opts.set_compression_type(rocksdb::DBCompressionType::None);

            let db = Arc::new(rocksdb::DB::open(&opts, directory).unwrap());
            let mut expected = 0;
            let pb = ProgressBar::new(key_nums * times);
            pb.set_style(ProgressStyle::default_bar()
            .template(
                "{prefix:.bold.dim} [{elapsed_precise}] [{bar:40}] [{per_sec}] ({pos}/{len}) {msg}",
            )
            .progress_chars("=> "));

            let mut missing = Rate::new();
            let mut found = Rate::new();
            let mut last_report = std::time::Instant::now();

            for _ in 0..times {
                for i in 0..key_nums / chunk_size {
                    let db = db.clone();
                    let tx = tx.clone();
                    let missing = missing.data.clone();
                    let found = found.data.clone();
                    pool.execute(move || {
                        let range = (i * chunk_size)..((i + 1) * chunk_size);
                        let mut rng = rand::thread_rng();
                        for _ in range {
                            let (key, _) = gen_kv_pair(rng.gen_range(0, key_nums), value_size);
                            match db.get(&key) {
                                Ok(Some(value)) => {
                                    assert_eq!(value.len(), value_size);
                                    found.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                }
                                Ok(None) => {
                                    missing.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    continue;
                                }
                                Err(err) => {
                                    panic!("{:?}", err);
                                }
                            }
                        }
                        tx.send(()).unwrap();
                    });
                    expected += 1;
                }
            }

            let begin = std::time::Instant::now();

            for _ in rx.iter().take(expected) {
                let now = std::time::Instant::now();
                let delta = now.duration_since(last_report);
                last_report = now;
                if delta > std::time::Duration::from_secs(1) {
                    println!(
                        "{}, rate: {}, found: {}, missing: {}",
                        now.duration_since(begin).as_secs_f64(),
                        (found.rate() + missing.rate()) as f64 / delta.as_secs_f64(),
                        found.now(),
                        missing.now()
                    );
                }
            }
            pb.finish_with_message("done");
        }
        #[cfg(feature = "enable-rocksdb")]
        ("rocks_iterate", Some(sub_matches)) => {
            let times: u64 = sub_matches.value_of("times").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();

            let mut opts = rocksdb::Options::default();
            opts.create_if_missing(true);
            opts.set_compression_type(rocksdb::DBCompressionType::None);
            let db = Arc::new(rocksdb::DB::open(&opts, directory).unwrap());

            let begin = std::time::Instant::now();
            let mut lst_report = std::time::Instant::now();
            let mut total = Rate::new();

            for _ in 0..times {
                let iter = db.iterator(rocksdb::IteratorMode::Start);
                for (_, value) in iter {
                    assert_eq!(value.len(), value_size);
                    total
                        .data
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let now = std::time::Instant::now();
                    if now.duration_since(lst_report) >= Duration::from_secs(1) {
                        lst_report = now;
                        println!(
                            "{}, rate: {}, total: {}",
                            now.duration_since(begin).as_secs_f64(),
                            total.rate(),
                            total.now()
                        );
                    }
                }
            }

            println!(
                "read total {} keys in {}",
                total.now(),
                begin.elapsed().as_secs_f64()
            )
        }
        _ => panic!("unsupported command"),
    }
}
