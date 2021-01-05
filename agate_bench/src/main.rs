use agatedb::AgateOptions;
use bytes::{Bytes, BytesMut};
use clap::clap_app;
use indicatif::{ProgressBar, ProgressStyle};
use rand::Rng;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_millis() as u64
}

fn main() {
    let matches = clap_app!(agate_bench =>
        (version: "1.0")
        (author: "Alex Chi <iskyzh@gmail.com>")
        (about: "Benchmark for AgateDB")
        (@arg directory: --directory +takes_value +required "database directory")
        (@arg threads: --threads +takes_value default_value("8") "threads")
        (@subcommand build =>
            (about: "build a database with given keys")
            (version: "1.0")
            (author: "Alex Chi <iskyzh@gmail.com>")
            (@arg key_nums: --key_nums +takes_value default_value("1024") "key numbers")
            (@arg value_size: --value_size +takes_value default_value("1024") "value size")
            (@arg chunk_size: --chunk_size +takes_value default_value("100") "pairs in one txn")
        )
    )
    .get_matches();

    let directory = matches.value_of("directory").unwrap();
    let threads: usize = matches.value_of("threads").unwrap().parse().unwrap();
    let pool = threadpool::ThreadPool::new(threads);
    let (tx, rx) = channel();

    match matches.subcommand() {
        ("build", Some(sub_matches)) => {
            let key_nums: u64 = sub_matches.value_of("key_nums").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();
            let chunk_size: u64 = sub_matches.value_of("chunk_size").unwrap().parse().unwrap();

            let mut options = AgateOptions::default();
            options.create_if_not_exists = true;
            options.sync_writes = true;
            let agate = Arc::new(options.open(directory).unwrap());
            let mut expected = 0;
            let pb = ProgressBar::new(key_nums);
            pb.set_style(ProgressStyle::default_bar()
            .template(
                "{prefix:.bold.dim} [{elapsed_precise}] [{bar:40}] [{per_sec}] ({pos}/{len}) {msg}",
            )
            .progress_chars("=> "));

            for i in 0..key_nums / chunk_size {
                let agate = agate.clone();
                let tx = tx.clone();
                pool.execute(move || {
                    let range = (i * chunk_size)..((i + 1) * chunk_size);
                    let mut txn = agate.new_transaction_at(unix_time(), true);
                    let mut rng = rand::thread_rng();
                    for _ in range.clone() {
                        let (key, value) = gen_kv_pair(rng.gen_range(0, key_nums), value_size);
                        txn.set(key, value).unwrap();
                    }
                    txn.commit_at(unix_time()).unwrap();
                    tx.send(()).unwrap();
                });
                expected += 1;
            }

            for _ in rx.iter().take(expected) {
                pb.inc(chunk_size);
            }
            pb.finish_with_message("done");
        }
        _ => panic!("unsupported command"),
    }
}
