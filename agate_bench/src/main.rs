use agatedb::AgateOptions;
use bytes::{Bytes, BytesMut};
use clap::clap_app;
use indicatif::ProgressBar;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use yatp::task::callback::Handle;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn gen_kv_pair(key: u64, value_size: usize) -> (Bytes, Bytes) {
    let key = Bytes::from(format!("{:016x}", key));
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

    let directory = PathBuf::from(matches.value_of("directory").unwrap());
    let threads: usize = matches.value_of("threads").unwrap().parse().unwrap();
    let pool = yatp::Builder::new("agatedb_bench")
        .max_thread_count(threads)
        .min_thread_count(threads)
        .build_callback_pool();
    let (tx, rx) = channel();

    match matches.subcommand() {
        ("build", Some(sub_matches)) => {
            let key_nums: u64 = sub_matches.value_of("key_nums").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();
            let chunk_size: u64 = sub_matches.value_of("chunk_size").unwrap().parse().unwrap();

            let mut options = AgateOptions {
                create_if_not_exists: true,
                dir: directory.clone(),
                value_dir: directory,
                ..Default::default()
            };
            let agate = Arc::new(options.open().unwrap());
            let mut expected = 0;
            let pb = ProgressBar::new(key_nums);

            for i in 0..key_nums / chunk_size {
                let agate = agate.clone();
                let tx = tx.clone();
                let pb = pb.clone();
                pool.spawn(move |_: &mut Handle<'_>| {
                    let range = i * chunk_size..(i + 1) * chunk_size;
                    let mut txn = agate.new_transaction_at(unix_time(), true);
                    for j in range.clone() {
                        let (key, value) = gen_kv_pair(j, value_size);
                        txn.set(key, value).unwrap();
                    }
                    txn.commit_at(unix_time()).unwrap();
                    pb.set_message(&format!("processed {:?}", range));
                    pb.inc(chunk_size);
                    tx.send(()).unwrap();
                });
                expected += 1;
            }

            rx.iter().take(expected).fold((), |_, _| ());
            pb.finish_with_message("done");

            pool.shutdown();
        }
        _ => panic!("unsupported command"),
    }
}
