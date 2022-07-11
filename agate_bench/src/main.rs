use std::{path::PathBuf, sync::Arc, time::Instant};

use agatedb::AgateOptions;
use clap::clap_app;
use indicatif::{ProgressBar, ProgressStyle};

#[path = "../../benches/common.rs"]
mod common;

use common::{
    agate_iterate, agate_populate, agate_randread, rocks_iterate, rocks_populate, rocks_randread,
};

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
            (@arg batch_size: --batch_size +takes_value default_value("1000") "pairs in one txn")
            (@arg value_size: --value_size +takes_value default_value("1024") "value size")
        )
        (@subcommand randread =>
            (about: "randomly read from database")
            (version: "1.0")
            (author: "TiKV authors")
            (@arg times: --times +takes_value default_value("5") "read how many times")
            (@arg key_nums: --key_nums +takes_value default_value("1024") "key numbers")
            (@arg value_size: --value_size +takes_value default_value("1024") "value size")
        )
        (@subcommand iterate =>
            (about: "iterate database")
            (version: "1.0")
            (author: "TiKV authors")
            (@arg times: --times +takes_value default_value("5") "read how many times")
            (@arg key_nums: --key_nums +takes_value default_value("1024") "key numbers")
            (@arg value_size: --value_size +takes_value default_value("1024") "value size")
        )
        (@subcommand rocks_populate =>
            (about: "build a database with given keys")
            (version: "1.0")
            (author: "TiKV authors")
            (@arg key_nums: --key_nums +takes_value default_value("1024") "key numbers")
            (@arg batch_size: --batch_size +takes_value default_value("1000") "pairs in one txn")
            (@arg value_size: --value_size +takes_value default_value("1024") "value size")
        )
        (@subcommand rocks_randread =>
            (about: "randomly read from database")
            (version: "1.0")
            (author: "TiKV authors")
            (@arg times: --times +takes_value default_value("5") "read how many times")
            (@arg key_nums: --key_nums +takes_value default_value("1024") "key numbers")
            (@arg value_size: --value_size +takes_value default_value("1024") "value size")
        )
        (@subcommand rocks_iterate =>
            (about: "iterate database")
            (version: "1.0")
            (author: "TiKV authors")
            (@arg times: --times +takes_value default_value("5") "read how many times")
            (@arg key_nums: --key_nums +takes_value default_value("1024") "key numbers")
            (@arg value_size: --value_size +takes_value default_value("1024") "value size")
        )
    )
    .get_matches();

    let directory = PathBuf::from(matches.value_of("directory").unwrap());
    let threads: u64 = matches.value_of("threads").unwrap().parse().unwrap();

    let mut agate_opts = AgateOptions {
        dir: directory.clone(),
        value_dir: directory.clone(),
        sync_writes: true,
        managed_txns: true,
        ..Default::default()
    };

    let mut rocks_opts = rocksdb::Options::default();
    rocks_opts.create_if_missing(true);
    rocks_opts.set_compression_type(rocksdb::DBCompressionType::None);

    match matches.subcommand() {
        ("populate", Some(sub_matches)) => {
            let key_nums: u64 = sub_matches.value_of("key_nums").unwrap().parse().unwrap();
            let batch_size: u64 = sub_matches.value_of("batch_size").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();

            let agate = Arc::new(agate_opts.open().unwrap());
            let chunk_size = key_nums / threads;

            let begin = Instant::now();

            agate_populate(agate, key_nums, chunk_size, batch_size, value_size);

            let cost = begin.elapsed();

            println!("populate {} keys in {} ms", key_nums, cost.as_millis());
        }
        ("randread", Some(sub_matches)) => {
            let times: u64 = sub_matches.value_of("times").unwrap().parse().unwrap();
            let key_nums: u64 = sub_matches.value_of("key_nums").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();

            let agate = Arc::new(agate_opts.open().unwrap());
            let chunk_size = key_nums / threads;

            let pb = ProgressBar::new(key_nums * times);
            pb.set_style(ProgressStyle::default_bar()
            .template(
                "{prefix:.bold.dim} [{elapsed_precise}] [{bar:40}] [{per_sec}] ({pos}/{len}) {msg}",
            )
            .progress_chars("=> "));

            let begin = Instant::now();

            for _ in 0..times {
                agate_randread(agate.clone(), key_nums, chunk_size, value_size);
                pb.inc(key_nums);
            }

            pb.finish_with_message("done");

            let cost = begin.elapsed();
            println!(
                "randread {} keys {} times in {} ms",
                key_nums,
                times,
                cost.as_millis()
            );
        }
        ("iterate", Some(sub_matches)) => {
            let times: u64 = sub_matches.value_of("times").unwrap().parse().unwrap();
            let key_nums: u64 = sub_matches.value_of("key_nums").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();

            let agate = Arc::new(agate_opts.open().unwrap());
            let chunk_size = key_nums / threads;

            let pb = ProgressBar::new(times);
            pb.set_style(ProgressStyle::default_bar()
            .template(
                "{prefix:.bold.dim} [{elapsed_precise}] [{bar:40}] [{per_sec}] ({pos}/{len}) {msg}",
            )
            .progress_chars("=> "));

            let begin = Instant::now();

            for _ in 0..times {
                agate_iterate(agate.clone(), key_nums, chunk_size, value_size);
                pb.inc(1);
            }

            pb.finish_with_message("done");

            let cost = begin.elapsed();
            println!(
                "iterate {} keys {} times in {} ms",
                key_nums,
                times,
                cost.as_millis()
            );
        }
        ("rocks_populate", Some(sub_matches)) => {
            let key_nums: u64 = sub_matches.value_of("key_nums").unwrap().parse().unwrap();
            let batch_size: u64 = sub_matches.value_of("batch_size").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();

            let db = Arc::new(rocksdb::DB::open(&rocks_opts, &directory).unwrap());
            let chunk_size = key_nums / threads;

            let begin = Instant::now();

            rocks_populate(db, key_nums, chunk_size, batch_size, value_size);

            let cost = begin.elapsed();

            println!("populate {} keys in {} ms", key_nums, cost.as_millis());
        }
        ("rocks_randread", Some(sub_matches)) => {
            let times: u64 = sub_matches.value_of("times").unwrap().parse().unwrap();
            let key_nums: u64 = sub_matches.value_of("key_nums").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();

            let db = Arc::new(rocksdb::DB::open(&rocks_opts, &directory).unwrap());
            let chunk_size = key_nums / threads;

            let pb = ProgressBar::new(key_nums * times);
            pb.set_style(ProgressStyle::default_bar()
            .template(
                "{prefix:.bold.dim} [{elapsed_precise}] [{bar:40}] [{per_sec}] ({pos}/{len}) {msg}",
            )
            .progress_chars("=> "));

            let begin = Instant::now();

            for _ in 0..times {
                rocks_randread(db.clone(), key_nums, chunk_size, value_size);
                pb.inc(key_nums);
            }

            pb.finish_with_message("done");

            let cost = begin.elapsed();
            println!(
                "randread {} keys {} times in {} ms",
                key_nums,
                times,
                cost.as_millis()
            );
        }
        ("rocks_iterate", Some(sub_matches)) => {
            let times: u64 = sub_matches.value_of("times").unwrap().parse().unwrap();
            let key_nums: u64 = sub_matches.value_of("key_nums").unwrap().parse().unwrap();
            let value_size: usize = sub_matches.value_of("value_size").unwrap().parse().unwrap();

            let db = Arc::new(rocksdb::DB::open(&rocks_opts, &directory).unwrap());
            let chunk_size = key_nums / threads;

            let pb = ProgressBar::new(times);
            pb.set_style(ProgressStyle::default_bar()
            .template(
                "{prefix:.bold.dim} [{elapsed_precise}] [{bar:40}] [{per_sec}] ({pos}/{len}) {msg}",
            )
            .progress_chars("=> "));

            let begin = Instant::now();

            for _ in 0..times {
                rocks_iterate(db.clone(), key_nums, chunk_size, value_size);
                pb.inc(1);
            }

            pb.finish_with_message("done");

            let cost = begin.elapsed();
            println!(
                "iterate {} keys {} times in {} ms",
                key_nums,
                times,
                cost.as_millis()
            );
        }

        _ => panic!("unsupported command"),
    }
}
