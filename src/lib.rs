#![allow(dead_code)]
#![feature(slice_fill)]

mod bloom;
mod checksum;
mod db;
mod entry;
mod error;
mod format;
mod iterator_trait;
mod levels;
mod memtable;
mod ops;
mod opt;
mod table;
mod util;
mod value;
mod wal;

pub use format::{get_ts, key_with_ts};
pub use opt::ChecksumVerificationMode;
pub use opt::Options as TableOptions;
pub use table::builder::Builder as TableBuilder;
pub use table::Table;
pub use value::Value;

pub use db::{Agate, AgateOptions};
pub use error::{Error, Result};
pub use iterator_trait::AgateIterator;
pub use skiplist::Skiplist;
