#[macro_use]
extern crate failure;

mod db;
mod entry;
mod error;
pub mod format;
mod levels;
mod memtable;
mod ops;
mod wal;

pub use db::{Agate, AgateOptions};
pub use error::{Error, Result};
pub use skiplist::Skiplist;
