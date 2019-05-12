#[macro_use]
extern crate failure;

mod db;
mod error;
mod wal;
mod memtable;

pub use error::{Error, Result};
pub use db::{Agate, AgateOptions};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
