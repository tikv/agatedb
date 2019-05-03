#[macro_use]
extern crate failure;

mod db;
mod error;
mod log;
mod memtable;

pub use error::{Error, Result};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
