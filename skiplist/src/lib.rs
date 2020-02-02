mod arena;
mod key;
mod list;

const MAX_HEIGHT: usize = 20;

pub use key::{FixedLengthSuffixComparitor, KeyComparitor};
pub use list::Skiplist;
