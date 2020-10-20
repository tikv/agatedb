mod arena;
mod key;
mod list;

const MAX_HEIGHT: usize = 20;

pub use key::{FixedLengthSuffixComparator, KeyComparator};
pub use list::Skiplist;
