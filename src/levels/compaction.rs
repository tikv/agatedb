use std::collections::HashSet;

use std::sync::{Arc, RwLock};

use bytes::{Bytes, BytesMut};

use super::LevelHandler;
use crate::format::{key_with_ts, user_key};
use crate::util::{KeyComparator, COMPARATOR};
use crate::{Error, Result, Table};

/// Represents a range of keys from `left` to `right`
/// TODO: use enum for this struct to represent infinite / finite range
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum KeyRange {
    Range { left: Bytes, right: Bytes },
    Inf,
}

impl KeyRange {
    pub fn inf() -> Self {
        Self::Inf
    }

    pub fn new(left: Bytes, right: Bytes) -> Self {
        assert!(COMPARATOR.compare_key(&left, &right) != std::cmp::Ordering::Greater);
        Self::Range { left, right }
    }

    /// Extend current key range with another
    pub fn extend(&self, other: &Self) -> Self {
        use KeyRange::{Inf, Range};
        match (self, other) {
            (Inf, _) => Inf,
            (_, Inf) => Inf,
            (
                Range {
                    left: self_left,
                    right: self_right,
                },
                Range {
                    left: other_left,
                    right: other_right,
                },
            ) => {
                let left =
                    if COMPARATOR.compare_key(other_left, self_left) == std::cmp::Ordering::Less {
                        other_left
                    } else {
                        self_left
                    }
                    .clone();
                let right = if COMPARATOR.compare_key(other_right, self_right)
                    == std::cmp::Ordering::Greater
                {
                    other_right
                } else {
                    self_right
                }
                .clone();
                Range { left, right }
            }
        }
    }

    /// Check if two key ranges overlap
    pub fn overlaps_with(&self, other: &Self) -> bool {
        use KeyRange::{Inf, Range};
        match (self, other) {
            (Inf, _) => true,
            (_, Inf) => true,
            (
                Range {
                    left: self_left,
                    right: self_right,
                },
                Range {
                    left: other_left,
                    right: other_right,
                },
            ) => {
                // [other_left, other_right] ... [self_left, self_right]
                if COMPARATOR.compare_key(other_right, self_left) == std::cmp::Ordering::Less {
                    return false;
                }
                // [self_left, self_right] ... [other_left, other_right]
                if COMPARATOR.compare_key(self_right, other_right) == std::cmp::Ordering::Less {
                    return false;
                }
                true
            }
        }
    }
}

#[derive(Default)]
pub struct LevelCompactStatus {
    pub ranges: Vec<KeyRange>,
    pub del_size: u64,
}

impl LevelCompactStatus {
    pub fn remove(&mut self, dst: &KeyRange) -> bool {
        let prev_ranges_len = self.ranges.len();
        // TODO: remove in place requires `drain_filter` feature.
        self.ranges = self.ranges.iter().filter(|x| x != &dst).cloned().collect();

        prev_ranges_len != self.ranges.len()
    }

    pub fn overlaps_with(&self, dst: &Option<KeyRange>) -> bool {
        if let Some(dst) = dst {
            for r in self.ranges.iter() {
                if r.overlaps_with(dst) {
                    return true;
                }
            }
            false
        } else {
            true
        }
    }
}

pub struct CompactStatus {
    pub levels: Vec<LevelCompactStatus>,
    pub tables: HashSet<u64>,
}

#[derive(Clone)]
pub struct CompactDef {
    pub compactor_id: usize,
    pub this_level: Arc<RwLock<LevelHandler>>,
    pub next_level: Arc<RwLock<LevelHandler>>,
    pub this_level_id: usize,
    pub next_level_id: usize,
    pub targets: Targets,
    pub prios: CompactionPriority,

    pub this_range: Option<KeyRange>,
    pub next_range: Option<KeyRange>,
    pub splits: Vec<KeyRange>,

    pub top: Vec<Table>,
    pub bot: Vec<Table>,

    pub this_size: u64,

    pub drop_prefixes: Vec<Bytes>,
}

impl CompactDef {
    pub fn new(
        compactor_id: usize,
        this_level: Arc<RwLock<LevelHandler>>,
        this_level_id: usize,
        next_level: Arc<RwLock<LevelHandler>>,
        next_level_id: usize,
        prios: CompactionPriority,
        targets: Targets,
    ) -> Self {
        Self {
            compactor_id,
            this_level,
            next_level,
            this_level_id,
            next_level_id,
            this_range: None,
            next_range: None,
            splits: vec![],
            this_size: 0,
            drop_prefixes: vec![],
            top: vec![],
            bot: vec![],
            targets,
            prios,
        }
    }

    pub fn all_tables(&self) -> Vec<Table> {
        let mut tables = self.top.clone();
        tables.append(&mut self.bot.clone());
        tables
    }
}

impl CompactStatus {
    pub fn delete(&mut self, compact_def: &CompactDef) {
        let this_level_id = compact_def.this_level_id;
        assert!(this_level_id < self.levels.len() - 1);

        let this_level = &mut self.levels[this_level_id];
        let next_level_id = compact_def.next_level_id;

        this_level.del_size -= compact_def.this_size;
        let mut found = this_level.remove(compact_def.this_range.as_ref().unwrap());
        drop(this_level);

        if let Some(next_range) = &compact_def.next_range {
            let next_level = &mut self.levels[next_level_id];
            found = next_level.remove(&next_range) && found;
        }

        if !found {
            let this = compact_def.this_range.clone();
            let next = compact_def.next_range.clone();
            println!("looking for {:?} in this level", this);
            println!("looking for {:?} in next level", next);
            panic!("key range not found");
        }

        for table in compact_def.top.iter() {
            assert!(self.tables.remove(&table.id()));
        }

        for table in compact_def.bot.iter() {
            assert!(self.tables.remove(&table.id()));
        }
    }

    pub fn compare_and_add(&mut self, compact_def: &CompactDef) -> Result<()> {
        let this_level = compact_def.this_level_id;
        assert!(this_level < self.levels.len() - 1);

        let next_level = compact_def.next_level_id;
        if self.levels[this_level].overlaps_with(&compact_def.this_range) {
            return Err(Error::CompactionError(format!(
                "{:?} overlap with this level {} {:?}",
                compact_def.this_range, compact_def.this_level_id, self.levels[this_level].ranges
            )));
        }
        if self.levels[next_level].overlaps_with(&compact_def.next_range) {
            return Err(Error::CompactionError(format!(
                "{:?} overlap with next level {} {:?}",
                compact_def.next_range, compact_def.next_level_id, self.levels[next_level].ranges
            )));
        }

        self.levels[this_level]
            .ranges
            .push(compact_def.this_range.clone().unwrap());
        self.levels[next_level]
            .ranges
            .push(compact_def.next_range.clone().unwrap());

        self.levels[this_level].del_size += compact_def.this_size;

        for table in compact_def.top.iter() {
            assert!(self.tables.insert(table.id()));
        }

        for table in compact_def.bot.iter() {
            assert!(self.tables.insert(table.id()));
        }

        Ok(())
    }

    pub fn overlaps_with(&self, level: usize, this: &Option<KeyRange>) -> bool {
        let this_level = &self.levels[level];
        this_level.overlaps_with(this)
    }
}

#[derive(Clone, Debug)]
pub struct CompactionPriority {
    pub level: usize,
    pub score: f64,
    pub adjusted: f64,
    pub drop_prefixes: Vec<Bytes>,
    pub targets: Targets,
}

#[derive(Clone, Debug)]
pub struct Targets {
    pub base_level: usize,
    pub target_size: Vec<u64>,
    pub file_size: Vec<u64>,
}

impl Targets {
    pub fn new() -> Self {
        Self {
            base_level: 0,
            target_size: vec![],
            file_size: vec![],
        }
    }
}

pub fn get_key_range(tables: &[Table]) -> Option<KeyRange> {
    if tables.is_empty() {
        return None;
    }

    let mut smallest = tables[0].smallest().clone();
    let mut biggest = tables[0].biggest().clone();

    for i in 1..tables.len() {
        if COMPARATOR.compare_key(tables[i].smallest(), &smallest) == std::cmp::Ordering::Less {
            smallest = tables[i].smallest().clone();
        }
        if COMPARATOR.compare_key(tables[i].biggest(), &biggest) == std::cmp::Ordering::Greater {
            biggest = tables[i].biggest().clone();
        }
    }
    let mut smallest_buf = BytesMut::with_capacity(smallest.len() + 8);
    let mut biggest_buf = BytesMut::with_capacity(biggest.len() + 8);
    smallest_buf.extend_from_slice(user_key(&smallest));
    biggest_buf.extend_from_slice(user_key(&biggest));
    return Some(KeyRange::new(
        key_with_ts(smallest_buf, std::u64::MAX),
        key_with_ts(biggest_buf, 0),
    ));
}

pub fn get_key_range_single(table: &Table) -> KeyRange {
    let smallest = table.smallest().clone();
    let biggest = table.biggest().clone();

    let mut smallest_buf = BytesMut::with_capacity(smallest.len() + 8);
    let mut biggest_buf = BytesMut::with_capacity(biggest.len() + 8);
    smallest_buf.extend_from_slice(user_key(&smallest));
    biggest_buf.extend_from_slice(user_key(&biggest));
    return KeyRange::new(
        key_with_ts(smallest_buf, std::u64::MAX),
        key_with_ts(biggest_buf, 0),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyrange_extend_non_overlap() {
        let k1 = KeyRange::new(
            Bytes::from_static(b"000000000000"),
            Bytes::from_static(b"dddd00000000"),
        );
        let k2 = KeyRange::new(
            Bytes::from_static(b"eeee00000000"),
            Bytes::from_static(b"ffff00000000"),
        );

        assert_eq!(
            k1.extend(&k2),
            KeyRange::new(
                Bytes::from_static(b"000000000000"),
                Bytes::from_static(b"ffff00000000")
            )
        );

        assert_eq!(
            k2.extend(&k1),
            KeyRange::new(
                Bytes::from_static(b"000000000000"),
                Bytes::from_static(b"ffff00000000")
            )
        );
    }

    #[test]
    fn test_keyrange_extend_overlap() {
        let k1 = KeyRange::new(
            Bytes::from_static(b"000000000000"),
            Bytes::from_static(b"eeee00000000"),
        );
        let k2 = KeyRange::new(
            Bytes::from_static(b"dddd00000000"),
            Bytes::from_static(b"ffff00000000"),
        );

        assert_eq!(
            k1.extend(&k2),
            KeyRange::new(
                Bytes::from_static(b"000000000000"),
                Bytes::from_static(b"ffff00000000")
            )
        );

        assert_eq!(
            k2.extend(&k1),
            KeyRange::new(
                Bytes::from_static(b"000000000000"),
                Bytes::from_static(b"ffff00000000")
            )
        );
    }

    #[test]
    fn test_keyrange_extend_inf() {
        let k1 = KeyRange::inf();
        let k2 = KeyRange::new(
            Bytes::from_static(b"dddd00000000"),
            Bytes::from_static(b"ffff00000000"),
        );

        assert_eq!(k1.extend(&k2), KeyRange::inf());
        assert_eq!(k2.extend(&k1), KeyRange::inf());
    }
}
