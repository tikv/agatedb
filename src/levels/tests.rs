use super::*;
use crate::{db::tests::with_agate_test, table::new_filename, Agate, TableOptions};

pub fn helper_dump_levels(lvctl: &LevelsController) {
    for level in &lvctl.core.levels {
        let level = level.read().unwrap();
        println!("--- Level {} ---", level.level);
        for table in &level.tables {
            println!(
                "#{} ({:?} - {:?}, {})",
                table.id(),
                table.smallest(),
                table.biggest(),
                table.size()
            );
        }
    }
}

struct KeyValVersion {
    key: Bytes,
    value: Bytes,
    version: u64,
    meta: u8,
}

impl KeyValVersion {
    fn new(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>, version: u64, meta: u8) -> Self {
        Self {
            key: Bytes::copy_from_slice(key.as_ref()),
            value: Bytes::copy_from_slice(value.as_ref()),
            version,
            meta,
        }
    }
}

fn create_and_open(agate: &mut Agate, td: Vec<KeyValVersion>, level: usize) {
    let mut table_opts = TableOptions::default();
    table_opts.block_size = agate.core.opts.block_size;
    table_opts.bloom_false_positive = agate.core.opts.bloom_false_positive;
    let mut builder = TableBuilder::new(table_opts.clone());
    for item in td {
        let key = key_with_ts(&item.key[..], item.version);
        let value = Value::new_with_meta(item.value.clone(), item.meta, 0);
        builder.add(&key, value, 0);
    }
    let filename = new_filename(agate.core.lvctl.reserve_file_id(), &agate.core.opts.dir);
    let table = Table::create(&filename, builder.finish(), table_opts).unwrap();
    agate
        .core
        .manifest
        .add_changes(vec![new_create_change(table.id(), level, 0)])
        .unwrap();
    let mut lv = agate.core.lvctl.core.levels[level].write().unwrap();
    lv.tables.push(table);
}

#[test]
fn overlap_same_keys() {
    with_agate_test(|agate| {
        let l0 = KeyValVersion::new("foo", "bar", 3, 0);
        let l1 = KeyValVersion::new("foo", "bar", 2, 0);
        create_and_open(agate, vec![l0], 0);
        create_and_open(agate, vec![l1], 1);

        let l0_tables = agate.core.lvctl.core.levels[0]
            .read()
            .unwrap()
            .tables
            .clone();
        let l1_tables = agate.core.lvctl.core.levels[1]
            .read()
            .unwrap()
            .tables
            .clone();

        // lv0 should overlap with lv0 tables
        assert!(agate.core.lvctl.core.check_overlap(&l0_tables, 0));
        // lv1 should overlap with lv0 tables
        assert!(agate.core.lvctl.core.check_overlap(&l0_tables, 1));
        // lv2 and lv3 should not overlap with lv0 tables
        assert!(!agate.core.lvctl.core.check_overlap(&l0_tables, 2));
        assert!(!agate.core.lvctl.core.check_overlap(&l1_tables, 2));
        assert!(!agate.core.lvctl.core.check_overlap(&l0_tables, 3));
        assert!(!agate.core.lvctl.core.check_overlap(&l1_tables, 3));
    });
}

#[test]
fn overlap_overlapping_keys() {
    with_agate_test(|agate| {
        let l0 = vec![
            KeyValVersion::new("a", "x", 1, 0),
            KeyValVersion::new("b", "x", 1, 0),
            KeyValVersion::new("foo", "bar", 3, 0),
        ];
        let l1 = vec![KeyValVersion::new("foo", "bar", 2, 0)];
        create_and_open(agate, l0, 0);
        create_and_open(agate, l1, 1);

        let l0_tables = agate.core.lvctl.core.levels[0]
            .read()
            .unwrap()
            .tables
            .clone();
        let l1_tables = agate.core.lvctl.core.levels[1]
            .read()
            .unwrap()
            .tables
            .clone();

        // lv0 should overlap with lv0 tables
        assert!(agate.core.lvctl.core.check_overlap(&l0_tables, 0));
        assert!(agate.core.lvctl.core.check_overlap(&l1_tables, 1));
        // lv1 should overlap with lv0 tables
        assert!(agate.core.lvctl.core.check_overlap(&l0_tables, 1));
        // lv2 and lv3 should not overlap with lv0 tables
        assert!(!agate.core.lvctl.core.check_overlap(&l0_tables, 2));
        assert!(!agate.core.lvctl.core.check_overlap(&l0_tables, 3));
    });
}

#[test]
fn overlap_non_overlapping_keys() {
    with_agate_test(|agate| {
        let l0 = vec![
            KeyValVersion::new("a", "x", 1, 0),
            KeyValVersion::new("b", "x", 1, 0),
            KeyValVersion::new("c", "bar", 3, 0),
        ];
        let l1 = vec![KeyValVersion::new("foo", "bar", 2, 0)];
        create_and_open(agate, l0, 0);
        create_and_open(agate, l1, 1);

        let l0_tables = agate.core.lvctl.core.levels[0]
            .read()
            .unwrap()
            .tables
            .clone();
        let l1_tables = agate.core.lvctl.core.levels[1]
            .read()
            .unwrap()
            .tables
            .clone();

        // lv1 should not overlap with lv0 tables
        assert!(!agate.core.lvctl.core.check_overlap(&l0_tables, 1));
        // lv2 and lv3 should not overlap with lv0 tables
        assert!(!agate.core.lvctl.core.check_overlap(&l0_tables, 2));
        assert!(!agate.core.lvctl.core.check_overlap(&l0_tables, 3));
    });
}
