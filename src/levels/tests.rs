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

mod overlap {
    use super::*;

    #[test]
    fn test_same_keys() {
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
    fn test_overlapping_keys() {
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
    fn test_non_overlapping_keys() {
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

            // lv1 should not overlap with lv0 tables
            assert!(!agate.core.lvctl.core.check_overlap(&l0_tables, 1));
            // lv2 and lv3 should not overlap with lv0 tables
            assert!(!agate.core.lvctl.core.check_overlap(&l0_tables, 2));
            assert!(!agate.core.lvctl.core.check_overlap(&l0_tables, 3));
        });
    }
}

macro_rules! assert_bytes_eq {
    ($left:expr, $right:expr) => {
        assert_eq!(
            Bytes::copy_from_slice($left),
            Bytes::copy_from_slice($right)
        )
    };
}

fn get_all_and_check(agate: &mut Agate, expected: Vec<KeyValVersion>) {
    agate
        .view(|txn| {
            let mut iter_opts = IteratorOptions::default();
            iter_opts.all_versions = true;
            let mut iter = txn.new_iterator(&iter_opts);
            iter.rewind();
            let mut cnt = 0;
            while iter.valid() {
                let it = iter.item();
                assert!(cnt < expected.len());
                assert_bytes_eq!(&it.key, &expected[cnt].key);
                assert_bytes_eq!(&it.vptr, &expected[cnt].value);
                assert_eq!(it.version, expected[cnt].version);
                assert_eq!(it.meta, expected[cnt].meta);
                iter.next();
                cnt += 1;
            }
            assert_eq!(cnt, expected.len());
            Ok(())
        })
        .unwrap();
}

mod compaction {
    use super::*;
    use crate::value::*;

    fn generate_test_compect_def(
        agate: &Agate,
        this_level_id: usize,
        next_level_id: usize,
    ) -> CompactDef {
        let targets = agate.core.lvctl.core.level_targets();
        let cpt_prio = CompactionPriority {
            targets: targets.clone(),
            level: 0,
            score: 0.0,
            adjusted: 0.0,
            drop_prefixes: vec![],
        };
        let mut compact_def = CompactDef::new(
            0,
            agate.core.lvctl.core.levels[this_level_id].clone(),
            this_level_id,
            agate.core.lvctl.core.levels[next_level_id].clone(),
            next_level_id,
            cpt_prio,
            targets,
        );
        compact_def.top = agate.core.lvctl.core.levels[this_level_id]
            .read()
            .unwrap()
            .tables
            .clone();
        compact_def.bot = agate.core.lvctl.core.levels[next_level_id]
            .read()
            .unwrap()
            .tables
            .clone();
        compact_def.targets.base_level = next_level_id;
        compact_def
    }

    #[test]
    fn test_l0_to_l1() {
        with_agate_test(|agate| {
            let l0 = vec![
                KeyValVersion::new("foo", "bar", 3, 0),
                KeyValVersion::new("fooz", "barz", 1, 0),
            ];
            let l01 = vec![KeyValVersion::new("foo", "bar", 2, 0)];
            let l1 = vec![KeyValVersion::new("foo", "bar", 1, 0)];
            create_and_open(agate, l0, 0);
            create_and_open(agate, l01, 0);
            create_and_open(agate, l1, 1);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    KeyValVersion::new("foo", "bar", 3, 0),
                    KeyValVersion::new("foo", "bar", 2, 0),
                    KeyValVersion::new("foo", "bar", 1, 0),
                    KeyValVersion::new("fooz", "barz", 1, 0),
                ],
            );
            let mut compact_def = generate_test_compect_def(agate, 0, 1);
            agate
                .core
                .lvctl
                .core
                .run_compact_def(std::usize::MAX, 0, &mut compact_def)
                .unwrap();
            get_all_and_check(
                agate,
                vec![
                    KeyValVersion::new("foo", "bar", 3, 0),
                    KeyValVersion::new("fooz", "barz", 1, 0),
                ],
            );
        })
    }

    #[test]
    fn test_l0_to_l1_with_dup() {
        with_agate_test(|agate| {
            let l0 = vec![
                KeyValVersion::new("foo", "barNew", 3, 0),
                KeyValVersion::new("fooz", "baz", 1, 0),
            ];
            let l01 = vec![KeyValVersion::new("foo", "bar", 4, 0)];
            let l1 = vec![KeyValVersion::new("foo", "bar", 3, 0)];
            create_and_open(agate, l0, 0);
            create_and_open(agate, l01, 0);
            create_and_open(agate, l1, 1);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    KeyValVersion::new("foo", "bar", 4, 0),
                    KeyValVersion::new("foo", "barNew", 3, 0),
                    KeyValVersion::new("fooz", "baz", 1, 0),
                ],
            );
            let mut compact_def = generate_test_compect_def(agate, 0, 1);
            agate
                .core
                .lvctl
                .core
                .run_compact_def(std::usize::MAX, 0, &mut compact_def)
                .unwrap();
            get_all_and_check(
                agate,
                vec![
                    KeyValVersion::new("foo", "bar", 4, 0),
                    KeyValVersion::new("fooz", "baz", 1, 0),
                ],
            );
        })
    }

    #[test]
    fn test_l0_to_l1_with_lower_overlap() {
        with_agate_test(|agate| {
            let l0 = vec![
                KeyValVersion::new("foo", "bar", 3, 0),
                KeyValVersion::new("fooz", "baz", 1, 0),
            ];
            let l01 = vec![KeyValVersion::new("foo", "bar", 2, 0)];
            let l1 = vec![KeyValVersion::new("foo", "bar", 1, 0)];
            let l2 = vec![KeyValVersion::new("foo", "bar", 0, 0)];

            create_and_open(agate, l0, 0);
            create_and_open(agate, l01, 0);
            create_and_open(agate, l1, 1);
            create_and_open(agate, l2, 2);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    KeyValVersion::new("foo", "bar", 3, 0),
                    KeyValVersion::new("foo", "bar", 2, 0),
                    KeyValVersion::new("foo", "bar", 1, 0),
                    KeyValVersion::new("foo", "bar", 0, 0),
                    KeyValVersion::new("fooz", "baz", 1, 0),
                ],
            );
            let mut compact_def = generate_test_compect_def(agate, 0, 1);
            agate
                .core
                .lvctl
                .core
                .run_compact_def(std::usize::MAX, 0, &mut compact_def)
                .unwrap();

            get_all_and_check(
                agate,
                vec![
                    KeyValVersion::new("foo", "bar", 3, 0),
                    KeyValVersion::new("foo", "bar", 0, 0),
                    KeyValVersion::new("fooz", "baz", 1, 0),
                ],
            );
        })
    }

    #[test]
    fn test_l1_to_l2() {
        with_agate_test(|agate| {
            let l1 = vec![
                KeyValVersion::new("foo", "bar", 3, 0),
                KeyValVersion::new("fooz", "baz", 1, 0),
            ];
            let l2 = vec![KeyValVersion::new("foo", "bar", 2, 0)];

            create_and_open(agate, l1, 1);
            create_and_open(agate, l2, 2);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    KeyValVersion::new("foo", "bar", 3, 0),
                    KeyValVersion::new("foo", "bar", 2, 0),
                    KeyValVersion::new("fooz", "baz", 1, 0),
                ],
            );
            let mut compact_def = generate_test_compect_def(agate, 1, 2);
            agate
                .core
                .lvctl
                .core
                .run_compact_def(std::usize::MAX, 1, &mut compact_def)
                .unwrap();

            get_all_and_check(
                agate,
                vec![
                    KeyValVersion::new("foo", "bar", 3, 0),
                    KeyValVersion::new("fooz", "baz", 1, 0),
                ],
            );
        })
    }

    #[test]
    fn test_l1_to_l2_with_delete() {
        // TODO: this test should also be done when version_to_retain > 1
        with_agate_test(|agate| {
            let l1 = vec![
                KeyValVersion::new("foo", "bar", 3, VALUE_DELETE),
                KeyValVersion::new("fooz", "baz", 1, VALUE_DELETE),
            ];
            let l2 = vec![KeyValVersion::new("foo", "bar", 2, 0)];
            let l3 = vec![KeyValVersion::new("foo", "bar", 1, 0)];

            create_and_open(agate, l1, 1);
            create_and_open(agate, l2, 2);
            create_and_open(agate, l3, 3);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    KeyValVersion::new("foo", "bar", 3, VALUE_DELETE),
                    KeyValVersion::new("foo", "bar", 2, 0),
                    KeyValVersion::new("foo", "bar", 1, 0),
                    KeyValVersion::new("fooz", "baz", 1, VALUE_DELETE),
                ],
            );
            let mut compact_def = generate_test_compect_def(agate, 1, 2);
            agate
                .core
                .lvctl
                .core
                .run_compact_def(std::usize::MAX, 1, &mut compact_def)
                .unwrap();

            get_all_and_check(
                agate,
                vec![
                    KeyValVersion::new("foo", "bar", 3, VALUE_DELETE),
                    KeyValVersion::new("foo", "bar", 1, 0),
                    KeyValVersion::new("fooz", "baz", 1, VALUE_DELETE),
                ],
            );
        })
    }

    #[test]
    fn test_l1_to_l2_with_bottom_overlap() {
        with_agate_test(|agate| {
            let l1 = vec![KeyValVersion::new("foo", "bar", 3, VALUE_DELETE)];
            let l2 = vec![
                KeyValVersion::new("foo", "bar", 2, 0),
                KeyValVersion::new("fooz", "baz", 2, VALUE_DELETE),
            ];
            let l3 = vec![KeyValVersion::new("fooz", "baz", 1, 0)];

            create_and_open(agate, l1, 1);
            create_and_open(agate, l2, 2);
            create_and_open(agate, l3, 3);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    KeyValVersion::new("foo", "bar", 3, VALUE_DELETE),
                    KeyValVersion::new("foo", "bar", 2, 0),
                    KeyValVersion::new("fooz", "baz", 2, VALUE_DELETE),
                    KeyValVersion::new("fooz", "baz", 1, 0),
                ],
            );
            let mut compact_def = generate_test_compect_def(agate, 1, 2);
            agate
                .core
                .lvctl
                .core
                .run_compact_def(std::usize::MAX, 1, &mut compact_def)
                .unwrap();

            get_all_and_check(
                agate,
                vec![
                    KeyValVersion::new("foo", "bar", 3, VALUE_DELETE),
                    KeyValVersion::new("fooz", "baz", 2, VALUE_DELETE),
                    KeyValVersion::new("fooz", "baz", 1, 0),
                ],
            );
        })
    }

    #[test]
    fn test_l1_to_l2_without_overlap() {
        with_agate_test(|agate| {
            let l1 = vec![
                KeyValVersion::new("foo", "bar", 3, VALUE_DELETE),
                KeyValVersion::new("fooz", "baz", 1, VALUE_DELETE),
            ];
            let l2 = vec![KeyValVersion::new("fooo", "barr", 2, 0)];

            create_and_open(agate, l1, 1);
            create_and_open(agate, l2, 2);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    KeyValVersion::new("foo", "bar", 3, VALUE_DELETE),
                    KeyValVersion::new("fooo", "barr", 2, 0),
                    KeyValVersion::new("fooz", "baz", 1, VALUE_DELETE),
                ],
            );
            let mut compact_def = generate_test_compect_def(agate, 1, 2);
            agate
                .core
                .lvctl
                .core
                .run_compact_def(std::usize::MAX, 1, &mut compact_def)
                .unwrap();

            get_all_and_check(agate, vec![KeyValVersion::new("fooo", "barr", 2, 0)]);
        })
    }
}
