use super::*;
use crate::{db::tests::with_agate_test, table::new_filename, Agate, TableOptions};

pub fn helper_dump_levels(lvctl: &LevelsController) {
    for level in &lvctl.core.levels {
        let level = level.read().unwrap();
        eprintln!("--- Level {} ---", level.level);
        for table in &level.tables {
            eprintln!(
                "#{} ({:?} - {:?}, {})",
                table.id(),
                table.smallest(),
                table.biggest(),
                table.size()
            );
        }
    }
}

macro_rules! kv {
    ($key: expr, $val: expr, $meta: expr, $user_meta: expr) => {
        KeyValVersion::new($key, $val, $meta, $user_meta)
    };
}

macro_rules! run_compact {
    ($agate: expr, $level: expr, $compact_def: expr) => {
        $agate
            .core
            .lvctl
            .core
            .run_compact_def(std::usize::MAX, $level, &mut $compact_def)
            .unwrap();
    };
}

#[derive(Debug)]
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
        builder.add(&key, &value, 0);
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
            let l0 = kv!("foo", "bar", 3, 0);
            let l1 = kv!("foo", "bar", 2, 0);
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
                kv!("a", "x", 1, 0),
                kv!("b", "x", 1, 0),
                kv!("foo", "bar", 3, 0),
            ];
            let l1 = vec![kv!("foo", "bar", 2, 0)];
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
                kv!("a", "x", 1, 0),
                kv!("b", "x", 1, 0),
                kv!("c", "bar", 3, 0),
            ];
            let l1 = vec![kv!("foo", "bar", 2, 0)];
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
            while iter.valid() {
                let it = iter.item();
                eprintln!("key={:?} val={:?}", it.key, it.vptr);
                iter.next();
            }
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

mod compaction {
    use super::*;
    use crate::{
        db::tests::{generate_test_agate_options, with_agate_test_options},
        value::*,
    };

    fn test_options() -> AgateOptions {
        let mut options = generate_test_agate_options();
        // disable compaction
        options.num_compactors = 0;
        options.num_versions_to_keep = 1;

        options
    }

    #[test]
    fn test_l0_to_l1() {
        with_agate_test_options(test_options(), |agate| {
            let l0 = vec![kv!("foo", "bar", 3, 0), kv!("fooz", "barz", 1, 0)];
            let l01 = vec![kv!("foo", "bar", 2, 0)];
            let l1 = vec![kv!("foo", "bar", 1, 0)];
            create_and_open(agate, l0, 0);
            create_and_open(agate, l01, 0);
            create_and_open(agate, l1, 1);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "barz", 1, 0),
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
                vec![kv!("foo", "bar", 3, 0), kv!("fooz", "barz", 1, 0)],
            );
        })
    }

    #[test]
    fn test_l0_to_l1_with_dup() {
        with_agate_test_options(test_options(), |agate| {
            let l0 = vec![kv!("foo", "barNew", 3, 0), kv!("fooz", "baz", 1, 0)];
            let l01 = vec![kv!("foo", "bar", 4, 0)];
            let l1 = vec![kv!("foo", "bar", 3, 0)];
            create_and_open(agate, l0, 0);
            create_and_open(agate, l01, 0);
            create_and_open(agate, l1, 1);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 4, 0),
                    kv!("foo", "barNew", 3, 0),
                    kv!("fooz", "baz", 1, 0),
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
                vec![kv!("foo", "bar", 4, 0), kv!("fooz", "baz", 1, 0)],
            );
        })
    }

    #[test]
    fn test_l0_to_l1_with_lower_overlap() {
        with_agate_test_options(test_options(), |agate| {
            let l0 = vec![kv!("foo", "bar", 3, 0), kv!("fooz", "baz", 1, 0)];
            let l01 = vec![kv!("foo", "bar", 2, 0)];
            let l1 = vec![kv!("foo", "bar", 1, 0)];
            let l2 = vec![kv!("foo", "bar", 0, 0)];

            create_and_open(agate, l0, 0);
            create_and_open(agate, l01, 0);
            create_and_open(agate, l1, 1);
            create_and_open(agate, l2, 2);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("foo", "bar", 0, 0),
                    kv!("fooz", "baz", 1, 0),
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
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 0, 0),
                    kv!("fooz", "baz", 1, 0),
                ],
            );
        })
    }

    #[test]
    fn test_l1_to_l2() {
        with_agate_test_options(test_options(), |agate| {
            let l1 = vec![kv!("foo", "bar", 3, 0), kv!("fooz", "baz", 1, 0)];
            let l2 = vec![kv!("foo", "bar", 2, 0)];

            create_and_open(agate, l1, 1);
            create_and_open(agate, l2, 2);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("fooz", "baz", 1, 0),
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
                vec![kv!("foo", "bar", 3, 0), kv!("fooz", "baz", 1, 0)],
            );
        })
    }

    #[test]
    fn test_l1_to_l2_with_delete() {
        // TODO: this test should also be done when version_to_retain > 1
        with_agate_test_options(test_options(), |agate| {
            let l1 = vec![
                kv!("foo", "bar", 3, VALUE_DELETE),
                kv!("fooz", "baz", 1, VALUE_DELETE),
            ];
            let l2 = vec![kv!("foo", "bar", 2, 0)];
            let l3 = vec![kv!("foo", "bar", 1, 0)];

            create_and_open(agate, l1, 1);
            create_and_open(agate, l2, 2);
            create_and_open(agate, l3, 3);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 3, VALUE_DELETE),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "baz", 1, VALUE_DELETE),
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
                    kv!("foo", "bar", 3, VALUE_DELETE),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "baz", 1, VALUE_DELETE),
                ],
            );
        })
    }

    #[test]
    fn test_l1_to_l2_with_bottom_overlap() {
        with_agate_test_options(test_options(), |agate| {
            let l1 = vec![kv!("foo", "bar", 3, VALUE_DELETE)];
            let l2 = vec![kv!("foo", "bar", 2, 0), kv!("fooz", "baz", 2, VALUE_DELETE)];
            let l3 = vec![kv!("fooz", "baz", 1, 0)];

            create_and_open(agate, l1, 1);
            create_and_open(agate, l2, 2);
            create_and_open(agate, l3, 3);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 3, VALUE_DELETE),
                    kv!("foo", "bar", 2, 0),
                    kv!("fooz", "baz", 2, VALUE_DELETE),
                    kv!("fooz", "baz", 1, 0),
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
                    kv!("foo", "bar", 3, VALUE_DELETE),
                    kv!("fooz", "baz", 2, VALUE_DELETE),
                    kv!("fooz", "baz", 1, 0),
                ],
            );
        })
    }

    #[test]
    fn test_l1_to_l2_without_overlap() {
        with_agate_test_options(test_options(), |agate| {
            let l1 = vec![
                kv!("foo", "bar", 3, VALUE_DELETE),
                kv!("fooz", "baz", 1, VALUE_DELETE),
            ];
            let l2 = vec![kv!("fooo", "barr", 2, 0)];

            create_and_open(agate, l1, 1);
            create_and_open(agate, l2, 2);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 3, VALUE_DELETE),
                    kv!("fooo", "barr", 2, 0),
                    kv!("fooz", "baz", 1, VALUE_DELETE),
                ],
            );
            let mut compact_def = generate_test_compect_def(agate, 1, 2);
            agate
                .core
                .lvctl
                .core
                .run_compact_def(std::usize::MAX, 1, &mut compact_def)
                .unwrap();

            get_all_and_check(agate, vec![kv!("fooo", "barr", 2, 0)]);
        })
    }

    #[test]
    fn test_l1_to_l2_with_splits() {
        with_agate_test_options(test_options(), |agate| {
            let l1 = vec![kv!("C", "bar", 3, VALUE_DELETE)];
            let l21 = vec![kv!("A", "bar", 2, 0)];
            let l22 = vec![kv!("B", "bar", 2, 0)];
            let l23 = vec![kv!("C", "bar", 2, 0)];
            let l24 = vec![kv!("D", "bar", 2, 0)];
            let l3 = vec![kv!("fooz", "baz", 1, 0)];

            create_and_open(agate, l1, 1);
            create_and_open(agate, l21, 2);
            create_and_open(agate, l22, 2);
            create_and_open(agate, l23, 2);
            create_and_open(agate, l24, 2);
            create_and_open(agate, l3, 3);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    kv!("A", "bar", 2, 0),
                    kv!("B", "bar", 2, 0),
                    kv!("C", "bar", 3, VALUE_DELETE),
                    kv!("C", "bar", 2, 0),
                    kv!("D", "bar", 2, 0),
                    kv!("fooz", "baz", 1, 0),
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
                    kv!("A", "bar", 2, 0),
                    kv!("B", "bar", 2, 0),
                    kv!("D", "bar", 2, 0),
                    kv!("fooz", "baz", 1, 0),
                ],
            );
        })
    }
}

mod compaction_two_versions {
    use super::*;
    use crate::db::tests::{generate_test_agate_options, with_agate_test_options};
    use crate::value::*;
    use crate::AgateOptions;

    fn test_options() -> AgateOptions {
        let mut options = generate_test_agate_options();
        // disable compaction
        options.num_compactors = 0;
        options.num_versions_to_keep = 2;

        options
    }

    #[test]
    fn test_with_overlap() {
        with_agate_test_options(test_options(), |agate| {
            let l1 = vec![kv!("foo", "bar", 3, 0), kv!("fooz", "baz", 1, VALUE_DELETE)];
            let l2 = vec![kv!("foo", "bar", 2, 0)];
            let l3 = vec![kv!("foo", "bar", 1, 0)];

            create_and_open(agate, l1, 1);
            create_and_open(agate, l2, 2);
            create_and_open(agate, l3, 3);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "baz", 1, 1),
                ],
            );

            let mut compact_def = generate_test_compect_def(agate, 1, 2);
            run_compact!(agate, 1, compact_def);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "baz", 1, 1),
                ],
            );

            let mut compact_def = generate_test_compect_def(agate, 2, 3);
            run_compact!(agate, 2, compact_def);

            get_all_and_check(
                agate,
                vec![kv!("foo", "bar", 3, 0), kv!("foo", "bar", 2, 0)],
            );
        })
    }
}

mod compaction_all_versions {
    use super::*;
    use crate::db::tests::{generate_test_agate_options, with_agate_test_options};
    use crate::value::*;
    use crate::AgateOptions;

    fn test_options() -> AgateOptions {
        let mut options = generate_test_agate_options();
        // disable compaction
        options.num_compactors = 0;
        options.num_versions_to_keep = std::usize::MAX;

        options
    }

    #[test]
    fn test_with_overlap() {
        with_agate_test_options(test_options(), |agate| {
            let l1 = vec![kv!("foo", "bar", 3, 0), kv!("fooz", "baz", 1, VALUE_DELETE)];
            let l2 = vec![kv!("foo", "bar", 2, 0)];
            let l3 = vec![kv!("foo", "bar", 1, 0)];

            create_and_open(agate, l1, 1);
            create_and_open(agate, l2, 2);
            create_and_open(agate, l3, 3);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "baz", 1, 1),
                ],
            );

            let mut compact_def = generate_test_compect_def(agate, 1, 2);
            run_compact!(agate, 1, compact_def);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "baz", 1, 1),
                ],
            );

            let mut compact_def = generate_test_compect_def(agate, 2, 3);
            run_compact!(agate, 2, compact_def);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                ],
            );
        })
    }

    #[test]
    fn test_without_overlap() {
        with_agate_test_options(test_options(), |agate| {
            let l1 = vec![
                kv!("foo", "bar", 3, VALUE_DELETE),
                kv!("fooz", "baz", 1, VALUE_DELETE),
            ];
            let l2 = vec![kv!("fooo", "barr", 2, 0)];

            create_and_open(agate, l1, 1);
            create_and_open(agate, l2, 2);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 3, VALUE_DELETE),
                    kv!("fooo", "barr", 2, 0),
                    kv!("fooz", "baz", 1, VALUE_DELETE),
                ],
            );

            let mut compact_def = generate_test_compect_def(agate, 1, 2);
            run_compact!(agate, 1, compact_def);

            get_all_and_check(agate, vec![kv!("fooo", "barr", 2, 0)]);
        })
    }
}

mod discard_ts {
    use super::*;
    use crate::db::tests::{generate_test_agate_options, with_agate_test_options};
    use crate::AgateOptions;

    fn test_options() -> AgateOptions {
        let mut options = generate_test_agate_options();
        // disable compaction
        options.num_compactors = 0;
        options.num_versions_to_keep = 1;

        options
    }

    #[test]
    fn test_all_keys_above_discard() {
        with_agate_test_options(test_options(), |agate| {
            let l0 = vec![kv!("foo", "bar", 4, 0), kv!("fooz", "baz", 3, 0)];
            let l01 = vec![kv!("foo", "bar", 3, 0)];
            let l1 = vec![kv!("foo", "bar", 2, 0)];

            create_and_open(agate, l0, 0);
            create_and_open(agate, l01, 0);
            create_and_open(agate, l1, 1);

            agate.core.orc.set_discard_ts(1);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 4, 0),
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("fooz", "baz", 3, 0),
                ],
            );

            let mut compact_def = generate_test_compect_def(agate, 0, 1);
            run_compact!(agate, 0, compact_def);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 4, 0),
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("fooz", "baz", 3, 0),
                ],
            );
        })
    }

    #[test]
    fn test_some_keys_above_discard() {
        with_agate_test_options(test_options(), |agate| {
            let l0 = vec![
                kv!("foo", "bar", 4, 0),
                kv!("foo", "bar", 3, 0),
                kv!("foo", "bar", 2, 0),
                kv!("fooz", "baz", 2, 0),
            ];

            let l1 = vec![kv!("foo", "bbb", 1, 0)];

            create_and_open(agate, l0, 0);
            create_and_open(agate, l1, 1);

            agate.core.orc.set_discard_ts(3);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 4, 0),
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bbb", 1, 0),
                    kv!("fooz", "baz", 2, 0),
                ],
            );

            let mut compact_def = generate_test_compect_def(agate, 0, 1);
            run_compact!(agate, 0, compact_def);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 4, 0),
                    kv!("foo", "bar", 3, 0),
                    kv!("fooz", "baz", 2, 0),
                ],
            );
        })
    }

    #[test]
    fn test_all_keys_below_discard() {
        with_agate_test_options(test_options(), |agate| {
            let l0 = vec![kv!("foo", "bar", 4, 0), kv!("fooz", "baz", 3, 0)];
            let l01 = vec![kv!("foo", "bar", 3, 0)];
            let l1 = vec![kv!("foo", "bar", 2, 0)];

            create_and_open(agate, l0, 0);
            create_and_open(agate, l01, 0);
            create_and_open(agate, l1, 1);

            agate.core.orc.set_discard_ts(10);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 4, 0),
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("fooz", "baz", 3, 0),
                ],
            );

            let mut compact_def = generate_test_compect_def(agate, 0, 1);
            run_compact!(agate, 0, compact_def);

            get_all_and_check(
                agate,
                vec![kv!("foo", "bar", 4, 0), kv!("fooz", "baz", 3, 0)],
            );
        })
    }
}

mod miscellaneous {
    use super::*;
    use crate::db::tests::{generate_test_agate_options, with_agate_test_options};
    use crate::value::*;
    use bytes::BytesMut;

    #[test]
    fn test_discard_first_version() {
        let mut options = generate_test_agate_options();
        // disable compaction
        options.num_compactors = 0;
        options.num_versions_to_keep = std::usize::MAX;

        with_agate_test_options(options, |agate| {
            let l0 = vec![kv!("foo", "bar", 1, 0)];
            let l01 = vec![kv!("foo", "bar", 2, VALUE_DISCARD_EARLIER_VERSIONS)];
            let l02 = vec![kv!("foo", "bar", 3, 0)];
            let l03 = vec![kv!("foo", "bar", 4, 0)];
            let l04 = vec![kv!("foo", "bar", 9, 0)];
            let l05 = vec![kv!("foo", "bar", 10, VALUE_DISCARD_EARLIER_VERSIONS)];

            create_and_open(agate, l0, 0);
            create_and_open(agate, l01, 0);
            create_and_open(agate, l02, 0);
            create_and_open(agate, l03, 0);
            create_and_open(agate, l04, 0);
            create_and_open(agate, l05, 0);

            agate.core.orc.set_discard_ts(7);

            let mut compact_def = generate_test_compect_def(agate, 0, 1);
            run_compact!(agate, 0, compact_def);

            get_all_and_check(
                agate,
                vec![
                    kv!("foo", "bar", 10, VALUE_DISCARD_EARLIER_VERSIONS),
                    kv!("foo", "bar", 9, 0),
                    kv!("foo", "bar", 4, 0),
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, VALUE_DISCARD_EARLIER_VERSIONS),
                ],
            );
        })
    }

    struct TestCase {
        name: &'static str,
        levels: Vec<Vec<KeyValVersion>>,
        result: Vec<KeyValVersion>,
    }

    #[test]
    fn test_level_get() {
        let cases = vec![
            TestCase {
                name: "normal",
                levels: vec![
                    vec![kv!("foo", "bar10", 10, 0), kv!("foo", "barSeven", 7, 0)],
                    vec![kv!("foo", "bar", 1, 0)],
                ],
                result: vec![
                    kv!("foo", "bar", 1, 0),
                    kv!("foo", "barSeven", 7, 0),
                    kv!("foo", "bar10", 10, 0),
                    kv!("foo", "bar10", 11, 0), // ver 11 doesn't exist so we should get bar10.
                    kv!("foo", "barSeven", 9, 0), // ver 9 doesn't exist so we should get barSeven.
                    kv!("foo", "bar10", 100000, 0), // ver doesn't exist so we should get bar10.
                ],
            },
            TestCase {
                name: "after gc",
                levels: vec![
                    vec![
                        kv!("foo", "barNew", 1, 0), // foo1 is above foo10 because of the GC.
                        kv!("foo", "bar10", 10, 0),
                        kv!("foo", "barSeven", 7, 0),
                    ],
                    vec![kv!("foo", "bar", 1, 0)],
                ],
                result: vec![
                    kv!("foo", "barNew", 1, 0),
                    kv!("foo", "barSeven", 7, 0),
                    kv!("foo", "bar10", 10, 0),
                    kv!("foo", "bar10", 11, 0), // Should return biggest version.
                ],
            },
            TestCase {
                name: "after two gc",
                levels: vec![
                    vec![
                        // Level 0 has 4 tables and each table has single key.
                        kv!("foo", "barL0", 1, 0), // foo1 is above foo10 because of the GC.
                        kv!("foo", "bar10", 10, 0),
                        kv!("foo", "barSeven", 7, 0),
                    ],
                    vec![
                        // Level 1 has 1 table with a single key.
                        // Level 1 also has a foo because it was moved twice during GC.
                        kv!("foo", "barL1", 1, 0),
                    ],
                    vec![
                        // Level 1 has 1 table with a single key.
                        kv!("foo", "bar", 1, 0),
                    ],
                ],
                result: vec![
                    kv!("foo", "barL0", 1, 0),
                    kv!("foo", "barSeven", 7, 0),
                    kv!("foo", "bar10", 10, 0),
                    kv!("foo", "bar10", 11, 0), // Should return biggest version.
                ],
            },
        ];

        for TestCase {
            name,
            levels,
            result,
        } in cases
        {
            eprintln!("running {}", name);
            with_agate_test(move |agate| {
                for (level, data) in levels.into_iter().enumerate() {
                    for val in data {
                        create_and_open(agate, vec![val], level);
                    }
                }
                for item in result {
                    let key = key_with_ts(BytesMut::from(&item.key[..]), item.version as u64);
                    let vs = agate.get(&key).unwrap();
                    eprintln!("item={:?}", item);
                    assert_eq!(item.value, vs.value);
                }
            })
        }
    }
}
