use std::fs;

use lazy_static::lazy_static;
use tempdir::TempDir;
use yatp::{task::callback::TaskCell, ThreadPool};

use super::*;
use crate::{
    db::tests::{generate_test_agate_options, helper_dump_dir},
    table::new_filename,
};

pub fn dump_levels(lvctl: &LevelsController) {
    for level in &lvctl.core.levels {
        let level = level.read().unwrap();
        println!("--- Level {} ---", level.level);
        for table in &level.tables {
            println!(
                "Table{} ({:?} - {:?}, {})",
                table.id(),
                table.smallest(),
                table.biggest(),
                table.size()
            );
        }
    }
}

pub fn lvctl_test_with_opts<F>(mut opts: AgateOptions, f: F)
where
    F: FnOnce(&mut LevelsController),
    F: Send + 'static,
{
    // So as to set and get discard timestamp by self.
    opts.managed_txns = true;

    let (tx, rx) = std::sync::mpsc::channel();

    let handle = std::thread::spawn(move || {
        let tmp_dir = TempDir::new("agatedb").unwrap().as_ref().to_path_buf();
        // TODO: why not exist?
        if !tmp_dir.exists() {
            fs::create_dir_all(tmp_dir.as_path()).unwrap();
        };
        helper_dump_dir(tmp_dir.as_path());
        opts.dir = tmp_dir.clone();
        opts.value_dir = tmp_dir.clone();

        let manifest = Arc::new(ManifestFile::open_or_create_manifest_file(&opts).unwrap());
        let orc = Arc::new(Oracle::new(&opts));

        let mut lvctl = LevelsController::new(&opts, manifest, orc).unwrap();

        f(&mut lvctl);

        println!("--- Agate directory ---");
        helper_dump_dir(tmp_dir.as_path());
        dump_levels(&lvctl);
        drop(lvctl);
        println!("--- after close ---");
        helper_dump_dir(tmp_dir.as_path());
        tx.send(()).expect("failed to complete test");
    });

    match rx.recv_timeout(std::time::Duration::from_secs(60)) {
        Ok(_) => handle.join().expect("thread panic"),
        Err(err) => panic!("error: {:?}", err),
    }
}

pub fn lvctl_test<F>(f: F)
where
    F: FnOnce(&mut LevelsController),
    F: Send + 'static,
{
    lvctl_test_with_opts(generate_test_agate_options(), f);
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

macro_rules! kv {
    ($key: expr, $val: expr, $version: expr, $meta: expr) => {
        KeyValVersion::new($key, $val, $version, $meta)
    };
}

fn create_and_open(lvctl: &mut LevelsController, td: Vec<KeyValVersion>, level: usize) {
    let table_opts = build_table_options(&lvctl.core.opts);
    let mut builder = TableBuilder::new(table_opts.clone());

    for item in td {
        let key = key_with_ts(&item.key[..], item.version);
        let value = Value::new_with_meta(item.value.clone(), item.meta, 0);
        builder.add(&key, &value, 0);
    }
    let filename = new_filename(lvctl.reserve_file_id(), &lvctl.core.opts.dir);
    let table = Table::create(&filename, builder.finish(), table_opts).unwrap();
    lvctl
        .core
        .manifest
        .add_changes(vec![new_create_change(table.id(), level, 0)])
        .unwrap();
    let mut lv = lvctl.core.levels[level].write().unwrap();
    lv.tables.push(table);
}

macro_rules! assert_bytes_eq {
    ($left:expr, $right:expr) => {
        assert_eq!(
            Bytes::copy_from_slice($left),
            Bytes::copy_from_slice($right)
        )
    };
}

fn get_all_and_check(lvctl: &mut LevelsController, expected: Vec<KeyValVersion>) {
    let mut iters = vec![];
    let iter_opts = IteratorOptions::default();

    lvctl.append_iterators(&mut iters, &iter_opts);

    let mut merge_iter = MergeIterator::from_iterators(iters.into_iter().collect(), false);

    merge_iter.rewind();
    while merge_iter.valid() {
        eprintln!("key={:?} val={:?}", merge_iter.key(), merge_iter.value());
        merge_iter.next();
    }

    let mut cnt = 0;

    merge_iter.rewind();
    while merge_iter.valid() {
        assert!(cnt < expected.len());
        assert_bytes_eq!(user_key(merge_iter.key()), &expected[cnt].key);
        assert_bytes_eq!(&merge_iter.value().value, &expected[cnt].value);
        assert_eq!(get_ts(merge_iter.key()), expected[cnt].version);
        assert_eq!(merge_iter.value().meta, expected[cnt].meta);
        merge_iter.next();
        cnt += 1;
    }
    assert_eq!(cnt, expected.len());
}

fn generate_test_compect_def(
    lvctl: &LevelsController,
    this_level_id: usize,
    next_level_id: usize,
) -> CompactDef {
    let targets = lvctl.core.level_targets();
    let cpt_prio = CompactionPriority {
        targets: targets.clone(),
        level: 0,
        score: 0.0,
        adjusted: 0.0,
        drop_prefixes: vec![],
    };
    let mut compact_def = CompactDef::new(
        0,
        lvctl.core.levels[this_level_id].clone(),
        this_level_id,
        lvctl.core.levels[next_level_id].clone(),
        next_level_id,
        cpt_prio,
        targets,
    );
    compact_def.top = lvctl.core.levels[this_level_id]
        .read()
        .unwrap()
        .tables
        .clone();
    compact_def.bot = lvctl.core.levels[next_level_id]
        .read()
        .unwrap()
        .tables
        .clone();
    compact_def.targets.base_level = next_level_id;
    compact_def
}

lazy_static! {
    pub static ref POOL: Arc<ThreadPool<TaskCell>> = Arc::new(
        yatp::Builder::new("agatedb")
            .max_thread_count(2)
            .min_thread_count(2)
            .build_callback_pool(),
    );
}

#[test]
fn test_lvctl_test() {
    lvctl_test(|_| {});
}

mod overlap {
    use super::*;

    #[test]
    fn test_same_keys() {
        lvctl_test(|lvctl| {
            let l0 = kv!("foo", "bar", 3, 0);
            let l1 = kv!("foo", "bar", 2, 0);
            create_and_open(lvctl, vec![l0], 0);
            create_and_open(lvctl, vec![l1], 1);

            let l0_tables = lvctl.core.levels[0].read().unwrap().tables.clone();
            let l1_tables = lvctl.core.levels[1].read().unwrap().tables.clone();

            // Level 0 should overlap with level 0 tables.
            assert!(lvctl.core.check_overlap(&l0_tables, 0));
            // Level 1 should overlap with level 0 tables (they have the same keys).
            assert!(lvctl.core.check_overlap(&l0_tables, 1));
            // Level 2 and 3 should not overlap with level 0 tables.
            assert!(!lvctl.core.check_overlap(&l0_tables, 2));
            assert!(!lvctl.core.check_overlap(&l1_tables, 2));
            assert!(!lvctl.core.check_overlap(&l0_tables, 3));
            assert!(!lvctl.core.check_overlap(&l1_tables, 3));
        });
    }

    #[test]
    fn test_overlapping_keys() {
        lvctl_test(|lvctl| {
            let l0 = vec![
                kv!("a", "x", 1, 0),
                kv!("b", "x", 1, 0),
                kv!("foo", "bar", 3, 0),
            ];
            let l1 = vec![kv!("foo", "bar", 2, 0)];
            create_and_open(lvctl, l0, 0);
            create_and_open(lvctl, l1, 1);

            let l0_tables = lvctl.core.levels[0].read().unwrap().tables.clone();
            let l1_tables = lvctl.core.levels[1].read().unwrap().tables.clone();

            // Level 0 should overlap with level 0 tables.
            assert!(lvctl.core.check_overlap(&l0_tables, 0));
            assert!(lvctl.core.check_overlap(&l1_tables, 1));
            // Level 1 should overlap with level 0 tables, "foo" key is common.
            assert!(lvctl.core.check_overlap(&l0_tables, 1));
            // Level 2 and 3 should not overlap with level 0 tables.
            assert!(!lvctl.core.check_overlap(&l0_tables, 2));
            assert!(!lvctl.core.check_overlap(&l0_tables, 3));
        });
    }

    #[test]
    fn test_non_overlapping_keys() {
        lvctl_test(|lvctl| {
            let l0 = vec![
                kv!("a", "x", 1, 0),
                kv!("b", "x", 1, 0),
                kv!("c", "bar", 3, 0),
            ];
            let l1 = vec![kv!("foo", "bar", 2, 0)];
            create_and_open(lvctl, l0, 0);
            create_and_open(lvctl, l1, 1);

            let l0_tables = lvctl.core.levels[0].read().unwrap().tables.clone();

            // Level 1 should not overlap with level 0 tables.
            assert!(!lvctl.core.check_overlap(&l0_tables, 1));
            // Level 2 and 3 should not overlap with level 0 tables.
            assert!(!lvctl.core.check_overlap(&l0_tables, 2));
            assert!(!lvctl.core.check_overlap(&l0_tables, 3));
        });
    }
}

mod compaction {
    use super::*;
    use crate::value::VALUE_DELETE;

    #[test]
    fn test_l0_to_l1() {
        lvctl_test(|lvctl| {
            let l0 = vec![kv!("foo", "bar", 3, 0), kv!("fooz", "barz", 1, 0)];
            let l01 = vec![kv!("foo", "bar", 2, 0)];
            let l1 = vec![kv!("foo", "bar", 1, 0)];
            // Level 0 has table l0 and l01.
            create_and_open(lvctl, l0, 0);
            create_and_open(lvctl, l01, 0);
            // Level 1 has table l1.
            create_and_open(lvctl, l1, 1);

            // Set a high discard timestamp so that all the keys are below the discard timestamp.
            lvctl.core.orc.set_discard_ts(10);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "barz", 1, 0),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 0, 1);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 0, &mut compact_def, POOL.clone())
                .unwrap();

            // foo version 2 should be dropped after compaction.
            get_all_and_check(
                lvctl,
                vec![kv!("foo", "bar", 3, 0), kv!("fooz", "barz", 1, 0)],
            );
        });
    }

    #[test]
    fn test_l0_to_l1_with_dup() {
        lvctl_test(|lvctl| {
            // We have foo version 3 on Level 0.
            let l0 = vec![kv!("foo", "barNew", 3, 0), kv!("fooz", "baz", 1, 0)];
            let l01 = vec![kv!("foo", "bar", 4, 0)];
            let l1 = vec![kv!("foo", "bar", 3, 0)];
            // Level 0 has table l0 and l01.
            create_and_open(lvctl, l0, 0);
            create_and_open(lvctl, l01, 0);
            // Level 1 has table l1.
            create_and_open(lvctl, l1, 1);

            // Set a high discard timestamp so that all the keys are below the discard timestamp.
            lvctl.core.orc.set_discard_ts(10);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 4, 0),
                    kv!("foo", "barNew", 3, 0),
                    kv!("fooz", "baz", 1, 0),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 0, 1);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 0, &mut compact_def, POOL.clone())
                .unwrap();

            // foo version 3 (both) should be dropped after compaction.
            get_all_and_check(
                lvctl,
                vec![kv!("foo", "bar", 4, 0), kv!("fooz", "baz", 1, 0)],
            );
        });
    }

    #[test]
    fn test_l0_to_l1_with_lower_overlap() {
        lvctl_test(|lvctl| {
            let l0 = vec![kv!("foo", "bar", 3, 0), kv!("fooz", "baz", 1, 0)];
            let l01 = vec![kv!("foo", "bar", 2, 0)];
            let l1 = vec![kv!("foo", "bar", 1, 0)];
            let l2 = vec![kv!("foo", "bar", 0, 0)];
            // Level 0 has table l0 and l01.
            create_and_open(lvctl, l0, 0);
            create_and_open(lvctl, l01, 0);
            // Level 1 has table l1.
            create_and_open(lvctl, l1, 1);
            // Level 2 has table l2.
            create_and_open(lvctl, l2, 2);

            // Set a high discard timestamp so that all the keys are below the discard timestamp.
            lvctl.core.orc.set_discard_ts(10);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("foo", "bar", 0, 0),
                    kv!("fooz", "baz", 1, 0),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 0, 1);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 0, &mut compact_def, POOL.clone())
                .unwrap();

            // foo version 2 and version 1 should be dropped after compaction.
            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 0, 0),
                    kv!("fooz", "baz", 1, 0),
                ],
            );
        });
    }

    #[test]
    fn test_l1_to_l2() {
        lvctl_test(|lvctl| {
            let l1 = vec![kv!("foo", "bar", 3, 0), kv!("fooz", "baz", 1, 0)];
            let l2 = vec![kv!("foo", "bar", 2, 0)];
            create_and_open(lvctl, l1, 1);
            create_and_open(lvctl, l2, 2);

            // Set a high discard timestamp so that all the keys are below the discard timestamp.
            lvctl.core.orc.set_discard_ts(10);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("fooz", "baz", 1, 0),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 1, 2);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 1, &mut compact_def, POOL.clone())
                .unwrap();

            // foo version 2 should be dropped after compaction.
            get_all_and_check(
                lvctl,
                vec![kv!("foo", "bar", 3, 0), kv!("fooz", "baz", 1, 0)],
            );
        });
    }

    #[test]
    fn test_l1_to_l2_delete_with_overlap() {
        lvctl_test(|lvctl| {
            let l1 = vec![
                kv!("foo", "bar", 3, VALUE_DELETE),
                kv!("fooz", "baz", 1, VALUE_DELETE),
            ];
            let l2 = vec![kv!("foo", "bar", 2, 0)];
            let l3 = vec![kv!("foo", "bar", 1, 0)];
            create_and_open(lvctl, l1, 1);
            create_and_open(lvctl, l2, 2);
            create_and_open(lvctl, l3, 3);

            // Set a high discard timestamp so that all the keys are below the discard timestamp.
            lvctl.core.orc.set_discard_ts(10);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, VALUE_DELETE),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "baz", 1, VALUE_DELETE),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 1, 2);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 1, &mut compact_def, POOL.clone())
                .unwrap();

            // foo version 2 should be dropped after compaction. fooz version 1 will remain because overlap
            // exists, which is expected because `has_overlap` is only checked once at the beginning of `compact_build_tables` method. Everything from level 1 is now in level 2.
            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, VALUE_DELETE),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "baz", 1, VALUE_DELETE),
                ],
            );

            compact_def = generate_test_compect_def(lvctl, 2, 3);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 2, &mut compact_def, POOL.clone())
                .unwrap();

            // Everything should be removed now.
            // TODO: Fix MergeIterator::from_iterators to construct invalid iterator from empty iters.
            // get_all_and_check(lvctl, vec![]);
        });
    }

    #[test]
    fn test_l1_to_l2_delete_with_bottom_overlap() {
        lvctl_test(|lvctl| {
            let l1 = vec![kv!("foo", "bar", 3, VALUE_DELETE)];
            let l2 = vec![kv!("foo", "bar", 2, 0), kv!("fooz", "baz", 2, VALUE_DELETE)];
            let l3 = vec![kv!("fooz", "baz", 1, 0)];
            create_and_open(lvctl, l1, 1);
            create_and_open(lvctl, l2, 2);
            create_and_open(lvctl, l3, 3);

            // Set a high discard timestamp so that all the keys are below the discard timestamp.
            lvctl.core.orc.set_discard_ts(10);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, VALUE_DELETE),
                    kv!("foo", "bar", 2, 0),
                    kv!("fooz", "baz", 2, VALUE_DELETE),
                    kv!("fooz", "baz", 1, 0),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 1, 2);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 1, &mut compact_def, POOL.clone())
                .unwrap();

            // The top table at Level 1 doesn't overlap Level 3, but the bottom table at Level 2
            // does, delete keys should not be removed.
            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, VALUE_DELETE),
                    kv!("fooz", "baz", 2, VALUE_DELETE),
                    kv!("fooz", "baz", 1, 0),
                ],
            );
        });
    }

    #[test]
    fn test_l1_to_l2_delete_without_overlap() {
        lvctl_test(|lvctl| {
            let l1 = vec![
                kv!("foo", "bar", 3, VALUE_DELETE),
                kv!("fooz", "baz", 1, VALUE_DELETE),
            ];
            let l2 = vec![kv!("fooo", "barr", 2, 0)];
            create_and_open(lvctl, l1, 1);
            create_and_open(lvctl, l2, 2);

            // Set a high discard timestamp so that all the keys are below the discard timestamp.
            lvctl.core.orc.set_discard_ts(10);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, VALUE_DELETE),
                    kv!("fooo", "barr", 2, 0),
                    kv!("fooz", "baz", 1, VALUE_DELETE),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 1, 2);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 1, &mut compact_def, POOL.clone())
                .unwrap();

            // foo version 2 should be dropped after compaction.
            get_all_and_check(lvctl, vec![kv!("fooo", "barr", 2, 0)]);
        });
    }

    #[test]
    fn test_l1_to_l2_delete_with_splits() {
        lvctl_test(|lvctl| {
            let l1 = vec![kv!("C", "bar", 3, VALUE_DELETE)];
            let l21 = vec![kv!("A", "bar", 2, 0)];
            let l22 = vec![kv!("B", "bar", 2, 0)];
            let l23 = vec![kv!("C", "bar", 2, 0)];
            let l24 = vec![kv!("D", "bar", 2, 0)];
            let l3 = vec![kv!("fooz", "baz", 1, 0)];
            create_and_open(lvctl, l1, 1);
            create_and_open(lvctl, l21, 2);
            create_and_open(lvctl, l22, 2);
            create_and_open(lvctl, l23, 2);
            create_and_open(lvctl, l24, 2);
            create_and_open(lvctl, l3, 3);

            // Set a high discard timestamp so that all the keys are below the discard timestamp.
            lvctl.core.orc.set_discard_ts(10);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("A", "bar", 2, 0),
                    kv!("B", "bar", 2, 0),
                    kv!("C", "bar", 3, VALUE_DELETE),
                    kv!("C", "bar", 2, 0),
                    kv!("D", "bar", 2, 0),
                    kv!("fooz", "baz", 1, 0),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 1, 2);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 1, &mut compact_def, POOL.clone())
                .unwrap();

            // C should be dropped after compaction.
            get_all_and_check(
                lvctl,
                vec![
                    kv!("A", "bar", 2, 0),
                    kv!("B", "bar", 2, 0),
                    kv!("D", "bar", 2, 0),
                    kv!("fooz", "baz", 1, 0),
                ],
            );
        });
    }
}

mod compaction_two_versions {
    use super::*;
    use crate::value::VALUE_DELETE;

    fn test_options() -> AgateOptions {
        let mut options = generate_test_agate_options();
        // Disable compaction.
        options.num_compactors = 0;
        options.num_versions_to_keep = 2;

        options
    }

    #[test]
    fn test_with_overlap() {
        lvctl_test_with_opts(test_options(), |lvctl| {
            let l1 = vec![kv!("foo", "bar", 3, 0), kv!("fooz", "baz", 1, VALUE_DELETE)];
            let l2 = vec![kv!("foo", "bar", 2, 0)];
            let l3 = vec![kv!("foo", "bar", 1, 0)];
            create_and_open(lvctl, l1, 1);
            create_and_open(lvctl, l2, 2);
            create_and_open(lvctl, l3, 3);

            // Set a high discard timestamp so that all the keys are below the discard timestamp.
            lvctl.core.orc.set_discard_ts(10);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "baz", 1, 1),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 1, 2);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 1, &mut compact_def, POOL.clone())
                .unwrap();

            // Nothing should be dropped after compaction because number of versions to keep is 2.
            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "baz", 1, 1),
                ],
            );

            compact_def = generate_test_compect_def(lvctl, 2, 3);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 2, &mut compact_def, POOL.clone())
                .unwrap();

            get_all_and_check(
                lvctl,
                vec![kv!("foo", "bar", 3, 0), kv!("foo", "bar", 2, 0)],
            );
        });
    }
}

mod compaction_all_versions {
    use super::*;
    use crate::value::VALUE_DELETE;

    fn test_options() -> AgateOptions {
        let mut options = generate_test_agate_options();
        // Disable compaction.
        options.num_compactors = 0;
        options.num_versions_to_keep = std::usize::MAX;

        options
    }

    #[test]
    fn test_with_overlap() {
        lvctl_test_with_opts(test_options(), |lvctl| {
            let l1 = vec![kv!("foo", "bar", 3, 0), kv!("fooz", "baz", 1, VALUE_DELETE)];
            let l2 = vec![kv!("foo", "bar", 2, 0)];
            let l3 = vec![kv!("foo", "bar", 1, 0)];
            create_and_open(lvctl, l1, 1);
            create_and_open(lvctl, l2, 2);
            create_and_open(lvctl, l3, 3);

            lvctl.core.orc.set_discard_ts(10);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "baz", 1, 1),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 1, 2);
            lvctl
                .core
                .run_compact_def(std::usize::MAX, 1, &mut compact_def, POOL.clone())
                .unwrap();

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bar", 1, 0),
                    kv!("fooz", "baz", 1, 1),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 2, 3);
            lvctl
                .core
                .run_compact_def(std::usize::MAX, 2, &mut compact_def, POOL.clone())
                .unwrap();

            get_all_and_check(
                lvctl,
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
        lvctl_test_with_opts(test_options(), |lvctl| {
            let l1 = vec![
                kv!("foo", "bar", 3, VALUE_DELETE),
                kv!("fooz", "baz", 1, VALUE_DELETE),
            ];
            let l2 = vec![kv!("fooo", "barr", 2, 0)];
            create_and_open(lvctl, l1, 1);
            create_and_open(lvctl, l2, 2);

            lvctl.core.orc.set_discard_ts(10);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 3, VALUE_DELETE),
                    kv!("fooo", "barr", 2, 0),
                    kv!("fooz", "baz", 1, VALUE_DELETE),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 1, 2);
            lvctl
                .core
                .run_compact_def(std::usize::MAX, 1, &mut compact_def, POOL.clone())
                .unwrap();

            get_all_and_check(lvctl, vec![kv!("fooo", "barr", 2, 0)]);
        })
    }
}

mod discard_ts {
    use super::*;

    fn test_options() -> AgateOptions {
        let mut options = generate_test_agate_options();
        // Disable compaction.
        options.num_compactors = 0;
        options.num_versions_to_keep = 1;

        options
    }

    #[test]
    fn test_all_keys_above_discard() {
        lvctl_test_with_opts(test_options(), |lvctl| {
            let l0 = vec![kv!("foo", "bar", 4, 0), kv!("fooz", "baz", 3, 0)];
            let l01 = vec![kv!("foo", "bar", 3, 0)];
            let l1 = vec![kv!("foo", "bar", 2, 0)];
            create_and_open(lvctl, l0, 0);
            create_and_open(lvctl, l01, 0);
            create_and_open(lvctl, l1, 1);

            // Set discard_ts to 1. All the keys are above discard_ts.
            lvctl.core.orc.set_discard_ts(1);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 4, 0),
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("fooz", "baz", 3, 0),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 0, 1);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 0, &mut compact_def, POOL.clone())
                .unwrap();

            get_all_and_check(
                lvctl,
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
        lvctl_test_with_opts(test_options(), |lvctl| {
            let l0 = vec![
                kv!("foo", "bar", 4, 0),
                kv!("foo", "bar", 3, 0),
                kv!("foo", "bar", 2, 0),
                kv!("fooz", "baz", 2, 0),
            ];
            let l1 = vec![kv!("foo", "bbb", 1, 0)];
            create_and_open(lvctl, l0, 0);
            create_and_open(lvctl, l1, 1);

            // Set discard_ts to 3. foo2 and foo1 should be dropped.
            lvctl.core.orc.set_discard_ts(3);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 4, 0),
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("foo", "bbb", 1, 0),
                    kv!("fooz", "baz", 2, 0),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 0, 1);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 0, &mut compact_def, POOL.clone())
                .unwrap();

            get_all_and_check(
                lvctl,
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
        lvctl_test_with_opts(test_options(), |lvctl| {
            let l0 = vec![kv!("foo", "bar", 4, 0), kv!("fooz", "baz", 3, 0)];
            let l01 = vec![kv!("foo", "bar", 3, 0)];
            let l1 = vec![kv!("foo", "bar", 2, 0)];
            create_and_open(lvctl, l0, 0);
            create_and_open(lvctl, l01, 0);
            create_and_open(lvctl, l1, 1);

            // Set discard_ts to 10. All the keys are below discard_ts.
            lvctl.core.orc.set_discard_ts(10);

            get_all_and_check(
                lvctl,
                vec![
                    kv!("foo", "bar", 4, 0),
                    kv!("foo", "bar", 3, 0),
                    kv!("foo", "bar", 2, 0),
                    kv!("fooz", "baz", 3, 0),
                ],
            );

            let mut compact_def = generate_test_compect_def(lvctl, 0, 1);

            lvctl
                .core
                .run_compact_def(std::usize::MAX, 0, &mut compact_def, POOL.clone())
                .unwrap();

            get_all_and_check(
                lvctl,
                vec![kv!("foo", "bar", 4, 0), kv!("fooz", "baz", 3, 0)],
            );
        })
    }
}

mod miscellaneous {
    use super::*;
    use crate::value::VALUE_DISCARD_EARLIER_VERSIONS;

    #[test]
    fn test_discard_first_version() {
        let mut options = generate_test_agate_options();
        // Disable compaction.
        options.num_compactors = 0;
        options.num_versions_to_keep = std::usize::MAX;

        lvctl_test_with_opts(options, |lvctl| {
            let l0 = vec![kv!("foo", "bar", 1, 0)];
            let l01 = vec![kv!("foo", "bar", 2, VALUE_DISCARD_EARLIER_VERSIONS)];
            let l02 = vec![kv!("foo", "bar", 3, 0)];
            let l03 = vec![kv!("foo", "bar", 4, 0)];
            let l04 = vec![kv!("foo", "bar", 9, 0)];
            let l05 = vec![kv!("foo", "bar", 10, VALUE_DISCARD_EARLIER_VERSIONS)];

            create_and_open(lvctl, l0, 0);
            create_and_open(lvctl, l01, 0);
            create_and_open(lvctl, l02, 0);
            create_and_open(lvctl, l03, 0);
            create_and_open(lvctl, l04, 0);
            create_and_open(lvctl, l05, 0);

            lvctl.core.orc.set_discard_ts(7);

            let mut compact_def = generate_test_compect_def(lvctl, 0, 1);
            lvctl
                .core
                .run_compact_def(std::usize::MAX, 0, &mut compact_def, POOL.clone())
                .unwrap();

            get_all_and_check(
                lvctl,
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
}
