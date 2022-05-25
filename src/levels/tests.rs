use std::fs;

use tempdir::TempDir;

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
        let orc = Arc::new(Oracle::default());

        let mut lvctl = LevelsController::new(opts.clone(), manifest, orc).unwrap();

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

#[allow(unused)]
macro_rules! kv {
    ($key: expr, $val: expr, $meta: expr, $user_meta: expr) => {
        KeyValVersion::new($key, $val, $meta, $user_meta)
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

#[test]
fn test_lvctl_test() {
    lvctl_test(|_| {});
}

#[test]
fn test_same_keys() {
    lvctl_test(|lvctl| {
        let l0 = kv!("foo", "bar", 3, 0);
        let l1 = kv!("foo", "bar", 2, 0);
        create_and_open(lvctl, vec![l0], 0);
        create_and_open(lvctl, vec![l1], 1);

        let l0_tables = lvctl.core.levels[0].read().unwrap().tables.clone();
        let l1_tables = lvctl.core.levels[1].read().unwrap().tables.clone();

        // lv0 should overlap with lv0 tables
        assert!(lvctl.core.check_overlap(&l0_tables, 0));
        // lv1 should overlap with lv0 tables
        assert!(lvctl.core.check_overlap(&l0_tables, 1));
        // lv2 and lv3 should not overlap with lv0 tables
        assert!(!lvctl.core.check_overlap(&l0_tables, 2));
        assert!(!lvctl.core.check_overlap(&l1_tables, 2));
        assert!(!lvctl.core.check_overlap(&l0_tables, 3));
        assert!(!lvctl.core.check_overlap(&l1_tables, 3));
    });
}
