use super::*;
use crate::{Agate, TableOptions, table::new_filename, db::tests::with_agate_test};

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

        assert!(agate.core.lvctl.core.check_overlap)
    });
}
