use bytes::{Bytes, BytesMut};
use tempdir::TempDir;
use tempfile::tempdir;

use super::*;
use crate::{
    entry::Entry,
    format::{append_ts, key_with_ts},
};

#[test]
fn test_open_mem_tables() {
    let mut opts = AgateOptions::default();
    let tmp_dir = tempdir().unwrap();
    opts.dir = tmp_dir.path().to_path_buf();

    let (_imm_tables, next_mem_fid) = Core::open_mem_tables(&opts).unwrap();
    assert_eq!(next_mem_fid, 1);
    let _mt = Core::open_mem_table(&opts, next_mem_fid).unwrap();
}

#[test]
fn test_memtable_persist() {
    let mut opts = AgateOptions::default();
    let tmp_dir = tempdir().unwrap();
    opts.dir = tmp_dir.path().to_path_buf();

    let mt = Core::open_mem_table(&opts, 1).unwrap();

    let mut key = BytesMut::from("key".to_string().as_bytes());
    append_ts(&mut key, 100);
    let key = key.freeze();
    let value = Value::new(key.clone());

    mt.put(key.clone(), value.clone()).unwrap();

    let value_get = mt.skl.get(&key).unwrap();
    assert_eq!(&Bytes::from(value.clone()), value_get);

    mt.mark_save();

    let mt = Core::open_mem_table(&opts, 1).unwrap();
    let value_get = mt.skl.get(&key).unwrap();
    assert_eq!(&Bytes::from(value), value_get);
}

#[test]
fn test_ensure_room_for_write() {
    let mut opts = AgateOptions::default();
    let tmp_dir = tempdir().unwrap();
    opts.dir = tmp_dir.path().to_path_buf();
    opts.value_dir = opts.dir.clone();

    // Wal::zero_next_entry will need MAX_HEADER_SIZE bytes free space.
    // So we should put bytes more than value_log_file_size but less than
    // 2*value_log_file_size - MAX_HEADER_SIZE.
    opts.value_log_file_size = 25;

    let inner = Core::new(opts).unwrap();

    {
        let mts = inner.mts.read().unwrap();
        assert_eq!(mts.nums_of_memtable(), 1);

        let mt = mts.mut_table();

        let key = key_with_ts(BytesMut::new(), 1);
        let value = Value::new(Bytes::new());
        // Put once, write_at in wal += 13, so we put twice to make write_at larger
        // than value_log_file_size.
        mt.put(key.clone(), value.clone()).unwrap();
        mt.put(key, value).unwrap();
    }

    inner.ensure_room_for_write().unwrap();

    let mts = inner.mts.read().unwrap();
    assert_eq!(mts.nums_of_memtable(), 2);
}

pub fn generate_test_agate_options() -> AgateOptions {
    AgateOptions {
        mem_table_size: 1 << 14,
        // Force more compaction.
        base_table_size: 1 << 15,
        // Set base level size small enought to make the compactor flush L0 to L5 and L6.
        base_level_size: 4 << 10,
        value_log_file_size: 4 << 20,
        ..Default::default()
    }
}

pub fn helper_dump_dir(path: &Path) {
    let mut result = vec![];
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            result.push(path);
        }
    }
    result.sort();

    for path in result {
        println!("{:?}", path);
    }
}

fn with_agate_test(in_memory: bool, f: impl FnOnce(Agate)) {
    let tmp_dir = TempDir::new("agatedb").unwrap();
    let agate = AgateOptions::default()
        .set_create_if_not_exists(true)
        .set_in_memory(in_memory)
        .set_value_log_file_size(4096)
        .open(&tmp_dir)
        .unwrap();
    f(agate);
    helper_dump_dir(tmp_dir.path());
    tmp_dir.close().unwrap();
}

#[test]
fn test_simple_get_put() {
    with_agate_test(false, |agate| {
        let key = key_with_ts(BytesMut::from("2333"), 0);
        let value = Bytes::from("2333333333333333");
        let req = Request {
            entries: vec![Entry::new(key.clone(), value)],
            ptrs: vec![],
            done: None,
        };
        agate.write_to_lsm(req).unwrap();
        let value = agate.get(&key).unwrap();
        assert_eq!(value.value, Bytes::from("2333333333333333"));
    });
}

fn generate_requests(n: usize) -> Vec<Request> {
    (0..n)
        .map(|i| Request {
            entries: vec![Entry::new(
                key_with_ts(BytesMut::from(format!("{:08x}", i).as_str()), 0),
                Bytes::from(i.to_string()),
            )],
            ptrs: vec![],
            done: None,
        })
        .collect()
}

fn verify_requests(n: usize, agate: &Agate) {
    for i in 0..n {
        let value = agate
            .get(&key_with_ts(
                BytesMut::from(format!("{:08x}", i).as_str()),
                0,
            ))
            .unwrap();
        assert_eq!(value.value, i.to_string());
    }
}

#[test]
fn test_flush_memtable() {
    with_agate_test(false, |agate| {
        agate.write_requests(generate_requests(2000)).unwrap();
        verify_requests(2000, &agate);
    });
}

#[test]
fn test_in_memory_agate() {
    with_agate_test(true, |agate| {
        agate.write_requests(generate_requests(10)).unwrap();
        verify_requests(10, &agate);
    });
}
