use super::*;
use crate::format::{key_with_ts, user_key};
use crate::value::Value;
use builder::Builder;
use tempdir::TempDir;

fn key(prefix: &[u8], i: usize) -> Bytes {
    Bytes::from([prefix, format!("{:04}", i).as_bytes()].concat())
}

fn key_isize(prefix: &[u8], i: isize) -> Bytes {
    Bytes::from([prefix, format!("{:04}", i).as_bytes()].concat())
}

#[test]
fn test_generate_key() {
    assert_eq!(key(b"key", 233), Bytes::from("key0233"));
}

fn get_test_table_options() -> Options {
    Options {
        block_size: 4 * 1024,
        table_size: 0,
        bloom_false_positive: 0.01,
    }
}

pub fn build_test_table(prefix: &[u8], n: usize, mut opts: Options) -> Table {
    if opts.block_size == 0 {
        opts.block_size = 4 * 1024;
    }
    assert!(n <= 10000);

    let mut kv_pairs = vec![];

    for i in 0..n {
        let k = key(prefix, i);
        let v = Bytes::from(i.to_string());
        kv_pairs.push((k, v));
    }

    build_table(kv_pairs, opts)
}

fn build_table(mut kv_pairs: Vec<(Bytes, Bytes)>, opts: Options) -> Table {
    let mut builder = Builder::new(opts.clone());
    let tmp_dir = TempDir::new("agatedb").unwrap();
    let filename = tmp_dir.path().join("1.sst".to_string());

    kv_pairs.sort_by(|x, y| x.0.cmp(&y.0));

    for (k, v) in kv_pairs {
        builder.add(&key_with_ts(&k[..], 0), Value::new_with_meta(v, b'A', 0), 0);
    }
    let data = builder.finish();

    Table::create(&filename, data, opts).unwrap()
    // you can also test in-memory table
    // Table::open_in_memory(data, 233, opts).unwrap()
    // `tmp_dir` will be dropped and the temp folder will be deleted
    // when we return from this function. However, as we saves file
    // descriptor to the file, we could still safely access that file.
}

#[test]
fn test_table_iterator() {
    for n in 99..=101 {
        let opts = get_test_table_options();
        let table = build_test_table(b"key", n, opts);
        let mut it = table.new_iterator(0);
        it.rewind();
        let mut count = 0;
        while it.valid() {
            let v = it.value();
            let k = it.key();
            assert_eq!(count.to_string(), v.value);
            assert_eq!(key_with_ts(&key(b"key", count)[..], 0), k);
            count += 1;
            it.next();
        }
        assert_eq!(count, n);
    }
}

#[test]
fn test_seek_to_first() {
    for n in vec![99, 100, 101, 199, 200, 250, 9999, 10000] {
        let opts = get_test_table_options();
        let table = build_test_table(b"key", n, opts);
        let mut it = table.new_iterator(0);
        it.seek_to_first();
        assert!(it.valid());
        assert_eq!(it.value().value, "0");
        assert_eq!(it.value().meta, b'A');
    }
}

#[test]
fn test_seek_to_last() {
    for n in vec![99, 100, 101, 199, 200, 250, 9999, 10000] {
        let opts = get_test_table_options();
        let table = build_test_table(b"key", n, opts);
        let mut it = table.new_iterator(0);
        it.seek_to_last();
        assert!(it.valid());
        assert_eq!(it.value().value, (n - 1).to_string());
        assert_eq!(it.value().meta, b'A');
        it.prev_inner();
        assert!(it.valid());
        assert_eq!(it.value().value, (n - 2).to_string());
        assert_eq!(it.value().meta, b'A');
    }
}

#[test]
fn test_seek() {
    let opts = get_test_table_options();
    let table = build_test_table(b"k", 10000, opts);
    let mut it = table.new_iterator(0);

    let data = vec![
        (b"abc".to_vec(), true, b"k0000".to_vec()),
        (b"k0100".to_vec(), true, b"k0100".to_vec()),
        (b"k0100b".to_vec(), true, b"k0101".to_vec()),
        (b"k1234".to_vec(), true, b"k1234".to_vec()),
        (b"k1234b".to_vec(), true, b"k1235".to_vec()),
        (b"k9999".to_vec(), true, b"k9999".to_vec()),
        (b"z".to_vec(), false, b"".to_vec()),
    ];

    for (input, valid, out) in data {
        it.seek(&key_with_ts(input.as_slice(), 0));
        assert_eq!(it.valid(), valid);
        if !valid {
            continue;
        }
        // compare Bytes to make output more readable
        assert_eq!(Bytes::copy_from_slice(user_key(it.key())), Bytes::from(out));
    }
}

#[test]
fn test_seek_for_prev() {
    let opts = get_test_table_options();
    let table = build_test_table(b"k", 10000, opts);
    let mut it = table.new_iterator(0);

    let data = vec![
        ("abc", false, ""),
        ("k0100", true, "k0100"),
        ("k0100b", true, "k0100"), // Test case where we jump to next block.
        ("k1234", true, "k1234"),
        ("k1234b", true, "k1234"),
        ("k9999", true, "k9999"),
        ("z", true, "k9999"),
    ];

    for (input, valid, out) in data {
        it.seek_for_prev(&key_with_ts(input.as_bytes(), 0));
        assert_eq!(it.valid(), valid);
        if !valid {
            continue;
        }
        // compare Bytes to make output more readable
        assert_eq!(Bytes::copy_from_slice(user_key(it.key())), Bytes::from(out));
    }
}

#[test]
fn test_iterate_from_start() {
    for n in vec![99, 100, 101, 199, 200, 250, 9999, 10000] {
        let opts = get_test_table_options();
        let table = build_test_table(b"key", n, opts);
        let mut it = table.new_iterator(0);
        it.reset();
        it.seek_to_first();
        assert!(it.valid());

        let mut count = 0;
        while it.valid() {
            let v = it.value();
            assert_eq!(count.to_string(), v.value);
            assert_eq!(b'A', v.meta);
            it.next();
            count += 1;
        }

        assert_eq!(n, count);
    }
}

#[test]
fn test_iterate_from_end() {
    for n in vec![99, 100, 101, 199, 200, 250, 9999, 10000] {
        let opts = get_test_table_options();
        let table = build_test_table(b"key", n, opts);
        let mut it = table.new_iterator(0);
        it.reset();
        it.seek(&key_with_ts(b"zzzzzz" as &[u8], 0));
        assert!(!it.valid());

        for i in (0..n).rev() {
            it.prev_inner();
            assert!(it.valid());
            let v = it.value();
            assert_eq!(i.to_string(), v.value);
            assert_eq!(b'A', v.meta);
        }
        it.prev_inner();
        assert!(!it.valid())
    }
}

#[test]
fn test_table() {
    let opts = get_test_table_options();
    let table = build_test_table(b"key", 10000, opts);
    let mut it = table.new_iterator(0);
    let mut kid = 1010;
    let seek = key_with_ts(&key(b"key", kid)[..], 0);
    it.seek(&seek);
    while it.valid() {
        assert_eq!(user_key(it.key()), &key(b"key", kid)[..]);
        kid += 1;
        it.next();
    }
    assert_eq!(kid, 10000);

    it.seek(&key_with_ts(&key(b"key", 99999)[..], 0));
    assert!(!it.valid());

    it.seek(&key_with_ts(&key_isize(b"key", -1)[..], 0));
    assert!(it.valid());

    assert_eq!(user_key(it.key()), key(b"key", 0));
}

#[test]
fn test_iterate_back_and_forth() {
    let opts = get_test_table_options();
    let table = build_test_table(b"key", 10000, opts);
    let mut it = table.new_iterator(0);
    let seek = key_with_ts(&key(b"key", 1010)[..], 0);

    it.seek(&seek);
    assert!(it.valid());
    assert_eq!(it.key(), &seek);

    it.prev_inner();
    it.prev_inner();
    assert!(it.valid());
    assert_eq!(user_key(it.key()), &key(b"key", 1008)[..]);

    it.next_inner();
    it.next_inner();
    assert!(it.valid());
    assert_eq!(user_key(it.key()), &key(b"key", 1010)[..]);

    it.seek(&key_with_ts(&key(b"key", 2000)[..], 0));
    assert!(it.valid());
    assert_eq!(user_key(it.key()), &key(b"key", 2000)[..]);

    it.prev_inner();
    assert!(it.valid());
    assert_eq!(user_key(it.key()), &key(b"key", 1999)[..]);

    it.seek_to_first();
    assert!(it.valid());
    assert_eq!(user_key(it.key()), &key(b"key", 0)[..]);
}

#[test]
fn test_uni_iterator() {
    let opts = get_test_table_options();
    let table = build_test_table(b"key", 10000, opts);

    let mut it = table.new_iterator(0);
    it.rewind();
    let mut count = 0;
    while it.valid() {
        let v = it.value();
        assert_eq!(count.to_string(), v.value);
        assert_eq!(b'A', v.meta);
        it.next();
        count += 1;
    }
    assert_eq!(count, 10000);

    let mut it = table.new_iterator(ITERATOR_REVERSED);
    it.rewind();
    let mut count = 0;
    while it.valid() {
        let v = it.value();
        assert_eq!((10000 - 1 - count).to_string(), v.value);
        assert_eq!(b'A', v.meta);
        it.next();
        count += 1;
    }
    assert_eq!(count, 10000);
}

// TODO: concat iterators and merge iterators

fn value(i: usize) -> Bytes {
    Bytes::from(format!("{:01048576}", i)) // 1MB value
}

#[test]
fn test_table_big_values() {
    let n: usize = 100;
    let opts = Options {
        block_size: 4 * 1024,
        bloom_false_positive: 0.01,
        table_size: (n as u64) * (1 << 20),
    };
    let mut builder = Builder::new(opts.clone());

    for i in 0..n {
        let key = key_with_ts(&key(b"", i)[..], i as u64 + 1);
        let vs = Value::new(value(i));
        builder.add(&key, vs, 0);
    }

    let tmp_dir = TempDir::new("agatedb").unwrap();
    let filename = tmp_dir.path().join("1.sst".to_string());

    let table = Table::create(&filename, builder.finish(), opts).unwrap();

    let mut it = table.new_iterator(0);
    assert!(it.valid());

    let mut count = 0;
    it.rewind();

    while it.valid() {
        assert_eq!(key(b"", count), user_key(it.key()));
        assert_eq!(value(count), it.value().value);
        it.next();
        count += 1;
    }

    assert!(!it.valid());
    assert_eq!(n, count);
    // TODO: support max_version in table
    // assert_eq!(n, table.max_version());
}
