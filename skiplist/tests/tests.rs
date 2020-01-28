use skiplist::*;
use std::str;

const ARENA_SIZE: u32 = 1 << 20;

fn new_value(v: usize) -> Vec<u8> {
    format!("{:05}", v).into_bytes()
}

#[test]
fn test_empty() {
    let key = b"aaa".to_vec();
    let list = Skiplist::with_capacity(ARENA_SIZE);
    let v = list.get(&key);
    assert!(v.is_none());

    let mut iter = list.iter_ref();
    assert!(!iter.valid());
    iter.seek_to_first();
    assert!(!iter.valid());
    iter.seek_to_last();
    assert!(!iter.valid());
    iter.seek(&key);
    assert!(!iter.valid());
    assert!(list.is_empty());
}

#[test]
fn test_basic() {
    let list = Skiplist::with_capacity(ARENA_SIZE);
    let table = vec![
        (b"key1" as &'static [u8], new_value(42)),
        (b"key2", new_value(52)),
        (b"key3", new_value(62)),
        (b"key5", format!("{:0102400}", 1).into_bytes()),
        (b"key4", new_value(72)),
    ];

    for (key, value) in &table {
        list.put(key, value);
    }

    assert_eq!(list.get(b"key"), None);
    assert_eq!(list.len(), 5);
    assert!(!list.is_empty());
    println!("start search");
    for (key, value) in &table {
        let tag = unsafe { str::from_utf8_unchecked(key) };
        assert_eq!(list.get(key), Some(value.as_slice()), "{}", tag);
    }
}
