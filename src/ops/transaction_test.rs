use crate::{assert_bytes_eq, Error};

/// Tests in managed mode.
mod managed_db {
    use bytes::{Bytes, BytesMut};

    use super::*;
    use crate::{
        db::tests::{generate_test_agate_options, run_agate_test, with_payload},
        entry::Entry,
        AgateOptions,
    };

    fn default_test_managed_opts() -> AgateOptions {
        let mut opts = generate_test_agate_options();
        opts.managed_txns = true;
        opts
    }

    fn test_with_options(opts: Option<AgateOptions>) {
        let key = |i: u64| Bytes::from(format!("key-{:02}", i));
        let value = |i: u64| Bytes::from(format!("val-{}", i));

        run_agate_test(opts, move |agate| {
            agate.view(|_txn| Ok(())).unwrap();

            // write data at t=3
            let mut txn = agate.new_transaction_at(3, true);
            for i in 0..=3 {
                txn.set_entry(Entry::new(key(i), value(i))).unwrap();
            }
            txn.commit_at(3).unwrap();

            // read data at t=2
            let mut txn = agate.new_transaction_at(2, false);
            for i in 0..=3 {
                assert!(matches!(txn.get(&key(i)), Err(Error::KeyNotFound(()))));
            }
            txn.discard();

            // read data at t=3
            let mut txn = agate.new_transaction_at(3, false);
            for i in 0..=3 {
                let item = txn.get(&key(i)).unwrap();
                assert_eq!(item.version, 3);
                assert_eq!(item.vptr, value(i));
            }
            txn.discard();

            // write data at t=7
            let mut txn = agate.new_transaction_at(6, true);
            for i in 0..=7 {
                if txn.get(&key(i)).is_err() {
                    txn.set_entry(Entry::new(key(i), value(i))).unwrap();
                }
            }
            txn.commit_at(7).unwrap();

            // read data at t=9
            let mut txn = agate.new_transaction_at(9, false);
            for i in 0..=9 {
                let item = txn.get(&key(i));
                if i <= 7 {
                    assert!(item.is_ok());
                } else {
                    assert!(matches!(item, Err(Error::KeyNotFound(()))));
                }

                if i <= 3 {
                    assert_eq!(item.as_ref().unwrap().version, 3);
                } else if i <= 7 {
                    assert_eq!(item.as_ref().unwrap().version, 7);
                }

                if i <= 7 {
                    let v = item.unwrap().vptr;
                    assert_eq!(value(i), v);
                }
            }
            txn.discard();
        })
    }

    #[test]
    fn test_on_disk() {
        test_with_options(Some(default_test_managed_opts()));
    }

    #[test]
    fn test_in_memory() {
        let mut opts = default_test_managed_opts();
        opts.in_memory = true;
        test_with_options(Some(opts));
    }

    #[test]
    fn test_big_value() {
        run_agate_test(Some(default_test_managed_opts()), |agate| {
            let payload = 1 << 20;
            let key = |i| Bytes::from(format!("{:08x}", i));

            for i in 0..15 {
                let mut txn = agate.new_transaction_at(3, true);
                let entry = Entry::new(
                    key(i),
                    with_payload(BytesMut::from(&key(i)[..]), payload, (i % 256) as u8),
                );
                txn.set_entry(entry).unwrap();
                txn.commit_at(7).unwrap();
            }

            agate
                .view(|txn| {
                    for i in 0..15 {
                        let key = key(i);
                        let item = txn.get(&key).unwrap();
                        assert_bytes_eq!(&key, &item.key);
                        let value = item.value();
                        assert_bytes_eq!(&key, &value[..8]);
                        for j in &value[8..] {
                            assert_eq!(*j, (i % 256) as u8);
                        }
                    }
                    Ok(())
                })
                .unwrap();
        });
    }
}

mod normal_db {
    use std::sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    };

    use bytes::{Bytes, BytesMut};
    use crossbeam_channel::select;
    use rand::Rng;
    use yatp::{task::callback::Handle, Builder};

    use super::*;
    use crate::{
        closer::Closer,
        db::tests::{generate_test_agate_options, run_agate_test},
        entry::Entry,
        iterator::{Iterator, IteratorOptions},
        key_with_ts,
        ops::transaction::Transaction,
        value::VALUE_DELETE,
        Agate, AgateOptions,
    };

    #[test]
    fn test_txn_simple() {
        run_agate_test(None, move |agate| {
            let key = |i: u64| Bytes::from(format!("key={:02}", i));
            let value = |i: u64| Bytes::from(format!("val={:02}", i));

            let mut txn = agate.new_transaction(true);

            for i in 0..5 {
                txn.set_entry(Entry::new(key(i), value(i))).unwrap();
            }

            let item = txn.get(&key(1)).unwrap();

            assert_bytes_eq!(&item.value(), &value(1));

            txn.commit().unwrap();
        });
    }

    #[test]
    fn test_txn_read_after_write() {
        let test = |agate: Arc<Agate>| {
            let pool = Builder::new("test_txn_read_after_write")
                .max_thread_count(10)
                .build_callback_pool();

            for i in 0..10 {
                let agate = agate.clone();

                pool.spawn(move |_: &mut Handle<'_>| {
                    let key = Bytes::from(format!("key={:02}", i));
                    agate
                        .update(|txn| txn.set_entry(Entry::new(key.clone(), key.clone())))
                        .unwrap();

                    agate
                        .view(|txn| {
                            let item = txn.get(&key).unwrap();

                            assert_bytes_eq!(&item.value(), &key.clone());

                            Ok(())
                        })
                        .unwrap();
                });
            }

            pool.shutdown();
        };

        run_agate_test(None, test);

        let mut opts = generate_test_agate_options();
        opts.in_memory = true;
        run_agate_test(Some(opts), test);
    }

    #[test]
    fn test_commit_async() {
        let key = |i: u64| Bytes::from(format!("key={:02}", i));

        run_agate_test(None, |agatedb| {
            let mut txn = agatedb.new_transaction(true);

            for i in 0..6 {
                txn.set_entry(Entry::new(key(i), Bytes::from(100.to_string())))
                    .unwrap();
            }

            txn.commit().unwrap();

            let pool = Builder::new("test_commit_async")
                .max_thread_count(10)
                .build_callback_pool();

            let closer = Closer::new();

            let agate = agatedb.clone();
            let c = closer.clone();
            pool.spawn(move |_: &mut Handle<'_>| loop {
                select! {
                    recv(c.get_receiver()) -> _ => {
                        return
                    }
                    default() => {
                        // Keep checking consistency.
                        let txn = agate.new_transaction(false);
                        let mut total = 0;
                        for i in 0..6{
                            let item = txn.get(&key(i)).unwrap();
                            let value = item.value();
                            let cnt: u64 = String::from_utf8(value.to_vec()).unwrap().parse().unwrap();
                            total += cnt;
                        }

                        assert_eq!(total, 600);
                    }

                }
            });

            for _ in 0..10 {
                let agate = agatedb.clone();
                pool.spawn(move |_: &mut Handle<'_>| {
                    let mut txn = agate.new_transaction(true);
                    let delta = rand::thread_rng().gen_range(0, 100);

                    for i in 0..3 {
                        txn.set_entry(Entry::new(key(i), Bytes::from((100 - delta).to_string())))
                            .unwrap();
                    }

                    for i in 3..6 {
                        txn.set_entry(Entry::new(key(i), Bytes::from((100 + delta).to_string())))
                            .unwrap();
                    }

                    txn.commit().unwrap();
                });
            }

            closer.close();
            pool.shutdown();
        });
    }

    #[test]
    fn test_txn_versions() {
        run_agate_test(None, |agate| {
            let key = Bytes::from("key");
            let valversion = |version| Bytes::from(format!("valversion={}", version));

            for i in 1..10 {
                let mut txn = agate.new_transaction(true);
                txn.set_entry(Entry::new(key.clone(), valversion(i)))
                    .unwrap();
                txn.commit().unwrap();

                assert_eq!(i, agate.core.orc.read_ts());
            }

            let check_iterator = |mut it: Iterator, i: u64| {
                let mut count = 0;

                it.rewind();
                while it.valid() {
                    let item = it.item();
                    assert_bytes_eq!(&item.key, &key);
                    assert_bytes_eq!(&item.value(), &valversion(i));

                    count += 1;

                    it.next();
                }

                assert_eq!(count, 1);
            };

            let check_all_versions = |mut it: Iterator, i: u64| {
                let mut version = if it.opt.reverse { 1 } else { i };

                let mut count = 0;
                it.rewind();
                while it.valid() {
                    let item = it.item();
                    assert_bytes_eq!(&item.key, &key);
                    assert_eq!(item.version, version);

                    let value = item.value();
                    assert_bytes_eq!(&value, &valversion(version));

                    count += 1;

                    if it.opt.reverse {
                        version += 1;
                    } else {
                        version -= 1;
                    }

                    it.next();
                }

                assert_eq!(count, i);
            };

            for i in 1..10 {
                let mut txn = agate.new_transaction(true);
                txn.read_ts = i; // Read version at i.

                let item = txn.get(&key).unwrap();
                let value = item.value();
                assert_bytes_eq!(&value, &valversion(i));

                let it = txn.new_iterator(&IteratorOptions::default());
                check_iterator(it, i);

                let reversed_it = txn.new_iterator(&IteratorOptions {
                    reverse: true,
                    ..Default::default()
                });
                check_iterator(reversed_it, i);

                let it = txn.new_iterator(&IteratorOptions {
                    all_versions: true,
                    ..Default::default()
                });
                check_all_versions(it, i);

                let reversed_it = txn.new_iterator(&IteratorOptions {
                    reverse: true,
                    all_versions: true,
                    ..Default::default()
                });
                check_all_versions(reversed_it, i);
            }

            let txn = agate.new_transaction(true);
            let item = txn.get(&key).unwrap();
            let value = item.value();
            assert_bytes_eq!(&value, &valversion(9));
        });
    }

    #[test]
    fn test_txn_write_skew() {
        run_agate_test(None, |agate| {
            let balance = |i: u64| Bytes::from(format!("{}", i));

            // Accounts.
            let ax = Bytes::from("x");
            let ay = Bytes::from("y");

            // Set balance to $100 in each account.
            let mut txn = agate.new_transaction(true);
            txn.set_entry(Entry::new(ax.clone(), balance(100))).unwrap();
            txn.set_entry(Entry::new(ay.clone(), balance(100))).unwrap();
            txn.commit().unwrap();

            let get_bal = |txn: &Transaction, key: Bytes| -> u64 {
                let item = txn.get(&key).unwrap();
                let value = item.value();
                String::from_utf8(value.to_vec()).unwrap().parse().unwrap()
            };

            // Start two transactions, each would read both accounts and deduct from one account.
            let mut txn1 = agate.new_transaction(true);

            let sum = get_bal(&txn1, ax.clone()) + get_bal(&txn1, ay.clone());
            assert_eq!(sum, 200);
            // Deduct 100 from ax.
            txn1.set_entry(Entry::new(ax.clone(), balance(0))).unwrap();
            // Read again.
            let sum = get_bal(&txn1, ax.clone()) + get_bal(&txn1, ay.clone());
            assert_eq!(sum, 100);

            let mut txn2 = agate.new_transaction(true);
            let sum = get_bal(&txn2, ax.clone()) + get_bal(&txn2, ay.clone());
            assert_eq!(sum, 200);
            // Deduct 100 from ay.
            txn2.set_entry(Entry::new(ay.clone(), balance(0))).unwrap();
            let sum = get_bal(&txn2, ax) + get_bal(&txn2, ay);
            assert_eq!(sum, 100);

            txn1.commit().unwrap();
            assert!(txn2.commit().is_err());

            assert_eq!(agate.core.orc.read_ts(), 2);
        });
    }

    #[test]
    fn test_txn_iteration_edge_case() {
        // a3, a2, b4 (del), b3, c2, c1
        // Read at ts=4 -> a3, c2
        // Read at ts=4(Uncommitted) -> a3, b4
        // Read at ts=3 -> a3, b3, c2
        // Read at ts=2 -> a2, c2
        // Read at ts=1 -> c1
        run_agate_test(None, |agate| {
            // Satisfy FixedLengthSuffixComparator need.
            let ka = Bytes::from("aaaaaaaa");
            let kb = Bytes::from("bbbbbbbb");
            let kc = Bytes::from("cccccccc");

            // c1
            let mut txn = agate.new_transaction(true);
            txn.set_entry(Entry::new(kc.clone(), Bytes::from("c1")))
                .unwrap();
            txn.commit().unwrap();
            assert_eq!(agate.core.orc.read_ts(), 1);

            // a2, c2
            let mut txn = agate.new_transaction(true);
            txn.set_entry(Entry::new(ka.clone(), Bytes::from("a2")))
                .unwrap();
            txn.set_entry(Entry::new(kc.clone(), Bytes::from("c2")))
                .unwrap();
            txn.commit().unwrap();
            assert_eq!(agate.core.orc.read_ts(), 2);

            // a3, b3
            let mut txn = agate.new_transaction(true);
            txn.set_entry(Entry::new(ka, Bytes::from("a3"))).unwrap();
            txn.set_entry(Entry::new(kb.clone(), Bytes::from("b3")))
                .unwrap();
            txn.commit().unwrap();
            assert_eq!(agate.core.orc.read_ts(), 3);

            // b4, c4(del) (Uncommitted)
            let mut txn4 = agate.new_transaction(true);
            txn4.set_entry(Entry::new(kb.clone(), Bytes::from("b4")))
                .unwrap();
            txn4.delete(kc).unwrap();
            assert_eq!(agate.core.orc.read_ts(), 3);

            // b4 (del)
            let mut txn = agate.new_transaction(true);
            txn.delete(kb).unwrap();
            txn.commit().unwrap();
            assert_eq!(agate.core.orc.read_ts(), 4);

            let check_iterator = |mut it: Iterator, expectd: Vec<&'static str>| {
                let mut index = 0;
                it.rewind();
                while it.valid() {
                    let item = it.item();
                    let value = item.value();

                    assert_bytes_eq!(&value, &Bytes::from(expectd[index]));

                    index += 1;
                    it.next();
                }
                assert_eq!(expectd.len(), index);
            };

            let mut txn = agate.new_transaction(true);
            let rev = IteratorOptions {
                reverse: true,
                ..Default::default()
            };

            let it = txn.new_iterator(&IteratorOptions::default());
            let it5 = txn4.new_iterator(&IteratorOptions::default());
            check_iterator(it, vec!["a3", "c2"]);
            check_iterator(it5, vec!["a3", "b4"]);

            let it = txn.new_iterator(&rev);
            let it5 = txn4.new_iterator(&rev);
            check_iterator(it, vec!["c2", "a3"]);
            check_iterator(it5, vec!["b4", "a3"]);

            txn.read_ts = 3;
            let it = txn.new_iterator(&IteratorOptions::default());
            check_iterator(it, vec!["a3", "b3", "c2"]);
            let it = txn.new_iterator(&rev);
            check_iterator(it, vec!["c2", "b3", "a3"]);

            txn.read_ts = 2;
            let it = txn.new_iterator(&IteratorOptions::default());
            check_iterator(it, vec!["a2", "c2"]);
            let it = txn.new_iterator(&rev);
            check_iterator(it, vec!["c2", "a2"]);

            txn.read_ts = 1;
            let it = txn.new_iterator(&IteratorOptions::default());
            check_iterator(it, vec!["c1"]);
            let it = txn.new_iterator(&rev);
            check_iterator(it, vec!["c1"]);
        });
    }

    #[test]
    fn test_txn_iteration_edge_case2() {
        // a2, a3, b4 (del), b3, c2, c1
        // Read at ts=4 -> a3, c2
        // Read at ts=3 -> a3, b3, c2
        // Read at ts=2 -> a2, c2
        // Read at ts=1 -> c1
        run_agate_test(None, |agate| {
            // Satisfy FixedLengthSuffixComparator need.
            let ka = Bytes::from("aaaaaaaa");
            let kb = Bytes::from("bbbbbbbb");
            let kc = Bytes::from("cccccccc");

            // c1
            let mut txn = agate.new_transaction(true);
            txn.set_entry(Entry::new(kc.clone(), Bytes::from("c1")))
                .unwrap();
            txn.commit().unwrap();
            assert_eq!(agate.core.orc.read_ts(), 1);

            // a2, c2
            let mut txn = agate.new_transaction(true);
            txn.set_entry(Entry::new(ka.clone(), Bytes::from("a2")))
                .unwrap();
            txn.set_entry(Entry::new(kc.clone(), Bytes::from("c2")))
                .unwrap();
            txn.commit().unwrap();
            assert_eq!(agate.core.orc.read_ts(), 2);

            // a3, b3
            let mut txn = agate.new_transaction(true);
            txn.set_entry(Entry::new(ka.clone(), Bytes::from("a3")))
                .unwrap();
            txn.set_entry(Entry::new(kb.clone(), Bytes::from("b3")))
                .unwrap();
            txn.commit().unwrap();
            assert_eq!(agate.core.orc.read_ts(), 3);

            // b4 (del)
            let mut txn = agate.new_transaction(true);
            txn.delete(kb).unwrap();
            txn.commit().unwrap();
            assert_eq!(agate.core.orc.read_ts(), 4);

            let check_iterator = |mut it: Iterator, expectd: Vec<&'static str>| {
                let mut index = 0;
                it.rewind();
                while it.valid() {
                    let item = it.item();
                    let value = item.value();

                    assert_bytes_eq!(&value, &Bytes::from(expectd[index]));

                    index += 1;
                    it.next();
                }
                assert_eq!(expectd.len(), index);
            };

            let mut txn = agate.new_transaction(true);
            let rev = IteratorOptions {
                reverse: true,
                ..Default::default()
            };

            let it = txn.new_iterator(&IteratorOptions::default());
            check_iterator(it, vec!["a3", "c2"]);
            let it = txn.new_iterator(&rev);
            check_iterator(it, vec!["c2", "a3"]);

            txn.read_ts = 5;
            {
                let mut it = txn.new_iterator(&IteratorOptions::default());
                it.seek(&ka);
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &ka);
                it.seek(&kc);
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kc);

                let mut it = txn.new_iterator(&rev);
                it.seek(&ka);
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &ka);
                it.seek(&kc);
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kc);
            }

            txn.read_ts = 3;
            let it = txn.new_iterator(&IteratorOptions::default());
            check_iterator(it, vec!["a3", "b3", "c2"]);
            let it = txn.new_iterator(&rev);
            check_iterator(it, vec!["c2", "b3", "a3"]);

            txn.read_ts = 2;
            let it = txn.new_iterator(&IteratorOptions::default());
            check_iterator(it, vec!["a2", "c2"]);
            let it = txn.new_iterator(&rev);
            check_iterator(it, vec!["c2", "a2"]);

            txn.read_ts = 1;
            let it = txn.new_iterator(&IteratorOptions::default());
            check_iterator(it, vec!["c1"]);
            let it = txn.new_iterator(&rev);
            check_iterator(it, vec!["c1"]);
        });
    }

    #[test]
    fn test_txn_iteration_edge_case3() {
        run_agate_test(None, |agate| {
            // Satisfy FixedLengthSuffixComparator need.
            let kb = Bytes::from("abcabcabc");
            let kc = Bytes::from("acdacdacd");
            let kd = Bytes::from("adeadeade");

            // c1
            let mut txn = agate.new_transaction(true);
            txn.set_entry(Entry::new(kc.clone(), Bytes::from("c1")))
                .unwrap();
            txn.commit().unwrap();
            assert_eq!(agate.core.orc.read_ts(), 1);

            // b2
            let mut txn = agate.new_transaction(true);
            txn.set_entry(Entry::new(kb.clone(), Bytes::from("b2")))
                .unwrap();
            txn.commit().unwrap();
            assert_eq!(agate.core.orc.read_ts(), 2);

            let mut txn2 = agate.new_transaction(true);
            txn2.set_entry(Entry::new(kd.clone(), Bytes::from("d2")))
                .unwrap();
            txn2.delete(kc.clone()).unwrap();

            let txn = agate.new_transaction(true);
            let rev = IteratorOptions {
                reverse: true,
                ..Default::default()
            };

            {
                let mut it = txn.new_iterator(&IteratorOptions::default());
                it.seek(&Bytes::from("ab"));
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kb);
                it.seek(&Bytes::from("ac"));
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kc);
                it.seek(&Bytes::new());
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kb);

                it.rewind();
                it.seek(&Bytes::new());
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kb);
                it.seek(&Bytes::from("ac"));
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kc);
            }

            {
                let mut it = txn2.new_iterator(&IteratorOptions::default());
                it.seek(&Bytes::from("abbbbbbb"));
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kb);
                it.seek(&Bytes::from("accccccc"));
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kd);
                it.seek(&Bytes::new());
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kb);

                it.rewind();
                it.seek(&Bytes::new());
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kb);
                it.seek(&Bytes::from("accccccc"));
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kd);
            }

            {
                let mut it = txn.new_iterator(&rev);
                it.seek(&Bytes::from("ac"));
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kb);
                it.seek(&Bytes::from("ad"));
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kc);
                it.seek(&Bytes::new());
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kc);

                it.rewind();
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kc);
                it.seek(&Bytes::from("ad"));
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kc);
            }

            {
                let mut it = txn2.new_iterator(&rev);
                it.seek(&Bytes::from("addddddd"));
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kb);
                it.seek(&Bytes::from("aeeeeeee"));
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kd);
                it.seek(&Bytes::new());
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kd);

                it.rewind();
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kd);
                it.seek(&Bytes::from("accccccc"));
                assert!(it.valid());
                assert_bytes_eq!(&it.item().key, &kb);
            }
        });
    }

    #[test]
    fn test_iterator_all_version_with_deleted() {
        let test = |agate: Arc<Agate>| {
            let key1 = Bytes::from("key1");
            let key2 = Bytes::from("key2");

            agate
                .update(|txn| {
                    txn.set_entry(Entry::new(key1.clone(), key1.clone()))
                        .unwrap();
                    txn.set_entry(Entry::new(key2.clone(), key2.clone()))
                })
                .unwrap();

            // Notice skiplist will not overwrite value with same key.
            agate
                .view(|txn| {
                    let item = txn.get(&key1).unwrap();
                    let mut item_key = BytesMut::new();
                    item_key.extend_from_slice(&item.key);
                    let entry = Entry {
                        key: key_with_ts(item_key, item.version + 1),
                        value: Bytes::new(),
                        meta: VALUE_DELETE,
                        user_meta: 0,
                        expires_at: 0,
                        version: 0,
                    };
                    agate
                        .core
                        .send_to_write_channel(vec![entry])
                        .unwrap()
                        .recv()
                        .unwrap()
                })
                .unwrap();

            agate
                .view(|txn| {
                    txn.read_ts = 2;
                    let mut it = txn.new_iterator(&IteratorOptions {
                        all_versions: true,
                        ..Default::default()
                    });

                    let mut count = 0;
                    it.rewind();
                    while it.valid() {
                        count += 1;

                        let item = it.item();
                        if count == 1 {
                            assert_bytes_eq!(&item.key, &key1);
                            assert!(item.meta & VALUE_DELETE > 0);
                        } else if count == 2 {
                            assert_bytes_eq!(&item.key, &key1);
                            assert_eq!(item.meta & VALUE_DELETE, 0);
                        } else {
                            assert_bytes_eq!(&item.key, &key2);
                        }

                        it.next();
                    }

                    assert_eq!(count, 3);
                    Ok(())
                })
                .unwrap();
        };

        run_agate_test(None, test);

        let opts = AgateOptions {
            in_memory: true,
            ..Default::default()
        };
        run_agate_test(Some(opts), test);
    }

    #[test]
    fn test_iterator_all_version_with_deleted2() {
        run_agate_test(None, |agate| {
            let key = Bytes::from("key");
            let value = Bytes::from("value");

            for i in 0..4 {
                agate
                    .update(|txn| {
                        if i % 2 == 0 {
                            txn.set_entry(Entry::new(key.clone(), value.clone()))
                        } else {
                            txn.delete(key.clone())
                        }
                    })
                    .unwrap();
            }

            agate
                .view(|txn| {
                    let mut it = txn.new_iterator(&IteratorOptions {
                        all_versions: true,
                        ..Default::default()
                    });

                    let mut count = 0;
                    it.rewind();
                    while it.valid() {
                        let item = it.item();
                        assert_bytes_eq!(&item.key, &key);

                        if count % 2 != 0 {
                            assert_bytes_eq!(&item.value(), &value);
                        } else {
                            assert!(item.meta & VALUE_DELETE > 0);
                        }

                        count += 1;
                        it.next();
                    }

                    assert_eq!(count, 4);

                    Ok(())
                })
                .unwrap();
        });
    }

    #[test]
    fn test_conflict() {
        let key = Bytes::from("foo");
        let set_count = Arc::new(AtomicU8::new(0));
        let test_count = Arc::new(AtomicU8::new(0));

        let key_clone = key.clone();
        let set_count_clone = set_count.clone();
        let test_count_clone = test_count.clone();
        let test_and_set = move |agate: Arc<Agate>| {
            test_count_clone.fetch_add(1, Ordering::SeqCst);

            let mut txn = agate.new_transaction(true);

            if let Err(Error::KeyNotFound(_)) = txn.get(&key_clone) {
                txn.set(key_clone.clone(), Bytes::from("AA")).unwrap();
                if txn.commit().is_ok() {
                    assert_eq!(set_count_clone.fetch_add(1, Ordering::SeqCst), 0);
                }
            }
        };

        let key_clone = key;
        let set_count_clone = set_count.clone();
        let test_count_clone = test_count.clone();
        let test_and_set_itr = move |agate: Arc<Agate>| {
            test_count_clone.fetch_add(1, Ordering::SeqCst);

            let mut txn = agate.new_transaction(true);

            let found = {
                let mut it = txn.new_iterator(&IteratorOptions::default());
                it.seek(&key_clone);
                it.valid()
            };

            if !found {
                txn.set(key_clone.clone(), Bytes::from("AA")).unwrap();
                if txn.commit().is_ok() {
                    assert_eq!(set_count_clone.fetch_add(1, Ordering::SeqCst), 0);
                }
            }
        };

        fn run_test<F>(set_count: Arc<AtomicU8>, test_count: Arc<AtomicU8>, f: F)
        where
            F: FnOnce(Arc<Agate>) + Clone + Send + 'static,
        {
            // If num_go is too large, we may not pass the leak sanitizer test.
            // Refer to https://github.com/tikv/agatedb/pull/143 for more details.
            let num_go = 4;

            set_count.store(0, Ordering::SeqCst);
            test_count.store(0, Ordering::SeqCst);

            let mut handles = vec![];

            run_agate_test(None, |agate| {
                for _ in 0..num_go {
                    let agate_clone = agate.clone();
                    let f_clone = f.clone();
                    handles.push(std::thread::spawn(|| {
                        f_clone(agate_clone);
                    }));
                }
            });

            handles
                .into_iter()
                .for_each(|handle| handle.join().unwrap());

            assert_eq!(test_count.load(Ordering::SeqCst), num_go);
            assert_eq!(set_count.load(Ordering::SeqCst), 1);
        }

        run_test(set_count.clone(), test_count.clone(), test_and_set);
        run_test(set_count, test_count, test_and_set_itr);
    }
}
