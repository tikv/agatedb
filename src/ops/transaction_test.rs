use crate::Error;

mod managed_db {
    use crate::{
        db::tests::{generate_test_agate_options, run_agate_test, with_payload},
        entry::Entry,
        AgateOptions,
    };
    use bytes::{Bytes, BytesMut};

    use super::*;

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

    macro_rules! assert_bytes_eq {
        ($left:expr, $right:expr) => {
            assert_eq!(
                Bytes::copy_from_slice($left),
                Bytes::copy_from_slice($right)
            )
        };
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
