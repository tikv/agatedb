use crate::{Error, Result};

mod managed_db {
    use crate::{db::tests::with_agate_test, entry::Entry};
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_on_disk() {
        let key = |i: u64| Bytes::from(format!("key-{:02}", i));
        let value = |i: u64| Bytes::from(format!("val-{}", i));

        with_agate_test(move |agate| {
            agate.view(|_txn| Ok(())).unwrap();

            // write data at t=3
            let mut txn = agate.new_transaction_at(3, true);
            for i in 0..=3 {
                txn.set_entry(Entry::new(key(i), value(i))).unwrap();
            }
            txn.commit_at(3).unwrap();

            // read data at t=2
            let txn = agate.new_transaction_at(2, false);
            for i in 0..=3 {
                assert!(matches!(txn.get(&key(i)), Err(Error::KeyNotFound(()))));
            }
            txn.discard();

            // read data at t=3
            let txn = agate.new_transaction_at(3, false);
            for i in 0..=3 {
                let item = txn.get(&key(i)).unwrap();
                assert_eq!(item.version, 3);
                assert_eq!(item.value, value(i));
            }
            txn.discard();

            // write data at t=7
            let mut txn = agate.new_transaction_at(6, true);
            for i in 0..=7 {
                if let Err(_) = txn.get(&key(i)) {
                    txn.set_entry(Entry::new(key(i), value(i))).unwrap();
                }
            }
            txn.commit_at(7).unwrap();

            // read data at t=9
            let txn = agate.new_transaction_at(9, false);
            for i in 0..=9 {
                let item = txn.get(&key(i));
                if i <= 7 {
                    let item = item.unwrap();
                } else {
                    assert!(matches!(item, Err(Error::KeyNotFound(()))));
                }
            }
            txn.discard();
        })
    }
}
