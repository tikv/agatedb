use super::transaction::Transaction;

mod managed_db {
    use crate::{db::tests::with_agate_test, entry::Entry};
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_on_disk() {
        let key = |i: u64| Bytes::from(format!("key-{:02}", i));
        let value = |i: u64| Bytes::from(format!("val-{}", i));

        with_agate_test(move |agate| {
            agate.view(|txn| Ok(())).unwrap();
            let mut txn = agate.new_transaction_at(3, true);
            for i in 0..=3 {
                txn.set_entry(Entry::new(key(i), value(i))).unwrap();
            }
        })
    }
}
