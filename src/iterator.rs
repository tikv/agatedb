use bytes::Bytes;

use crate::value::VALUE_DELETE;

pub enum PrefetchStatus {
    No,
    Prefetched,
}

pub struct Item {
    pub(crate) key: Bytes,
    pub(crate) vptr: Bytes,
    pub(crate) value: Bytes,
    pub(crate) version: u64,
    pub(crate) expires_at: u64,

    pub(crate) status: PrefetchStatus,

    pub(crate) meta: u8,
    pub(crate) user_meta: u8,
}

pub fn is_deleted_or_expired(meta: u8, expires_at: u64) -> bool {
    if meta & crate::value::VALUE_DELETE != 0 {
        return true;
    }
    if expires_at == 0 {
        return false;
    }
    expires_at <= crate::util::unix_time()
}
