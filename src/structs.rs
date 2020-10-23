use bytes::Bytes;

pub struct Entry {
    pub key: Bytes,
    pub value: Bytes,
    pub expires_at: u64,
    pub version: u64,
    pub offset: u32,
    pub user_meta: u8,
    pub meta: u8,
    hlen: usize 
}

impl Entry {
    pub fn new(key: Bytes, value: Bytes, expires_at: u64, version: u64, user_meta: u8, meta: u8) -> Self {
        Self {
            key,
            value,
            expires_at,
            version,
            user_meta,
            meta,
            hlen: 0,
            offset: 0
        }
    }
}