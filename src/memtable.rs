pub struct Memtable {
    data: Vec<u8>,
}

impl Memtable {
    pub fn with_capacity(size: usize) -> Memtable {
        Memtable {
            data: Vec::with_capacity(size),
        }
    }
}
