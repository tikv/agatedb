pub mod meta {
    include!(concat!(env!("OUT_DIR"), "/meta.rs"));

    impl Checksum {
        pub fn merge_from_bytes(&self, _: &Checksum) {
            unimplemented!();
        }
    }
}
