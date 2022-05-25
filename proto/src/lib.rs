#[allow(clippy::derive_partial_eq_without_eq)]
pub mod meta {
    include!(concat!(env!("OUT_DIR"), "/meta.rs"));
}
