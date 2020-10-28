use crate::value::Value;
use bytes::Bytes;

pub trait AgateIterator {
    fn next(&mut self);
    fn rewind(&mut self);
    fn seek(&mut self, key: &Bytes);
    fn key(&self) -> &[u8];
    fn value(&self) -> Value;
    fn valid(&self) -> bool;
}
