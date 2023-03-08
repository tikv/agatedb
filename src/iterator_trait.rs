use bytes::Bytes;
use enum_dispatch::enum_dispatch;

use crate::value::Value;

/// `AgateIterator` defines the interface of all iterators,
/// including `TableIterator`, `MergeIterator` and `ConcatIterator`.
#[enum_dispatch]
pub trait AgateIterator {
    fn next(&mut self);
    fn rewind(&mut self);
    fn seek(&mut self, key: &Bytes);
    fn key(&self) -> &[u8];
    fn value(&self) -> Value;
    fn valid(&self) -> bool;
    fn prev(&mut self);
    fn to_last(&mut self);
}
