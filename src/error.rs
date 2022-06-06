use std::{io, ops::Range, result, sync::PoisonError};

use thiserror::Error;

use crate::value::ValuePointer;

#[derive(Debug)]
pub struct InvalidValuePointerError {
    pub vptr: ValuePointer,
    pub kvlen: usize,
    pub range: Range<u32>,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Configuration: {0}")]
    Config(String),
    #[error("IO error: {0}")]
    Io(#[source] Box<io::Error>),
    #[error("Empty key")]
    EmptyKey,
    #[error("Too long: {0}")]
    TooLong(String),
    #[error("Invalid checksum")]
    InvalidChecksum(String),
    #[error("Invalid filename")]
    InvalidFilename(String),
    #[error("Prost encode error: {0}")]
    Encode(#[source] Box<prost::EncodeError>),
    #[error("Prost decode error: {0}")]
    Decode(#[source] Box<prost::DecodeError>),
    #[error("Invalid data: {0}")]
    VarDecode(&'static str),
    #[error("Error when reading table: {0}")]
    TableRead(String),
    #[error("Database Closed")]
    DBClosed,
    #[error("Error when reading from log: {0}")]
    LogRead(String),
    #[error("Invalid VP: {0:?}")]
    InvalidValuePointer(Box<InvalidValuePointerError>),
    #[error("Invalid Log Offset: {0} > {1}")]
    InvalidLogOffset(u32, u32),
    #[error("VLog Not Found: id={0}")]
    VlogNotFound(u32),
    #[error("Error when compaction: {0}")]
    CompactionError(String),
    #[error("{0}")]
    CustomError(String),
    #[error("Error when creating file in read-only mode: {0}")]
    ReadOnlyError(String),
    #[error("Lock Poison")]
    PoisonError(String),
    #[error("Join Error")]
    JoinError(String),
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Error {
        Error::Io(Box::new(e))
    }
}

impl From<InvalidValuePointerError> for Error {
    #[inline]
    fn from(e: InvalidValuePointerError) -> Error {
        Error::InvalidValuePointer(Box::new(e))
    }
}

impl From<prost::EncodeError> for Error {
    #[inline]
    fn from(e: prost::EncodeError) -> Error {
        Error::Encode(Box::new(e))
    }
}

impl From<prost::DecodeError> for Error {
    #[inline]
    fn from(e: prost::DecodeError) -> Error {
        Error::Decode(Box::new(e))
    }
}

impl<T: Sized> From<PoisonError<T>> for Error {
    #[inline]
    fn from(e: PoisonError<T>) -> Error {
        Error::PoisonError(e.to_string())
    }
}

pub type Result<T> = result::Result<T, Error>;
