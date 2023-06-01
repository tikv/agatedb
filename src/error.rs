use std::{
    io,
    ops::Range,
    result,
    sync::{Arc, PoisonError},
};

use crossbeam_channel::SendError;
use thiserror::Error;

use crate::value::ValuePointer;

#[derive(Debug, Error, Clone)]
pub enum Error {
    #[error("Invalid Configuration: {0}")]
    Config(String),
    #[error("IO error: {0}")]
    Io(#[source] Arc<io::Error>),
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
    #[error("Invalid VP: {vptr:?}, kvlen {kvlen}, {range:?}")]
    InvalidValuePointer {
        vptr: ValuePointer,
        kvlen: usize,
        range: Range<u32>,
    },
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
    #[error("Txn is too big to fit into one request")]
    TxnTooBig,
    #[error("Send error {0}")]
    SendError(String),
    #[error("Key not found")]
    KeyNotFound(()),
    #[error("This transaction has been discarded. Create a new one")]
    DiscardedTxn,
    #[error("No room for write")]
    WriteNoRoom(()),
    #[error("Cannot run value log GC when DB is opened in InMemory mode")]
    ErrGCInMemoryMode,
    #[error("Invalid request")]
    ErrInvalidRequest,
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Error {
        Error::Io(Arc::new(e))
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

impl<T: Sized> From<SendError<T>> for Error {
    #[inline]
    fn from(e: SendError<T>) -> Error {
        Error::SendError(e.to_string())
    }
}

pub type Result<T> = result::Result<T, Error>;
