use std::io;
use std::result;

use thiserror::Error;
use std::sync::PoisonError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Configuration: {0}")]
    Config(String),
    #[error("IO error: {0}")]
    Io(#[source] Box<io::Error>),
    #[error("Empty key")]
    EmptyKey,
    #[error("{0}")]
    TooLong(String),
    #[error("Invalid checksum")]
    InvalidChecksum(String),
    #[error("Invalid filename")]
    InvalidFilename(String),
    #[error("Invalid prost data: {0}")]
    Decode(#[source] Box<prost::DecodeError>),
    #[error("Invalid data: {0}")]
    VarDecode(&'static str),
    #[error("{0}")]
    TableRead(String),
    #[error("Database Closed")]
    DBClosed,
    #[error("Lock Poison")]
    PoisonError(String)
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Error {
        Error::Io(Box::new(e))
    }
}

impl From<prost::DecodeError> for Error {
    #[inline]
    fn from(e: prost::DecodeError) -> Error {
        Error::Decode(Box::new(e))
    }
}

impl <T: Sized> From<PoisonError<T>> for Error {
    #[inline]
    fn from(e: PoisonError<T>) -> Error {
        Error::PoisonError(e.to_string())
    }
}

pub type Result<T> = result::Result<T, Error>;
