use std::io;
use std::result;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Invalid Configuration: {}", _0)]
    Config(String),
    #[fail(display = "IO error: {}", _0)]
    Io(#[fail(cause)] io::Error),
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Error {
        Error::Io(e)
    }
}

pub type Result<T> = result::Result<T, Error>;
