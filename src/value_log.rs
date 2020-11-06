use crate::value::Request;
use crate::Result;

pub struct ValueLog {}

impl ValueLog {
    pub fn new() -> Self {
        Self {}
    }

    pub fn write(&self, _requests: &[Request]) -> Result<()> {
        // TODO: implementation
        Ok(())
    }
}
