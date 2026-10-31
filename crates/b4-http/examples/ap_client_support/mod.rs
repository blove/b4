mod application;
mod capture;
mod control;
mod files;
mod gate;
mod process;
use std::fmt;
#[derive(Clone, Copy, Debug)]
pub struct Error(pub &'static str);
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
pub use process::{run, Args};
