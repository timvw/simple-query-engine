#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    Arrow(arrow2::error::ArrowError),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IO(err)
    }
}

impl From<arrow2::error::ArrowError> for Error {
    fn from(err: arrow2::error::ArrowError) -> Error {
        Error::Arrow(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;