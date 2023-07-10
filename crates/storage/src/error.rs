use std::io::Error as IOError;

use prost;

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind) -> Error {
        Error { kind }
    }

    pub(crate) fn generic<E: std::error::Error>(err: E) -> Error {
        Error {
            kind: ErrorKind::StorageError(err.to_string()),
        }
    }

    pub(crate) fn any<E: ToString>(msg: E) -> Error {
        Error {
            kind: ErrorKind::StorageError(msg.to_string()),
        }
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }
}

impl From<IOError> for Error {
    fn from(value: IOError) -> Self {
        Error::new(ErrorKind::IOError(value))
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    StorageError(String),
    MissingFile(String),
    IOError(IOError),
    InvalidTrailer(String),
    InvalidMajorVersion(u8),
    ProtoDecodeError(prost::DecodeError),
    UnsupportedFile(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self.kind {
            ErrorKind::MissingFile(ref path) => write!(f, "File not found: {}", path),
            ErrorKind::StorageError(ref msg) => {
                write!(f, "storage error: {}", msg)
            }
            ErrorKind::IOError(io_error) => {
                write!(f, "IO Error: {}", io_error)
            }
            ErrorKind::InvalidTrailer(msg) => {
                write!(f, "Invalid trailer: {}", msg)
            }
            ErrorKind::InvalidMajorVersion(version) => {
                write!(f, "Invalid major version: {}", version)
            },
            ErrorKind::ProtoDecodeError(decode_error) => {
                write!(f, "Protobuf decode error: {}", decode_error)
            },
            ErrorKind::UnsupportedFile(unsupported_msg) => {
                write!(f, "Unsupported file: {}", unsupported_msg)
            }
        }
    }
}
