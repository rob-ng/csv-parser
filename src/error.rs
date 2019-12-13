use std::fmt::{self, Debug, Display};

#[derive(Debug)]
pub struct Error {
    line: usize,
    col: usize,
    kind: ErrorKind,
}

impl Error {
    pub fn new(line: usize, col: usize, kind: ErrorKind) -> Self {
        Error { line, col, kind }
    }
}

#[derive(Debug)]
pub enum ErrorKind {
    BadField(String),
    UnequalNumFields { expected_num: usize, num: usize },
    Io(std::io::Error),
    Utf8(std::str::Utf8Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error {
                line,
                col,
                kind: ErrorKind::BadField(msg),
            } => write!(
                f,
                "Field is malformed: {} (line: {}, column: {})",
                msg, line, col
            ),
            Error {
                line,
                kind: ErrorKind::UnequalNumFields { expected_num, num },
                ..
            } => {
                let relation = if num < expected_num {
                    "too few"
                } else {
                    "too many"
                };
                write!(
                    f,
                    "Record has {} fields: {} vs {} (line: {})",
                    relation, num, expected_num, line
                )
            }
            Error {
                line,
                kind: ErrorKind::Io(err),
                ..
            } => write!(f, "Problem reading CSV: {} (line: {})", err, line),
            Error {
                line,
                col,
                kind: ErrorKind::Utf8(err),
            } => write!(
                f,
                "CSV contains invalid UTF-8: {} (line: {}, column: {})",
                err, line, col
            ),
        }
    }
}

impl From<std::io::Error> for ErrorKind {
    fn from(e: std::io::Error) -> Self {
        ErrorKind::Io(e)
    }
}

impl From<std::str::Utf8Error> for ErrorKind {
    fn from(e: std::str::Utf8Error) -> Self {
        ErrorKind::Utf8(e)
    }
}
