use std::error::Error as StdError;
use std::fmt::{self, Debug, Display};

/// Error that can occur during parsing.
#[derive(Debug)]
pub struct Error {
    /// Line in CSV where error occured.
    line: usize,
    /// Type of error.
    kind: ErrorKind,
}

impl Error {
    pub(crate) fn new(line: usize, kind: ErrorKind) -> Self {
        Error { line, kind }
    }
}

/// Kinds of error that can occur during parsing.
#[derive(Debug)]
pub enum ErrorKind {
    /// Error that occurs due to field being malformed.
    BadField { col: usize, msg: String },
    /// Error that occurs due to record having too few or too many fields.
    UnequalNumFields { expected_num: usize, num: usize },
    /// Error that occurs due to record exceeding max number of bytes.
    RecordTooLarge { max_record_size: usize },
    /// Error that occurs due to problems reading CSV.
    Io(std::io::Error),
    /// Error that occurs when CSV contains non-UTF-8 values.
    Utf8(std::str::Utf8Error),
}

impl StdError for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error {
                line,
                kind: ErrorKind::BadField { col, msg },
            } => write!(
                f,
                "Field is malformed: {} (line: {}, col: {})",
                msg,
                line,
                // Columns, like lines, should start at 1.
                col + 1
            ),
            Error {
                line,
                kind: ErrorKind::UnequalNumFields { expected_num, num },
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
                kind: ErrorKind::RecordTooLarge { max_record_size },
            } => write!(
                f,
                "Record exceeds max record size '{} bytes' (line: {})",
                max_record_size, line
            ),
            Error {
                line,
                kind: ErrorKind::Io(err),
            } => write!(f, "Problem reading CSV: {} (line: {})", err, line),
            Error {
                line,
                kind: ErrorKind::Utf8(err),
            } => write!(f, "CSV contains invalid UTF-8: {} (line: {})", err, line),
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
