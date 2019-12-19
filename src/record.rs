use crate::{Error, Parser};
use std::io::Read;
use std::ops::Range;

/// Iterator of `Record`s read from CSV.
pub struct Records<R> {
    parser: Parser<R>,
}

/// CSV record.
#[derive(Debug)]
pub struct Record {
    /// Contents of the record.
    buf: Vec<u8>,
    // TODO Add methods to avoid directly exposing this field.
    /// Ranges describing locations of fields within `buf`.
    pub(crate) field_bounds: Vec<Range<usize>>,
}

impl<R> Records<R> {
    pub(crate) fn new(parser: Parser<R>) -> Self {
        Self { parser }
    }
}

impl<R> Iterator for Records<R>
where
    R: Read,
{
    type Item = std::result::Result<Record, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.parser.next_record()
    }
}

impl<'a> Record {
    pub(crate) fn new(buf: Vec<u8>, field_bounds: Vec<Range<usize>>) -> Self {
        Self { buf, field_bounds }
    }

    /// Returns record's fields as strings.
    pub fn fields(&self) -> Vec<&str> {
        let buf_as_str = unsafe { std::str::from_utf8_unchecked(&self.buf) };
        self.field_bounds
            .iter()
            .map(|bounds| &buf_as_str[bounds.clone()])
            .collect()
    }
}
