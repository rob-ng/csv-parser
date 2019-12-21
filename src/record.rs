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
    /// Contents of record.
    buf: String,
    /// Locations of fields within `buf`.
    field_bounds: Vec<Range<usize>>,
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
    pub(crate) fn new(buf: String, field_bounds: Vec<Range<usize>>) -> Self {
        Self { buf, field_bounds }
    }

    pub(crate) fn num_fields(&self) -> usize {
        self.field_bounds.len()
    }

    pub(crate) fn set_num_fields(&mut self, num_fields: usize) {
        self.field_bounds.resize(num_fields, 0..0);
    }

    /// Returns record's fields as strings.
    pub fn fields(&self) -> Vec<&str> {
        self.field_bounds
            .iter()
            .map(|bounds| &self.buf[bounds.clone()])
            .collect()
    }
}
