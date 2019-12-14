use crate::error::{Error, ErrorKind};
use std::io::{BufRead, BufReader, Read};
use std::ops::Range;

type Result<T> = std::result::Result<T, ErrorKind>;

#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) newline: Vec<u8>,
    pub(crate) quote: u8,
    pub(crate) separator: u8,
    pub(crate) columns: Option<Vec<String>>,
    pub(crate) should_detect_columns: bool,
    pub(crate) should_ltrim_fields: bool,
    pub(crate) should_rtrim_fields: bool,
    pub(crate) should_relax_column_count_less: bool,
    pub(crate) should_relax_column_count_more: bool,
    pub(crate) should_skip_empty_rows: bool,
    pub(crate) should_skip_rows_with_error: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            newline: vec![b'\n'],
            quote: b'"',
            separator: b',',
            columns: None,
            should_detect_columns: false,
            should_ltrim_fields: true,
            should_rtrim_fields: true,
            should_relax_column_count_less: false,
            should_relax_column_count_more: false,
            should_skip_empty_rows: true,
            should_skip_rows_with_error: false,
        }
    }
}

/// Iterator over CSV records.
pub struct Records<R> {
    /// CSV line reader.
    csv_reader: CsvReader<R>,
    /// Column names, if any.
    columns: Option<Vec<String>>,
    /// Configuration settings.
    config: Config,
    /// Callback(s) to perform after a line is read.
    on_read_line: Vec<
        fn(
            line_reader: &mut CsvReader<R>,
            curr_line_read: Option<Result<RecordBuffer>>,
        ) -> Option<Result<RecordBuffer>>,
    >,
    /// Callback(s) to perform after a record is created but before it is returned.
    on_record: Vec<
        fn(
            curr_record: Option<Result<Record>>,
            columns: &mut Option<Vec<String>>,
        ) -> Option<Result<Record>>,
    >,
    /// Method for parsing fields.
    parse_field: fn(
        &mut Self,
        record_buf: &mut RecordBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>>,
}

impl<R> Records<R>
where
    R: Read,
{
    pub(crate) fn new(csv: R, config: Config) -> Self {
        let csv_reader = CsvReader::new(csv, &config.newline);

        let on_read_line = {
            let mut on_read_line: Vec<
                fn(&mut CsvReader<R>, Option<Result<RecordBuffer>>) -> Option<Result<RecordBuffer>>,
            > = vec![];
            if config.should_skip_empty_rows {
                on_read_line.push(Self::on_read_line_skip_empty_lines);
            }
            on_read_line
        };

        let on_record = {
            let mut on_record: Vec<
                fn(
                    curr_record: Option<Result<Record>>,
                    columns: &mut Option<Vec<String>>,
                ) -> Option<Result<Record>>,
            > = vec![];
            if config.should_relax_column_count_less {
                on_record.push(Self::on_record_relax_columns_less);
            };
            if config.should_relax_column_count_more {
                on_record.push(Self::on_record_relax_columns_more);
            };
            on_record.push(match config.should_detect_columns {
                true => Self::on_record_detect_columns,
                false => Self::on_record_index_columns,
            });
            if config.should_skip_rows_with_error {
                on_record.push(Self::on_record_skip_malformed);
            }
            on_record
        };

        let parse_field = match (config.should_ltrim_fields, config.should_rtrim_fields) {
            (false, false) => Self::parse_field,
            (true, false) => Self::parse_field_ltrim,
            (false, true) => Self::parse_field_rtrim,
            (true, true) => Self::parse_field_trim,
        };

        Records {
            csv_reader,
            columns: config.columns.clone(),
            config: config,
            on_read_line,
            on_record,
            parse_field,
        }
    }

    fn record(&mut self, mut record_buf: RecordBuffer) -> Option<Result<Record>> {
        let expected_num_fields = self.columns.as_ref().map_or(1, |cols| cols.len());
        let mut field_bounds = Vec::with_capacity(expected_num_fields);

        let mut start = 0;
        loop {
            let (bounds, end) = match (self.parse_field)(self, &mut record_buf, start) {
                Some(Ok(result)) => result,
                Some(Err(e)) => return Some(Err(e)),
                None => break,
            };

            start = end;
            field_bounds.push(bounds);

            match record_buf.get(start) {
                Some(&c) if c == self.config.separator => {
                    start += 1;
                    continue;
                }
                Some(&_c) if start == record_buf.len_sans_newline() => break,
                Some(&_c) => unreachable!(),
                None => break,
            }
        }

        let record = Record::new(record_buf, field_bounds);

        Some(Ok(record))
    }

    fn quote(
        &mut self,
        record_buf: &mut RecordBuffer,
        start: usize,
    ) -> Result<(Range<usize>, usize)> {
        // Start at first byte after quotation byte.
        let start = start + 1;

        let mut old_buf_len = record_buf.len();
        let mut end = start;
        loop {
            if end >= record_buf.len_sans_newline() {
                old_buf_len = record_buf.len();
                match record_buf.append_line(&mut self.csv_reader) {
                    Some(Ok(())) => end = old_buf_len,
                    Some(Err(e)) => return Err(e),
                    None => break,
                };
            }

            match record_buf.get(end) {
                Some(&c) if c == self.config.quote => {
                    let next_index = end + 1;
                    match record_buf.get(next_index) {
                        Some(&c) if c == self.config.quote => {
                            // Remove duplicate leaving only escaped quote in buffer.
                            record_buf.remove(next_index);
                        }
                        _ => return Ok((start..end, next_index)),
                    }
                }
                Some(_c) => (),
                None => break,
            }

            end += 1
        }

        Err(ErrorKind::BadField {
            col: end - old_buf_len,
            msg: String::from("Quoted field is missing closing quotation"),
        })
    }

    fn text(
        &mut self,
        record_buf: &mut RecordBuffer,
        start: usize,
    ) -> Result<(Range<usize>, usize)> {
        let mut end = start;

        while end < record_buf.len_sans_newline() {
            match record_buf.get(end) {
                Some(&c) if c == self.config.quote => {
                    return Err(ErrorKind::BadField {
                        col: end,
                        msg: format!(
                            "Unquoted fields cannot contain quote character: `{}`",
                            self.config.quote
                        ),
                    });
                }
                Some(&c) if c != self.config.separator => end += 1,
                _ => break,
            }
        }

        Ok((start..end, end))
    }
}

impl<R> Records<R>
where
    R: Read,
{
    fn parse_field(
        &mut self,
        record_buf: &mut RecordBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>> {
        let first_byte = record_buf.get(start)?;
        if first_byte == &self.config.quote {
            Some(self.quote(record_buf, start).and_then(
                |(bounds, end)| match record_buf.get(end) {
                    Some(&c)
                        if c != self.config.separator && end < record_buf.len_sans_newline() =>
                    {
                        return Err(ErrorKind::BadField {
                            col: end,
                            msg: String::from(
                                "Quoted fields cannot contain trailing unquoted values",
                            ),
                        });
                    }
                    _ => Ok((bounds, end)),
                },
            ))
        } else {
            Some(self.text(record_buf, start))
        }
    }

    fn parse_field_ltrim(
        &mut self,
        record_buf: &mut RecordBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>> {
        let start = Self::trim_start(record_buf, start);
        self.parse_field(record_buf, start)
    }

    fn parse_field_rtrim(
        &mut self,
        record_buf: &mut RecordBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>> {
        let first_byte = record_buf.get(start)?;
        if first_byte == &self.config.quote {
            Some(
                self.quote(record_buf, start)
                    .map(|(bounds, end)| {
                        let end = Self::trim_start(record_buf, end);
                        (bounds, end)
                    })
                    .and_then(|(bounds, end)| match record_buf.get(end) {
                        Some(&c)
                            if c != self.config.separator
                                && end < record_buf.len_sans_newline() =>
                        {
                            return Err(ErrorKind::BadField {
                                col: end,
                                msg: String::from(
                                    "Quoted fields cannot contain trailing unquoted values",
                                ),
                            });
                        }
                        _ => Ok((bounds, end)),
                    }),
            )
        } else {
            Some(self.text(record_buf, start).map(|(bounds, end)| {
                let bounds = Self::trim_end(&record_buf, bounds);
                (bounds, end)
            }))
        }
    }

    fn parse_field_trim(
        &mut self,
        record_buf: &mut RecordBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>> {
        let start = Self::trim_start(&record_buf, start);
        self.parse_field_rtrim(record_buf, start)
    }

    // Negligible improvement from inlining.
    #[inline]
    fn trim_start(record_buf: &RecordBuffer, start: usize) -> usize {
        let buf_segment = &record_buf.as_str()[start..];
        let trimmed = buf_segment.trim_start().as_bytes();
        let len_trimmed = record_buf.len() - start - trimmed.len();
        start + len_trimmed
    }

    // Negligible improvement from inlining.
    #[inline]
    fn trim_end(record_buf: &RecordBuffer, field_bounds: Range<usize>) -> Range<usize> {
        let buf_segment = &record_buf.as_str()[field_bounds.clone()];
        let trimmed = buf_segment.trim_end().as_bytes();
        let len_trimmed = field_bounds.end - field_bounds.start - trimmed.len();
        let end = field_bounds.end - len_trimmed;
        field_bounds.start..end
    }
}

impl<R> Records<R>
where
    R: Read,
{
    fn on_record_relax_columns_less(
        mut record: Option<Result<Record>>,
        columns: &mut Option<Vec<String>>,
    ) -> Option<Result<Record>> {
        match (&mut record, &columns) {
            (Some(Ok(record)), Some(columns)) if record.field_bounds.len() < columns.len() => {
                record.field_bounds.resize(columns.len(), 0..0);
            }
            _ => (),
        };
        record
    }

    fn on_record_relax_columns_more(
        mut record: Option<Result<Record>>,
        columns: &mut Option<Vec<String>>,
    ) -> Option<Result<Record>> {
        match (&mut record, &columns) {
            (Some(Ok(record)), Some(columns)) if record.field_bounds.len() > columns.len() => {
                record.field_bounds.truncate(columns.len());
            }
            _ => (),
        };
        record
    }

    fn on_record_index_columns(
        record: Option<Result<Record>>,
        columns: &mut Option<Vec<String>>,
    ) -> Option<Result<Record>> {
        if let Some(Ok(record)) = record {
            match columns {
                Some(columns) if columns.len() == record.field_bounds.len() => Some(Ok(record)),
                Some(columns) => Some(Err(ErrorKind::UnequalNumFields {
                    expected_num: columns.len(),
                    num: record.field_bounds.len(),
                })),
                None => {
                    let standin_columns = vec![String::from(""); record.field_bounds.len()];
                    columns.replace(standin_columns);
                    Some(Ok(record))
                }
            }
        } else {
            record
        }
    }

    fn on_record_detect_columns(
        record: Option<Result<Record>>,
        columns: &mut Option<Vec<String>>,
    ) -> Option<Result<Record>> {
        if let Some(Ok(record)) = record {
            match columns {
                Some(columns) if columns.len() == record.field_bounds.len() => Some(Ok(record)),
                Some(columns) => Some(Err(ErrorKind::UnequalNumFields {
                    expected_num: columns.len(),
                    num: record.field_bounds.len(),
                })),
                None => {
                    let found_columns = record.fields().iter().map(|f| f.to_string()).collect();
                    columns.replace(found_columns);
                    None
                }
            }
        } else {
            record
        }
    }

    fn on_record_skip_malformed(
        record: Option<Result<Record>>,
        _columns: &mut Option<Vec<String>>,
    ) -> Option<Result<Record>> {
        match &record {
            Some(Err(ErrorKind::BadField { .. }))
            | Some(Err(ErrorKind::UnequalNumFields { .. })) => None,
            _ => record,
        }
    }
}

impl<R> Records<R>
where
    R: Read,
{
    fn on_read_line_skip_empty_lines(
        reader: &mut CsvReader<R>,
        record_buf: Option<Result<RecordBuffer>>,
    ) -> Option<Result<RecordBuffer>> {
        let mut curr_record_buf = record_buf;
        while let Some(Ok(record_buf)) = &curr_record_buf {
            let line = record_buf.as_str();
            if line.trim().is_empty() {
                curr_record_buf = RecordBuffer::new(reader);
            } else {
                break;
            }
        }
        curr_record_buf
    }
}

impl<R> Iterator for Records<R>
where
    R: Read,
{
    type Item = std::result::Result<Record, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        let next_record_buf = RecordBuffer::new(&mut self.csv_reader);
        let line_reader = &mut self.csv_reader;
        let record_buf = match self
            .on_read_line
            .iter()
            .fold(next_record_buf, |line, on_read_line| {
                on_read_line(line_reader, line)
            }) {
            Some(Ok(buf)) => buf,
            Some(Err(e)) => return Some(Err(Error::new(self.csv_reader.line_count, e))),
            None => return None,
        };

        let next_record = self.record(record_buf);
        let columns = &mut self.columns;
        match self
            .on_record
            .iter()
            .fold(next_record, |record, on_record| on_record(record, columns))
        {
            Some(Ok(record)) => Some(Ok(record)),
            Some(Err(e)) => Some(Err(Error::new(self.csv_reader.line_count, e))),
            None => self.next(),
        }
    }
}

/// Represents a CSV record.
#[derive(Debug)]
pub struct Record {
    /// Contents of the record.
    buf: RecordBuffer,
    /// Ranges describing locations of fields within `buf`.
    field_bounds: Vec<Range<usize>>,
}

impl<'a> Record {
    fn new(buf: RecordBuffer, field_bounds: Vec<Range<usize>>) -> Self {
        Record { buf, field_bounds }
    }

    /// Returns record's fields as strings.
    pub fn fields(&self) -> Vec<&str> {
        self.field_bounds
            .iter()
            .map(|bounds| &self.buf.as_str()[bounds.clone()])
            .collect()
    }
}

#[derive(Debug)]
struct RecordBuffer {
    buf: Vec<u8>,
    len_from_trailing_newline: usize,
    line_count: usize,
}

impl RecordBuffer {
    fn new<R: Read>(reader: &mut CsvReader<R>) -> Option<Result<Self>> {
        let mut buf = Vec::with_capacity(reader.last_lines_capacity);
        match reader.read_line(&mut buf) {
            Some(Ok(len_from_trailing_newline)) => match std::str::from_utf8(&buf) {
                Ok(_) => Some(Ok(RecordBuffer {
                    buf,
                    len_from_trailing_newline,
                    line_count: 1,
                })),
                Err(e) => Some(Err(ErrorKind::Utf8(e))),
            },
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }

    fn append_line<R: Read>(&mut self, reader: &mut CsvReader<R>) -> Option<Result<()>> {
        let initial_buf_len = self.buf.len();
        match reader.read_line(&mut self.buf) {
            Some(Ok(len_from_trailing_newline)) => {
                self.len_from_trailing_newline = len_from_trailing_newline;
                match std::str::from_utf8(&self.buf[initial_buf_len..]) {
                    Ok(_) => Some(Ok(())),
                    Err(e) => Some(Err(ErrorKind::Utf8(e))),
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }

    fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.buf) }
    }

    // 20% improvement from inlining this. No idea why.
    #[inline]
    fn get(&self, index: usize) -> Option<&u8> {
        self.buf.get(index)
    }

    // 3.5% improvement from inlining this.
    #[inline]
    fn len(&self) -> usize {
        self.buf.len()
    }

    // 20% improvement from inlining this. No idea why.
    #[inline]
    fn len_sans_newline(&self) -> usize {
        self.len() - self.len_from_trailing_newline
    }

    // Negligible improvement from inlining this.
    #[inline]
    fn remove(&mut self, index: usize) {
        self.buf.remove(index);
    }
}

struct CsvReader<R> {
    reader: BufReader<R>,
    newline: Vec<u8>,
    line_count: usize,
    last_lines_capacity: usize,
}

impl<R> CsvReader<R>
where
    R: Read,
{
    fn new(reader: R, newline: &[u8]) -> Self {
        CsvReader {
            reader: BufReader::new(reader),
            newline: Vec::from(newline),
            line_count: 0,
            last_lines_capacity: 0,
        }
    }

    fn read_line(&mut self, buf: &mut Vec<u8>) -> Option<Result<usize>> {
        self.line_count += 1;
        let last = self
            .newline
            .last()
            .expect("Newline terminator cannot be empty");
        let len_from_trailing_newline = loop {
            match self.reader.read_until(*last, buf) {
                Ok(0) => {
                    if buf.len() == 0 {
                        return None;
                    } else {
                        break 0;
                    }
                }
                Ok(_n) => {
                    if buf.ends_with(&self.newline) {
                        break self.newline.len();
                    } else {
                        continue;
                    }
                }
                Err(e) => return Some(Err(ErrorKind::Io(e))),
            }
        };
        Some(Ok(len_from_trailing_newline))
    }
}
