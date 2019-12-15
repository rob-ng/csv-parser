use crate::error::{Error, ErrorKind};
use std::io::{BufRead, BufReader, Read};
use std::ops::Range;

type Result<T> = std::result::Result<T, ErrorKind>;

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

#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) columns: Option<Vec<String>>,
    pub(crate) max_record_size: usize,
    pub(crate) newline: Vec<u8>,
    pub(crate) quote: u8,
    pub(crate) separator: u8,
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
            columns: None,
            max_record_size: 4096,
            newline: vec![b'\n'],
            quote: b'"',
            separator: b',',
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

type OnReadLine<R> = fn(
    line_reader: &mut CsvReader<R>,
    curr_line_read: Option<Result<RecordBuffer>>,
) -> Option<Result<RecordBuffer>>;

type OnRecord = fn(
    curr_record: Option<Result<Record>>,
    columns: &mut Option<Vec<String>>,
) -> Option<Result<Record>>;

type FieldParser<Slf> = fn(
    &mut Slf,
    record_buf: &mut RecordBuffer,
    start: usize,
) -> Option<Result<(Range<usize>, usize)>>;

/// Iterator over CSV records.
pub struct Records<R> {
    /// CSV line reader.
    csv_reader: CsvReader<R>,
    /// Column names, if any.
    columns: Option<Vec<String>>,
    /// Configuration settings.
    config: Config,
    /// Callback(s) to perform after a line is read.
    on_read_line: Vec<OnReadLine<R>>,
    /// Callback(s) to perform after a record is created but before it is returned.
    on_record: Vec<OnRecord>,
    /// Method for parsing fields.
    parse_field: FieldParser<Self>,
}

impl<R> Records<R>
where
    R: Read,
{
    pub(crate) fn new(csv: R, config: Config) -> Self {
        let csv_reader = CsvReader::new(csv, &config.newline, config.max_record_size);

        let on_read_line = {
            let mut on_read_line: Vec<OnReadLine<R>> = vec![];
            if config.should_skip_empty_rows {
                on_read_line.push(Self::on_read_line_skip_empty_lines);
            }
            on_read_line
        };

        let on_record = {
            let mut on_record: Vec<OnRecord> = vec![];
            if config.should_relax_column_count_less {
                on_record.push(Self::on_record_relax_columns_less);
            };
            if config.should_relax_column_count_more {
                on_record.push(Self::on_record_relax_columns_more);
            };
            on_record.push(if config.should_detect_columns {
                Self::on_record_detect_columns
            } else {
                Self::on_record_index_columns
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
            config,
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
        while end < record_buf.len() {
            if end >= record_buf.len_sans_newline() {
                old_buf_len = record_buf.len();
                match record_buf.append_line(&mut self.csv_reader) {
                    Some(Ok(())) => end = old_buf_len,
                    Some(Err(e)) => return Err(e),
                    None => break,
                };
            }

            if record_buf.get_unchecked(end) == self.config.quote {
                let next_index = end + 1;
                match record_buf.get(next_index) {
                    Some(&c) if c == self.config.quote => {
                        // Remove duplicate leaving only escaped quote in buffer.
                        record_buf.remove(next_index);
                    }
                    _ => return Ok((start..end, next_index)),
                }
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
        // Using while-loop here is faster than iteration in benchmarks. Probably has to due with overhead of creating iterators for each record.
        let mut end = start;
        while end < record_buf.len_sans_newline() {
            match record_buf.get_unchecked(end) {
                byte if byte == self.config.separator => return Ok((start..end, end)),
                byte if byte == self.config.quote => {
                    return Err(ErrorKind::BadField {
                        col: end,
                        msg: format!(
                            "Unquoted fields cannot contain quote character: `{}`",
                            self.config.quote
                        ),
                    })
                }
                _ => end += 1,
            }
        }

        Ok((start..end, end))
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

macro_rules! parse_field {
    (left, $self:expr, $record_buf:expr, $start:expr) => {{
        let first_byte = $record_buf.get($start)?;
        if first_byte == &$self.config.quote {
            Some($self.quote($record_buf, $start).and_then(|(bounds, end)| {
                match $record_buf.get(end) {
                    Some(&c) if c == $self.config.separator => Ok((bounds, end)),
                    Some(&_c) if end >= $record_buf.len_sans_newline() => Ok((bounds, end)),
                    None => Ok((bounds, end)),
                    _ => Err(ErrorKind::BadField {
                        col: end,
                        msg: String::from("Quoted fields cannot contain trailing unquoted values"),
                    }),
                }
            }))
        } else {
            Some($self.text($record_buf, $start))
        }
    }};

    (right, $self:expr, $record_buf:expr, $start:expr) => {{
        let first_byte = $record_buf.get($start)?;
        if first_byte == &$self.config.quote {
            Some($self.quote($record_buf, $start).and_then(|(bounds, end)| {
                match $record_buf.get(end) {
                    Some(&c) if c == $self.config.separator => return Ok((bounds, end)),
                    Some(&_c) if end >= $record_buf.len_sans_newline() => return Ok((bounds, end)),
                    None => return Ok((bounds, end)),
                    Some(_) => (),
                };
                let end = parse_field!(trim_start, $record_buf, end);
                match $record_buf.get(end) {
                    Some(&c) if c == $self.config.separator => return Ok((bounds, end)),
                    Some(&_c) if end >= $record_buf.len_sans_newline() => return Ok((bounds, end)),
                    None => return Ok((bounds, end)),
                    _ => Err(ErrorKind::BadField {
                        col: end,
                        msg: String::from("Quoted fields cannot contain trailing unquoted values"),
                    }),
                }
            }))
        } else {
            Some($self.text($record_buf, $start).map(|(mut bounds, end)| {
                let buf_segment = &$record_buf.as_str()[bounds.clone()];
                let trimmed = buf_segment.trim_end().as_bytes();
                bounds.end = bounds.start + trimmed.len();
                (bounds, end)
            }))
        }
    }};

    (trim_start, $record_buf:expr, $start:expr) => {{
        let buf_segment = &$record_buf.as_str()[$start..];
        let trimmed = buf_segment.trim_start().as_bytes();
        $record_buf.len() - trimmed.len()
    }};
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
        parse_field!(left, self, record_buf, start)
    }

    fn parse_field_ltrim(
        &mut self,
        record_buf: &mut RecordBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>> {
        let start = parse_field!(trim_start, record_buf, start);
        parse_field!(left, self, record_buf, start)
    }

    fn parse_field_rtrim(
        &mut self,
        record_buf: &mut RecordBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>> {
        parse_field!(right, self, record_buf, start)
    }

    fn parse_field_trim(
        &mut self,
        record_buf: &mut RecordBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>> {
        let start = parse_field!(trim_start, record_buf, start);
        parse_field!(right, self, record_buf, start)
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

    // 20% improvement from inlining this. No idea why.
    #[inline]
    fn get_unchecked(&self, index: usize) -> u8 {
        self.buf[index]
    }

    // 3.5% improvement from inlining this.
    #[inline]
    fn len(&self) -> usize {
        self.buf.len()
    }

    // 20% improvement from inlining this. No idea why.
    #[inline]
    fn len_sans_newline(&self) -> usize {
        self.buf.len() - self.len_from_trailing_newline
    }

    // Negligible improvement from inlining this.
    #[inline]
    fn remove(&mut self, index: usize) {
        self.buf.remove(index);
    }
}

struct CsvReader<R> {
    last_lines_capacity: usize,
    line_count: usize,
    max_record_size: usize,
    newline: Vec<u8>,
    reader: BufReader<R>,
}

impl<R> CsvReader<R>
where
    R: Read,
{
    fn new(reader: R, newline: &[u8], max_record_size: usize) -> Self {
        CsvReader {
            last_lines_capacity: 0,
            line_count: 0,
            max_record_size,
            newline: newline.to_vec(),
            reader: BufReader::new(reader),
        }
    }

    fn read_line(&mut self, buf: &mut Vec<u8>) -> Option<Result<usize>> {
        self.line_count += 1;
        let last = self
            .newline
            .last()
            .expect("Newline terminator cannot be empty");
        // We allow 1 more byte than the limit to indicate the limit has been eclipsed.
        let read_limit = if buf.len() > self.max_record_size {
            0
        } else {
            self.max_record_size - buf.len() + 1
        };
        let len_from_trailing_newline = loop {
            match self
                .reader
                .by_ref()
                .take(read_limit as u64)
                .read_until(*last, buf)
            {
                Ok(0) => {
                    if buf.is_empty() {
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
        if buf.len() >= read_limit {
            return Some(Err(ErrorKind::RecordTooLarge {
                max_record_size: self.max_record_size,
            }));
        }
        Some(Ok(len_from_trailing_newline))
    }
}
