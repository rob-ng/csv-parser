use crate::{Error, ErrorKind, Record, Records};
use std::io::{BufRead, BufReader, Read};
use std::ops::Range;

/// Configurer and creator of `Parser`s.
#[derive(Default)]
pub struct ParserBuilder {
    config: Config,
}

type Result<T> = std::result::Result<T, ErrorKind>;

type OnReadLine<R> = fn(
    current_record_buffer: &mut RecordBuffer<R>,
    last_read_attempt: Option<Result<usize>>,
) -> Option<Result<usize>>;

type OnRecord = fn(
    curr_record: Option<Result<Record>>,
    columns: &mut Option<Vec<String>>,
) -> Option<Result<Record>>;

type FieldParser<Parser> = fn(&mut Parser, start: usize) -> Option<Result<(Range<usize>, usize)>>;

/// CSV parser.
pub struct Parser<R> {
    /// Buffer containing contents of current record.
    current_record_buffer: RecordBuffer<R>,
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

macro_rules! config {
    ($name:ident, $field:ident) => {
        pub fn $name(&mut self, value: bool) -> &mut Self {
            self.config.$field = value;
            self
        }
    };

    ($name:ident, $field:ident, $value_type:ty) => {
        pub fn $name(&mut self, value: $value_type) -> &mut Self {
            self.config.$field = value;
            self
        }
    };
}

impl ParserBuilder {
    pub fn new() -> Self {
        Self {
            config: Default::default(),
        }
    }

    /// Sets the escape character to the given byte.
    config!(escape, escape, u8);

    /// Sets maximum record size. Parser longer than this value will result in an error.
    config!(max_record_size, max_record_size, usize);

    /// Sets newline terminator to given bytes.
    pub fn newline(&mut self, newline: &[u8]) -> &mut Self {
        self.config.newline = newline.to_vec();
        self
    }

    /// Sets field separator to given byte.
    config!(separator, separator, u8);

    /// Sets quote marker to given byte.
    config!(quote, quote, u8);

    /// Determines whether fields should be left-trimmed of whitespace.
    config!(ltrim, should_ltrim_fields);

    /// Determines whether fields should be right-trimmed of whitespace.
    config!(rtrim, should_rtrim_fields);

    /// Determines whether fields should be left *and* right trimmed of whitespace.
    pub fn trim(&mut self, should_trim: bool) -> &mut Self {
        self.config.should_ltrim_fields = should_trim;
        self.config.should_rtrim_fields = should_trim;
        self
    }

    /// Determines whether first row should be treated as column names for subsequent rows.
    config!(detect_columns, should_detect_columns);

    /// Sets column names for rows to given values. Overrides `detect_columns`.
    pub fn columns(&mut self, columns: Vec<String>) -> &mut Self {
        self.config.columns.replace(columns);
        self
    }

    /// Determines whether records should be allowed to have fewer fields than the number of columns.
    config!(relax_column_count_less, should_relax_column_count_less);

    /// Determines whether records should be allowed to have more fields than the number of columns.
    config!(relax_column_count_more, should_relax_column_count_more);

    /// Determines whether records should be allowed to have more *or* fewer fields than the number of columns.
    pub fn relax_column_count(&mut self, should_relax: bool) -> &mut Self {
        self.config.should_relax_column_count_less = should_relax;
        self.config.should_relax_column_count_more = should_relax;
        self
    }

    /// Determines whether rows of only whitespace should be skipped. Does not apply to rows in quoted fields.
    config!(skip_empty_rows, should_skip_empty_rows);

    /// Determines whether parsing errors from malformed CSV should be skipped. Other kinds of errors (e.g. IO) are not affected.
    config!(skip_rows_with_error, should_skip_rows_with_error);

    /// Returns a `Parser` based on the given reader.
    pub fn from_reader<R>(&self, csv_source: R) -> Parser<R>
    where
        R: Read,
    {
        let config = self.config.clone();
        Parser::new(csv_source, config)
    }
}

impl<R> Parser<R>
where
    R: Read,
{
    fn new(csv: R, config: Config) -> Self {
        let current_record_buffer = RecordBuffer::new(csv, &config.newline, config.max_record_size);

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

        Parser {
            current_record_buffer,
            columns: config.columns.clone(),
            config,
            on_read_line,
            on_record,
            parse_field,
        }
    }

    pub fn records(self) -> Records<R> {
        Records::new(self)
    }

    pub(crate) fn next_record(&mut self) -> Option<std::result::Result<Record, Error>> {
        let read_attempt = self.current_record_buffer.append_next_line();
        let current_record_buffer = &mut self.current_record_buffer;
        match self
            .on_read_line
            .iter()
            .fold(read_attempt, |line, on_read_line| {
                on_read_line(current_record_buffer, line)
            }) {
            Some(Ok(buf)) => buf,
            Some(Err(e)) => return Some(Err(Error::new(self.current_record_buffer.line_count, e))),
            None => return None,
        };

        let next_record = self.record();
        let columns = &mut self.columns;
        match self
            .on_record
            .iter()
            .fold(next_record, |record, on_record| on_record(record, columns))
        {
            Some(Ok(record)) => Some(Ok(record)),
            Some(Err(e)) => Some(Err(Error::new(self.current_record_buffer.line_count, e))),
            None => {
                self.current_record_buffer.clear();
                self.next_record()
            }
        }
    }

    fn record(&mut self) -> Option<Result<Record>> {
        let expected_num_fields = self.columns.as_ref().map_or(1, |cols| cols.len());
        let mut field_bounds = Vec::with_capacity(expected_num_fields);

        let mut start = 0;
        loop {
            let (bounds, end) = match (self.parse_field)(self, start) {
                Some(Ok(result)) => result,
                Some(Err(e)) => return Some(Err(e)),
                None => break,
            };

            start = end;
            field_bounds.push(bounds);

            if start >= self.current_record_buffer.len_sans_newline() {
                break;
            }

            match self.current_record_buffer.get_unchecked(start) {
                c if c == self.config.separator => start += 1,
                _c => unreachable!(),
            }
        }

        let record_buf = self.current_record_buffer.take_inner();
        let record = Record::new(record_buf, field_bounds);

        Some(Ok(record))
    }

    fn quote(&mut self, start: usize) -> Result<(Range<usize>, usize)> {
        // Start at first byte after quotation byte.
        let start = start + 1;

        let mut end = start;
        loop {
            while end < self.current_record_buffer.len_sans_newline() {
                let curr = self.current_record_buffer.get_unchecked(end);
                let next_index = end + 1;

                if curr == self.config.escape {
                    match self.current_record_buffer.get(next_index) {
                        Some(&c) if c == self.config.quote => {
                            // Remove escape character from buffer leaving only escaped value.
                            self.current_record_buffer.remove(end);
                        }
                        _ if curr == self.config.quote => return Ok((start..end, next_index)),
                        _ => (),
                    }
                } else if curr == self.config.quote {
                    return Ok((start..end, next_index));
                }

                end += 1
            }
            let old_buf_len = self.current_record_buffer.len();
            match self.current_record_buffer.append_next_line() {
                Some(Ok(_nread)) => end = old_buf_len,
                Some(Err(e)) => return Err(e),
                None => {
                    return Err(ErrorKind::BadField {
                        col: end - old_buf_len,
                        msg: String::from("Quoted field is missing closing quotation"),
                    })
                }
            };
        }
    }

    fn text(&mut self, start: usize) -> Result<(Range<usize>, usize)> {
        // Using while-loop here is faster than iteration in benchmarks. Probably has to due with overhead of creating iterators for each record.
        let mut end = start;
        let max = self.current_record_buffer.len_sans_newline();
        while end < max {
            match self.current_record_buffer.get_unchecked(end) {
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

macro_rules! parse_field {
    (left, $self:expr, $start:expr) => {{
        let first_byte = $self.current_record_buffer.get($start)?;
        if first_byte == &$self.config.quote {
            Some($self.quote($start).and_then(|(bounds, end)| {
                match $self.current_record_buffer.get(end) {
                    Some(&c)
                        if c == $self.config.separator
                            || end >= $self.current_record_buffer.len_sans_newline() =>
                    {
                        Ok((bounds, end))
                    }
                    None => Ok((bounds, end)),
                    Some(&_c) => Err(ErrorKind::BadField {
                        col: end,
                        msg: String::from("Quoted fields cannot contain trailing unquoted values"),
                    }),
                }
            }))
        } else {
            Some($self.text($start))
        }
    }};

    (right, $self:expr, $start:expr) => {{
        let first_byte = $self.current_record_buffer.get($start)?;
        if first_byte == &$self.config.quote {
            Some($self.quote($start).and_then(|(bounds, mut end)| {
                if let Some(&c) = $self.current_record_buffer.get(end) {
                    if c != $self.config.separator
                        && end < $self.current_record_buffer.len_sans_newline()
                    {
                        end = parse_field!(trim_start, $self, end);
                    }
                }
                match $self.current_record_buffer.get(end) {
                    Some(&c)
                        if c == $self.config.separator
                            || end >= $self.current_record_buffer.len_sans_newline() =>
                    {
                        return Ok((bounds, end))
                    }
                    None => return Ok((bounds, end)),
                    _ => Err(ErrorKind::BadField {
                        col: end,
                        msg: String::from("Quoted fields cannot contain trailing unquoted values"),
                    }),
                }
            }))
        } else {
            Some($self.text($start).map(|(mut bounds, end)| {
                let buf_segment = &$self.current_record_buffer.as_str()[bounds.clone()];
                let trimmed = buf_segment.trim_end().as_bytes();
                bounds.end = bounds.start + trimmed.len();
                (bounds, end)
            }))
        }
    }};

    (trim_start, $self:expr, $start:expr) => {{
        let buf_segment = &$self.current_record_buffer.as_str()[$start..];
        let trimmed = buf_segment.trim_start().as_bytes();
        $self.current_record_buffer.len() - trimmed.len()
    }};
}

impl<R> Parser<R>
where
    R: Read,
{
    fn parse_field(&mut self, start: usize) -> Option<Result<(Range<usize>, usize)>> {
        parse_field!(left, self, start)
    }

    fn parse_field_ltrim(&mut self, start: usize) -> Option<Result<(Range<usize>, usize)>> {
        let start = parse_field!(trim_start, self, start);
        parse_field!(left, self, start)
    }

    fn parse_field_rtrim(&mut self, start: usize) -> Option<Result<(Range<usize>, usize)>> {
        parse_field!(right, self, start)
    }

    fn parse_field_trim(&mut self, start: usize) -> Option<Result<(Range<usize>, usize)>> {
        let start = parse_field!(trim_start, self, start);
        parse_field!(right, self, start)
    }
}

impl<R> Parser<R>
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

impl<R> Parser<R>
where
    R: Read,
{
    fn on_read_line_skip_empty_lines(
        current_record_buffer: &mut RecordBuffer<R>,
        last_read_attempt: Option<Result<usize>>,
    ) -> Option<Result<usize>> {
        let mut curr_read_attempt = last_read_attempt;
        while let Some(Ok(_bytes_read)) = &curr_read_attempt {
            let line = current_record_buffer.as_str();
            if line.trim().is_empty() {
                current_record_buffer.clear();
                curr_read_attempt = current_record_buffer.append_next_line();
            } else {
                break;
            }
        }
        curr_read_attempt
    }
}

///////////////////////////////////////////////////////////////////////////////
/// Private
///////////////////////////////////////////////////////////////////////////////
#[derive(Clone)]
struct Config {
    columns: Option<Vec<String>>,
    escape: u8,
    max_record_size: usize,
    newline: Vec<u8>,
    quote: u8,
    separator: u8,
    should_detect_columns: bool,
    should_ltrim_fields: bool,
    should_rtrim_fields: bool,
    should_relax_column_count_less: bool,
    should_relax_column_count_more: bool,
    should_skip_empty_rows: bool,
    should_skip_rows_with_error: bool,
}

struct RecordBuffer<R> {
    buf: Vec<u8>,
    len_trailing_newline: usize,
    line_count: usize,
    max_record_size: usize,
    newline: Vec<u8>,
    reader: BufReader<R>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            columns: None,
            escape: b'"',
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

impl<R> RecordBuffer<R>
where
    R: Read,
{
    fn new(reader: R, newline: &[u8], max_record_size: usize) -> Self {
        Self {
            buf: vec![],
            len_trailing_newline: 0,
            line_count: 0,
            max_record_size,
            newline: newline.to_vec(),
            reader: BufReader::new(reader),
        }
    }

    fn take_inner(&mut self) -> Vec<u8> {
        let buf_capacity = self.buf.capacity();
        std::mem::replace(&mut self.buf, Vec::with_capacity(buf_capacity))
    }

    fn append_next_line(&mut self) -> Option<Result<usize>> {
        self.line_count += 1;
        let last = self
            .newline
            .last()
            .expect("Newline terminator cannot be empty");
        let mut bytes_read = 0;
        if self.buf.len() < self.max_record_size {
            // We allow 1 more byte than the limit to indicate the limit has been eclipsed.
            let read_limit = self.max_record_size - self.buf.len() + 1;
            loop {
                match self
                    .reader
                    .by_ref()
                    .take(read_limit as u64)
                    .read_until(*last, &mut self.buf)
                {
                    Ok(0) => {
                        if bytes_read == 0 {
                            return None;
                        } else {
                            self.len_trailing_newline = 0;
                            break;
                        }
                    }
                    Ok(n) => {
                        if self.buf.ends_with(&self.newline) {
                            self.len_trailing_newline = self.newline.len();
                            break;
                        } else {
                            bytes_read += n;
                            continue;
                        }
                    }
                    Err(e) => return Some(Err(ErrorKind::Io(e))),
                }
            }
        }
        if self.buf.len() > self.max_record_size {
            return Some(Err(ErrorKind::RecordTooLarge {
                max_record_size: self.max_record_size,
            }));
        }
        match std::str::from_utf8(&self.buf) {
            Ok(_valid_utf8) => Some(Ok(bytes_read)),
            Err(e) => Some(Err(ErrorKind::Utf8(e))),
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
        self.buf.len() - self.len_trailing_newline
    }

    // Negligible improvement from inlining this.
    #[inline]
    fn remove(&mut self, index: usize) {
        self.buf.remove(index);
    }

    // Negligible improvement from inlining this.
    #[inline]
    fn clear(&mut self) {
        self.buf.clear();
    }
}
