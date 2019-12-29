use crate::{Error, ErrorKind, Record, Records};
use std::io::{BufRead, BufReader, Read};
use std::ops::Range;

/// Configurer and creator of `Parser`s.
#[derive(Default)]
pub struct ParserBuilder {
    /// Configuration settings.
    config: Config,
}

type FieldParser<Parser> = fn(&mut Parser, start: usize) -> Result<(Range<usize>, usize)>;

type OnReadLine<R> = fn(
    current_record_buffer: &mut RecordBuffer<R>,
    last_read_attempt: Result<usize>,
) -> Result<usize>;

type OnRecord = fn(
    curr_record: Result<Option<Record>>,
    headers: &mut Option<Vec<String>>,
) -> Result<Option<Record>>;

type Result<T> = std::result::Result<T, ErrorKind>;

/// CSV parser.
pub struct Parser<R> {
    /// Configuration settings.
    config: Config,
    /// Buffer containing contents of current record.
    current_record_buffer: RecordBuffer<R>,
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

    /// Sets comment indicator to given bytes.
    pub fn comment(&mut self, comment: &[u8]) -> &mut Self {
        self.config.comment.replace(comment.to_vec());
        self
    }

    /// Sets the escape character to the given byte.
    config!(escape, escape, u8);

    /// Sets header names for rows to given values. Overrides `detect_headers`.
    pub fn headers(&mut self, headers: Vec<String>) -> &mut Self {
        self.config.headers.replace(headers);
        self
    }

    /// Determines whether first row should be treated as header names for subsequent rows.
    config!(detect_headers, should_detect_headers);

    /// Sets maximum record size. Parser longer than this value will result in an error.
    config!(max_record_size, max_record_size, usize);

    /// Sets newline terminator to given bytes.
    pub fn newline(&mut self, newline: &[u8]) -> &mut Self {
        self.config.newline = newline.to_vec();
        self
    }

    /// Sets quote marker to given byte.
    config!(quote, quote, u8);

    /// Sets field separator to given byte.
    config!(separator, separator, u8);

    /// Determines whether fields should be start-trimmed of whitespace.
    config!(trim_start, should_trim_start_fields);

    /// Determines whether fields should be end-trimmed of whitespace.
    config!(trim_end, should_trim_end_fields);

    /// Determines whether fields should be start *and* end trimmed of whitespace.
    pub fn trim(&mut self, should_trim: bool) -> &mut Self {
        self.config.should_trim_start_fields = should_trim;
        self.config.should_trim_end_fields = should_trim;
        self
    }

    /// Determines whether records should be allowed to have fewer fields than the number of headers.
    config!(relax_field_count_less, should_relax_field_count_less);

    /// Determines whether records should be allowed to have more fields than the number of headers.
    config!(relax_field_count_more, should_relax_field_count_more);

    /// Determines whether records should be allowed to have more *or* fewer fields than the number of headers.
    pub fn relax_field_count(&mut self, should_relax: bool) -> &mut Self {
        self.config.should_relax_field_count_less = should_relax;
        self.config.should_relax_field_count_more = should_relax;
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
        let current_record_buffer = RecordBuffer::new(csv, &config);

        let on_read_line = {
            let mut on_read_line: Vec<OnReadLine<R>> = vec![];
            if config.should_skip_empty_rows {
                on_read_line.push(Self::on_read_line_skip_empty_lines);
            }
            if config.comment.is_some() {
                on_read_line.push(Self::on_read_line_skip_comments);
            }
            on_read_line
        };

        let on_record = {
            let mut on_record: Vec<OnRecord> = vec![];
            match (
                config.should_relax_field_count_less,
                config.should_relax_field_count_more,
            ) {
                (false, false) => (),
                (true, false) => on_record.push(Self::on_record_relax_headers_less),
                (false, true) => on_record.push(Self::on_record_relax_headers_more),
                (true, true) => on_record.push(Self::on_record_relax_headers),
            };
            on_record.push(if config.should_detect_headers {
                Self::on_record_detect_headers
            } else {
                Self::on_record_log_headers
            });
            if config.should_skip_rows_with_error {
                on_record.push(Self::on_record_skip_malformed);
            }
            on_record
        };

        let parse_field = match (
            config.should_trim_start_fields,
            config.should_trim_end_fields,
        ) {
            (false, false) => Self::parse_field,
            (true, false) => Self::parse_field_trim_start,
            (false, true) => Self::parse_field_trim_end,
            (true, true) => Self::parse_field_trim,
        };

        Parser {
            current_record_buffer,
            config,
            on_read_line,
            on_record,
            parse_field,
        }
    }

    /// Returns the fields of the first record read by the `Parser`.
    ///
    /// This will cause the `Parser` to read a record if none have been read.
    pub fn headers(&mut self) -> std::result::Result<&[String], Error> {
        if self.config.headers.is_none() {
            return match self.next_record() {
                Some(Ok(_nread)) => match &self.config.headers {
                    Some(cols) => Ok(cols),
                    None => unreachable!(),
                },
                Some(Err(e)) => Err(e),
                None => Ok(&[]),
            };
        }
        match &self.config.headers {
            Some(headers) => Ok(headers),
            None => Ok(&[]),
        }
    }

    /// Converts the `Parser` into an iterator of `Record`s.
    pub fn records(self) -> Records<R> {
        Records::new(self)
    }

    /// Returns the next `Record` (if any) parsed from the underlying reader.
    pub(crate) fn next_record(&mut self) -> Option<std::result::Result<Record, Error>> {
        let initial_read_attempt = self.current_record_buffer.append_next_line();
        let current_record_buffer = &mut self.current_record_buffer;

        let read_attempt = self
            .on_read_line
            .iter()
            .fold(initial_read_attempt, |read_attempt, on_read_line| {
                on_read_line(current_record_buffer, read_attempt)
            });

        match read_attempt {
            Ok(0) => None,
            Err(e) => Some(Err(Error::new(self.current_record_buffer.line_count, e))),
            Ok(_nread) => {
                let next_record = Some(self.record()).transpose();
                let headers = &mut self.config.headers;

                match self
                    .on_record
                    .iter()
                    .fold(next_record, |record, on_record| on_record(record, headers))
                {
                    Ok(Some(record)) => Some(Ok(record)),
                    Ok(None) => {
                        self.current_record_buffer.clear();
                        self.next_record()
                    }
                    Err(e) => Some(Err(Error::new(self.current_record_buffer.line_count, e))),
                }
            }
        }
    }

    fn record(&mut self) -> Result<Record> {
        let expected_num_fields = self.config.headers.as_ref().map_or(1, |cols| cols.len());
        let mut field_bounds = Vec::with_capacity(expected_num_fields);

        let mut start = 0;
        loop {
            let (bounds, end) = (self.parse_field)(self, start)?;

            start = end;
            field_bounds.push(bounds);

            if start >= self.current_record_buffer.len_sans_newline() {
                break;
            }

            if self.current_record_buffer.get_unchecked(start) != self.config.separator {
                return Err(ErrorKind::BadField {
                    col: end,
                    msg: String::from("Fields cannot contain trailing values"),
                });
            }

            start += 1;
        }

        let record_buf = self.current_record_buffer.take_inner_as_string();
        let record = Record::new(record_buf, field_bounds);

        Ok(record)
    }

    fn quote(&mut self, start: usize) -> Result<(Range<usize>, usize)> {
        // Start at first byte after quotation byte.
        let start = start + 1;

        let mut end = start;
        loop {
            let mut max = self.current_record_buffer.len_sans_newline();

            while end < max {
                let curr = self.current_record_buffer.get_unchecked(end);
                let next_index = end + 1;

                if curr == self.config.escape {
                    match self.current_record_buffer.get(next_index) {
                        Some(&c) if c == self.config.quote => {
                            // Remove escape character from buffer leaving only escaped value.
                            self.current_record_buffer.remove(end);
                            max -= 1;
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
                Ok(0) => {
                    return Err(ErrorKind::BadField {
                        col: old_buf_len - end,
                        msg: String::from("Quoted field is missing closing quotation"),
                    });
                }
                Ok(_nread) => end = old_buf_len,
                Err(e) => return Err(e),
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
                            "Unquoted fields cannot contain quote byte: `{}`",
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
    (default, $self:expr, $start:expr) => {{
        let first_byte = $self.current_record_buffer.get($start);
        match first_byte {
            Some(&byte) if byte == $self.config.quote => $self.quote($start),
            _ => $self.text($start),
        }
    }};

    (with_trim_end, $self:expr, $start:expr) => {{
        let first_byte = $self.current_record_buffer.get($start);
        match first_byte {
            Some(&byte) if byte == $self.config.quote => {
                $self.quote($start).and_then(|(bounds, mut end)| {
                    if let Some(&c) = $self.current_record_buffer.get(end) {
                        if c != $self.config.separator
                            && end < $self.current_record_buffer.len_sans_newline()
                        {
                            end = parse_field!(trim_start, $self, end);
                        }
                    }
                    Ok((bounds, end))
                })
            }
            _ => $self.text($start).map(|(mut bounds, end)| {
                let buf_segment = &$self.current_record_buffer.as_str()[bounds.clone()];
                let trimmed = buf_segment.trim_end().as_bytes();
                bounds.end = bounds.start + trimmed.len();
                (bounds, end)
            }),
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
    fn parse_field(&mut self, start: usize) -> Result<(Range<usize>, usize)> {
        parse_field!(default, self, start)
    }

    fn parse_field_trim_start(&mut self, start: usize) -> Result<(Range<usize>, usize)> {
        let start = parse_field!(trim_start, self, start);
        parse_field!(default, self, start)
    }

    fn parse_field_trim_end(&mut self, start: usize) -> Result<(Range<usize>, usize)> {
        parse_field!(with_trim_end, self, start)
    }

    fn parse_field_trim(&mut self, start: usize) -> Result<(Range<usize>, usize)> {
        let start = parse_field!(trim_start, self, start);
        parse_field!(with_trim_end, self, start)
    }
}

impl<R> Parser<R>
where
    R: Read,
{
    fn on_record_relax_headers_less(
        mut record: Result<Option<Record>>,
        headers: &mut Option<Vec<String>>,
    ) -> Result<Option<Record>> {
        match (&mut record, &headers) {
            (Ok(Some(record)), Some(headers)) if record.num_fields() < headers.len() => {
                record.set_num_fields(headers.len());
            }
            _ => (),
        };
        record
    }

    fn on_record_relax_headers_more(
        mut record: Result<Option<Record>>,
        headers: &mut Option<Vec<String>>,
    ) -> Result<Option<Record>> {
        match (&mut record, &headers) {
            (Ok(Some(record)), Some(headers)) if record.num_fields() > headers.len() => {
                record.set_num_fields(headers.len());
            }
            _ => (),
        };
        record
    }

    fn on_record_relax_headers(
        mut record: Result<Option<Record>>,
        headers: &mut Option<Vec<String>>,
    ) -> Result<Option<Record>> {
        match (&mut record, &headers) {
            (Ok(Some(record)), Some(headers)) if record.num_fields() != headers.len() => {
                record.set_num_fields(headers.len());
            }
            _ => (),
        };
        record
    }

    fn on_record_log_headers(
        record: Result<Option<Record>>,
        headers: &mut Option<Vec<String>>,
    ) -> Result<Option<Record>> {
        match (&record, &headers) {
            (Ok(Some(rec)), Some(cols)) if cols.len() == rec.num_fields() => record,
            (Ok(Some(rec)), Some(cols)) => Err(ErrorKind::UnequalNumFields {
                expected_num: cols.len(),
                num: rec.num_fields(),
            }),
            (Ok(Some(rec)), None) => {
                let found_headers = rec.fields().iter().map(|f| f.to_string()).collect();
                headers.replace(found_headers);
                record
            }
            _ => record,
        }
    }

    fn on_record_detect_headers(
        record: Result<Option<Record>>,
        headers: &mut Option<Vec<String>>,
    ) -> Result<Option<Record>> {
        match (&record, &headers) {
            (Ok(Some(rec)), Some(cols)) if cols.len() == rec.num_fields() => record,
            (Ok(Some(rec)), Some(cols)) => Err(ErrorKind::UnequalNumFields {
                expected_num: cols.len(),
                num: rec.num_fields(),
            }),
            (Ok(Some(rec)), None) => {
                let found_headers = rec.fields().iter().map(|f| f.to_string()).collect();
                headers.replace(found_headers);
                Ok(None)
            }
            _ => record,
        }
    }

    fn on_record_skip_malformed(
        record: Result<Option<Record>>,
        _headers: &mut Option<Vec<String>>,
    ) -> Result<Option<Record>> {
        match &record {
            Err(ErrorKind::BadField { .. }) | Err(ErrorKind::UnequalNumFields { .. }) => Ok(None),
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
        last_read_attempt: Result<usize>,
    ) -> Result<usize> {
        let mut curr_read_attempt = last_read_attempt;
        loop {
            match curr_read_attempt {
                Ok(nread) if nread > 0 && current_record_buffer.is_only_whitespace() => {
                    current_record_buffer.clear();
                    curr_read_attempt = current_record_buffer.append_next_line();
                }
                _ => return curr_read_attempt,
            }
        }
    }

    fn on_read_line_skip_comments(
        current_record_buffer: &mut RecordBuffer<R>,
        last_read_attempt: Result<usize>,
    ) -> Result<usize> {
        let mut curr_read_attempt = last_read_attempt;
        loop {
            match curr_read_attempt {
                Ok(nread) if nread > 0 && current_record_buffer.is_comment() => {
                    current_record_buffer.clear();
                    curr_read_attempt = current_record_buffer.append_next_line();
                }
                _ => return curr_read_attempt,
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
/// Private
///////////////////////////////////////////////////////////////////////////////
#[derive(Clone)]
struct Config {
    headers: Option<Vec<String>>,
    comment: Option<Vec<u8>>,
    escape: u8,
    max_record_size: usize,
    newline: Vec<u8>,
    quote: u8,
    separator: u8,
    should_detect_headers: bool,
    should_trim_start_fields: bool,
    should_trim_end_fields: bool,
    should_relax_field_count_less: bool,
    should_relax_field_count_more: bool,
    should_skip_empty_rows: bool,
    should_skip_rows_with_error: bool,
}

struct RecordBuffer<R> {
    buf: Vec<u8>,
    comment: Option<Vec<u8>>,
    len_trailing_newline: usize,
    line_count: usize,
    max_record_size: usize,
    newline: Vec<u8>,
    reader: BufReader<R>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            headers: None,
            comment: None,
            escape: b'"',
            max_record_size: 4096,
            newline: vec![b'\n'],
            quote: b'"',
            separator: b',',
            should_detect_headers: false,
            should_trim_start_fields: true,
            should_trim_end_fields: true,
            should_relax_field_count_less: false,
            should_relax_field_count_more: false,
            should_skip_empty_rows: true,
            should_skip_rows_with_error: false,
        }
    }
}

impl<R> RecordBuffer<R>
where
    R: Read,
{
    fn new(reader: R, config: &Config) -> Self {
        Self {
            buf: vec![],
            comment: config.comment.clone(),
            len_trailing_newline: 0,
            line_count: 0,
            max_record_size: config.max_record_size,
            newline: config.newline.to_vec(),
            reader: BufReader::new(reader),
        }
    }

    fn take_inner_as_string(&mut self) -> String {
        let buf_capacity = self.buf.capacity();
        let old_buf = std::mem::replace(&mut self.buf, Vec::with_capacity(buf_capacity));
        // This is safe because `buf` is checked to be valid UTF-8.
        unsafe { String::from_utf8_unchecked(old_buf) }
    }

    fn append_next_line(&mut self) -> Result<usize> {
        self.line_count += 1;
        let last = *self
            .newline
            .last()
            .expect("Newline terminator cannot be empty");
        let mut nread = 0;
        let initial_buf_len = self.buf.len();
        if initial_buf_len < self.max_record_size {
            // We allow 1 more byte than the limit to indicate the limit has been eclipsed.
            let read_limit = self.max_record_size - initial_buf_len + 1;
            let mut reader = self.reader.by_ref().take(read_limit as u64);
            loop {
                match reader.read_until(last, &mut self.buf) {
                    Ok(0) if nread == 0 => {
                        return Ok(0);
                    }
                    Ok(0) => {
                        self.len_trailing_newline = 0;
                        break;
                    }
                    Ok(n) => {
                        nread += n;
                        if self.buf.ends_with(&self.newline) {
                            self.len_trailing_newline = self.newline.len();
                            break;
                        }
                    }
                    Err(e) => return Err(ErrorKind::Io(e)),
                }
            }
        }
        if self.buf.len() > self.max_record_size {
            return Err(ErrorKind::RecordTooLarge {
                max_record_size: self.max_record_size,
            });
        }
        match std::str::from_utf8(&self.buf[initial_buf_len..]) {
            Ok(_valid_utf8) => Ok(nread),
            Err(e) => Err(ErrorKind::Utf8(e)),
        }
    }

    fn is_only_whitespace(&self) -> bool {
        self.as_str().trim_start().is_empty()
    }

    fn is_comment(&self) -> bool {
        match &self.comment {
            Some(comment) => self.buf.starts_with(comment),
            _ => unreachable!(),
        }
    }

    fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.buf) }
    }

    #[inline]
    fn get(&self, index: usize) -> Option<&u8> {
        self.buf.get(index)
    }

    #[inline]
    fn get_unchecked(&self, index: usize) -> u8 {
        unsafe { *self.buf.get_unchecked(index) }
    }

    #[inline]
    fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    fn len_sans_newline(&self) -> usize {
        self.buf.len() - self.len_trailing_newline
    }

    #[inline]
    fn remove(&mut self, index: usize) {
        self.buf.remove(index);
    }

    #[inline]
    fn clear(&mut self) {
        self.buf.clear();
    }
}
