use crate::error::{Error, ErrorKind};
use crate::Parser;
use std::io::{BufRead, BufReader, Read};
use std::ops::Range;

type Result<T> = std::result::Result<T, ErrorKind>;

#[derive(Clone)]
pub struct Config {
    pub newline: Vec<u8>,
    pub separator: u8,
    pub quote: u8,
    pub columns: Option<Vec<String>>,
    pub should_detect_columns: bool,
    pub should_relax_column_count_less: bool,
    pub should_relax_column_count_more: bool,
    pub should_skip_empty_rows: bool,
    pub should_skip_rows_with_error: bool,
    pub should_ltrim_fields: bool,
    pub should_rtrim_fields: bool,
}

// TODO Use defaults
impl Default for Config {
    fn default() -> Self {
        Config {
            newline: vec![b'\n'],
            separator: b',',
            quote: b'"',
            columns: None,
            should_detect_columns: false,
            should_relax_column_count_less: false,
            should_relax_column_count_more: false,
            should_skip_empty_rows: true,
            should_skip_rows_with_error: false,
            should_ltrim_fields: true,
            should_rtrim_fields: true,
        }
    }
}

/// Iterator over CSV records.
pub struct Records<R> {
    /// Iterator over lines of CSV.
    csv_lines: Lines<R>,
    /// Column names, if any.
    columns: Option<Vec<String>>,
    /// Configuration settings.
    config: Config,
    /// Callback(s) to perform after a line is read.
    on_read_line: Vec<
        fn(
            line_reader: &mut Lines<R>,
            curr_line_read: Option<Result<LineBuffer>>,
        ) -> Option<Result<LineBuffer>>,
    >,
    /// Callback(s) to perform after a record is created but before it is returned.
    on_record: Vec<
        fn(
            curr_record: Option<Result<Record>>,
            columnds: &mut Option<Vec<String>>,
        ) -> Option<Result<Record>>,
    >,
    /// Method for parsing fields.
    parse_field: fn(
        &mut Self,
        line_buf: &mut LineBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>>,
}

impl<R> Records<R>
where
    R: Read,
{
    fn parse_field(
        &mut self,
        line_buf: &mut LineBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>> {
        let first_byte = line_buf.buf.get(start)?;
        if first_byte == &self.config.quote {
            Some(self.quote(line_buf, start).and_then(
                |(bounds, end)| match line_buf.buf.get(end) {
                    Some(&c) if c != self.config.separator && end < line_buf.len_sans_newline() => {
                        return Err(ErrorKind::BadField(String::from(
                            "Quoted fields cannot contain trailing unquoted values",
                        )))
                    }
                    _ => Ok((bounds, end)),
                },
            ))
        } else {
            Some(self.text(line_buf, start))
        }
    }

    fn parse_field_ltrim(
        &mut self,
        line_buf: &mut LineBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>> {
        let start = Self::on_field_start_trim(&mut line_buf.buf, start);
        self.parse_field(line_buf, start)
    }

    fn parse_field_rtrim(
        &mut self,
        line_buf: &mut LineBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>> {
        let first_byte = line_buf.buf.get(start)?;
        if first_byte == &self.config.quote {
            Some(
                self.quote(line_buf, start)
                    .map(|(bounds, end)| {
                        let end = Self::on_field_start_trim(&mut line_buf.buf, end);
                        (bounds, end)
                    })
                    .and_then(|(bounds, end)| match line_buf.buf.get(end) {
                        Some(&c)
                            if c != self.config.separator && end < line_buf.len_sans_newline() =>
                        {
                            return Err(ErrorKind::BadField(String::from(
                                "Quoted fields cannot contain trailing unquoted values",
                            )))
                        }
                        _ => Ok((bounds, end)),
                    }),
            )
        } else {
            Some(self.text(line_buf, start).map(|(bounds, end)| {
                let bounds = Self::on_field_end_trim(&line_buf.buf, bounds);
                (bounds, end)
            }))
        }
    }

    fn parse_field_trim(
        &mut self,
        line_buf: &mut LineBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>> {
        let start = Self::on_field_start_trim(&mut line_buf.buf, start);
        self.parse_field_rtrim(line_buf, start)
    }

    fn on_field_start_trim(buf: &Vec<u8>, start: usize) -> usize {
        let line = unsafe { std::str::from_utf8_unchecked(&buf[start..]) };
        let trimmed = line.trim_start().as_bytes();
        let len_trimmed = buf.len() - start - trimmed.len();
        start + len_trimmed
    }

    fn on_field_end_trim(buf: &Vec<u8>, field_bounds: Range<usize>) -> Range<usize> {
        let line = unsafe { std::str::from_utf8_unchecked(&buf[field_bounds.clone()]) };
        let trimmed = line.trim_end().as_bytes();
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
    fn on_read_line_verify_is_utf8(
        _lines: &mut Lines<R>,
        line_buf: Option<Result<LineBuffer>>,
    ) -> Option<Result<LineBuffer>> {
        if let Some(Ok(line)) = &line_buf {
            return match std::str::from_utf8(&line.buf) {
                Ok(_line) => line_buf,
                Err(e) => return Some(Err(ErrorKind::Utf8(e))),
            };
        }
        line_buf
    }

    fn on_read_line_skip_empty_lines(
        lines: &mut Lines<R>,
        line_buf: Option<Result<LineBuffer>>,
    ) -> Option<Result<LineBuffer>> {
        if let Some(Ok(line_buf)) = line_buf {
            // By the time this function is called, the current should already have been verified to have contained valid UTF-8.
            let line = unsafe { std::str::from_utf8_unchecked(&line_buf.buf) };
            return if line.trim().is_empty() {
                let next_line = lines.next();
                Records::<R>::on_read_line_skip_empty_lines(lines, next_line)
            } else {
                Some(Ok(line_buf))
            };
        }
        line_buf
    }
}

impl<R> Records<R>
where
    R: Read,
{
    pub fn new(csv: R, config: Config) -> Self {
        let csv_lines = Lines::new(csv, &config.newline);

        let on_read_line = {
            let mut on_read_line: Vec<
                fn(&mut Lines<R>, Option<Result<LineBuffer>>) -> Option<Result<LineBuffer>>,
            > = vec![Self::on_read_line_verify_is_utf8];
            if config.should_skip_empty_rows {
                on_read_line.push(Self::on_read_line_skip_empty_lines);
            }
            on_read_line
        };

        let on_record = {
            let mut on_record: Vec<
                fn(
                    curr_record: Option<Result<Record>>,
                    columnds: &mut Option<Vec<String>>,
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
            csv_lines,
            columns: config.columns.clone(),
            config: config,
            on_read_line,
            on_record,
            parse_field,
        }
    }

    fn record(&mut self, mut line_buf: LineBuffer) -> Option<Result<Record>> {
        let expected_num_fields = self.columns.as_ref().map_or(1, |cols| cols.len());
        let mut field_bounds = Vec::with_capacity(expected_num_fields);

        let mut start = 0;
        loop {
            let (bounds, end) = match (self.parse_field)(self, &mut line_buf, start) {
                Some(Ok(result)) => result,
                Some(Err(e)) => return Some(Err(e)),
                None => break,
            };

            start = end;
            field_bounds.push(bounds);

            match line_buf.buf.get(start) {
                Some(&c) if c == self.config.separator => {
                    start += 1;
                    continue;
                }
                Some(&_c) if start == line_buf.len_sans_newline() => break,
                Some(&_c) => unreachable!(),
                None => break,
            }
        }

        let record = Record::new(line_buf.buf, field_bounds);

        Some(Ok(record))
    }

    /*fn field(
        &mut self,
        line_buf: &mut LineBuffer,
        start: usize,
    ) -> Option<Result<(Range<usize>, usize)>> {
        /*match line_buf.buf.get(start) {
            Some(&c) if c == self.config.quote => Some(self.quote(line_buf, start)),
            Some(_c) => Some((self.parse_text)(self, line_buf, start)),
            None => None,
        }
        */
    }*/

    fn quote(&mut self, line_buf: &mut LineBuffer, start: usize) -> Result<(Range<usize>, usize)> {
        // Start at first byte after quotation byte.
        let start = start + 1;

        let mut end = start;
        loop {
            if end >= line_buf.len_sans_newline() {
                let old_buf_len = line_buf.buf.len();
                match self.csv_lines.append_to(line_buf) {
                    Some(Ok(())) => end = old_buf_len,
                    Some(Err(e)) => return Err(e),
                    None => break,
                };
            }

            match line_buf.buf.get(end) {
                Some(&c) if c == self.config.quote => {
                    let next_index = end + 1;
                    match line_buf.buf.get(next_index) {
                        Some(&c) if c == self.config.quote => {
                            // Remove duplicate leaving only escaped quote in buffer.
                            line_buf.buf.remove(next_index);
                        }
                        _ => return Ok((start..end, next_index)),
                    }
                }
                Some(_c) => (),
                None => break,
            }

            end += 1
        }

        Err(ErrorKind::BadField(String::from(
            "Quoted field is missing closing quotation",
        )))
    }

    fn text(&mut self, line_buf: &mut LineBuffer, start: usize) -> Result<(Range<usize>, usize)> {
        let mut end = start;

        while end < line_buf.len_sans_newline() {
            match line_buf.buf.get(end) {
                Some(&c) if c == self.config.quote => {
                    return Err(ErrorKind::BadField(format!(
                        "Unquoted fields cannot contain quote character: `{}`",
                        self.config.quote
                    )))
                }
                Some(&c) if c != self.config.separator => end += 1,
                _ => break,
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
        let next_line = self.csv_lines.next();
        let line_reader = &mut self.csv_lines;
        let line_buf = match self
            .on_read_line
            .iter()
            .fold(next_line, |line, on_read_line| {
                on_read_line(line_reader, line)
            }) {
            Some(Ok(buf)) => buf,
            Some(Err(e)) => return Some(Err(Error::new(0, 0, e))),
            None => return None,
        };

        let next_record = self.record(line_buf);
        let columns = &mut self.columns;
        match self
            .on_record
            .iter()
            .fold(next_record, |record, on_record| on_record(record, columns))
        {
            Some(Ok(record)) => Some(Ok(record)),
            Some(Err(e)) => Some(Err(Error::new(0, 0, e))),
            None => self.next(),
        }
    }
}

#[derive(Debug)]
/// Represents a CSV record.
pub struct Record {
    /// Contents of the record as a bytes.
    buf: Vec<u8>,
    /// Ranges describing locations of fields within `buf`.
    field_bounds: Vec<Range<usize>>,
}

impl<'a> Record {
    fn new(buf: Vec<u8>, field_bounds: Vec<Range<usize>>) -> Self {
        Record { buf, field_bounds }
    }

    pub fn fields(&self) -> Vec<&str> {
        self.field_bounds
            .iter()
            .map(|bounds| unsafe {
                // Records created by `Records` are guaranteed to have only valid UTF-8.
                std::str::from_utf8_unchecked(&self.buf[bounds.start..bounds.end])
            })
            .collect()
    }
}

struct LineBuffer {
    buf: Vec<u8>,
    len_trailing_newline: usize,
}

impl LineBuffer {
    fn new(buf: Vec<u8>, len_trailing_newline: usize) -> Self {
        LineBuffer {
            buf,
            len_trailing_newline,
        }
    }

    fn len_sans_newline(&self) -> usize {
        self.buf.len() - self.len_trailing_newline
    }
}

struct Lines<R> {
    reader: BufReader<R>,
    newline: Vec<u8>,
    line_count: usize,
    last_lines_capacity: usize,
}

impl<R> Lines<R>
where
    R: Read,
{
    fn new(reader: R, newline: &[u8]) -> Self {
        Lines {
            reader: BufReader::new(reader),
            newline: Vec::from(newline),
            line_count: 0,
            last_lines_capacity: 0,
        }
    }

    fn append_to(&mut self, line_buf: &mut LineBuffer) -> Option<Result<()>> {
        let last = self
            .newline
            .last()
            .expect("Newline terminator cannot be empty");
        loop {
            match self.reader.read_until(*last, &mut line_buf.buf) {
                Ok(0) if line_buf.buf.len() == 0 => return None,
                Ok(n) if n == 0 => {
                    line_buf.len_trailing_newline = 0;
                    break;
                }
                Ok(_n) if line_buf.buf.ends_with(&self.newline) => {
                    line_buf.len_trailing_newline = self.newline.len();
                    break;
                }
                Ok(_n) => continue,
                Err(e) => return Some(Err(ErrorKind::Io(e))),
            };
        }
        self.last_lines_capacity = line_buf.buf.capacity();
        Some(Ok(()))
    }
}

impl<R> Iterator for Lines<R>
where
    R: Read,
{
    type Item = Result<LineBuffer>;
    fn next(&mut self) -> Option<Self::Item> {
        let last = self
            .newline
            .last()
            .expect("Newline terminator cannot be empty");
        let mut buf = Vec::with_capacity(self.last_lines_capacity);
        let len_trailing_newline;
        loop {
            match self.reader.read_until(*last, &mut buf) {
                Ok(0) if buf.len() == 0 => return None,
                Ok(n) if n == 0 => {
                    len_trailing_newline = 0;
                    break;
                }
                Ok(_n) if buf.ends_with(&self.newline) => {
                    len_trailing_newline = self.newline.len();
                    break;
                }
                Ok(_n) => continue,
                Err(e) => return Some(Err(ErrorKind::Io(e))),
            };
        }
        self.last_lines_capacity = buf.capacity();
        Some(Ok(LineBuffer::new(buf, len_trailing_newline)))
    }
}
/*
type Result<T> = std::result::Result<T, ErrorKind>;
type Field = String;
type Record = Vec<Field>;

type RecordMiddleware<'a> =
    &'a dyn Fn(Result<Option<Record>>, &mut Option<Record>) -> Result<Option<Record>>;

pub struct Records<'a, R>
where
    R: Read,
{
    csv: Lines<'a, BufReader<R>>,
    curr_line: Vec<char>,
    columns: Option<Record>,
    parser: &'a Parser,
    parse_quote: &'a dyn Fn(&mut Self) -> Result<Field>,
    parse_text: &'a dyn Fn(&mut Self) -> Result<Vec<char>>,
    parse_record: &'a dyn Fn(&mut Self) -> Result<Option<Record>>,
    record_middleware: Vec<RecordMiddleware<'a>>,
    read_line: &'a dyn Fn(&mut Self) -> Result<Option<usize>>,
}

impl<'a, R> Records<'a, R>
where
    R: Read,
{
    pub fn new(csv_source: R, parser: &'a Parser) -> Self {
        let parse_quote = &Self::quote_default;

        let parse_text: &'a dyn Fn(&mut Self) -> Result<Vec<char>> =
            match (parser.should_ltrim_fields, parser.should_rtrim_fields) {
                (true, true) => &Self::text_trim,
                (true, false) => &Self::text_ltrim,
                (false, true) => &Self::text_rtrim,
                (false, false) => &Self::text_default,
            };

        let parse_record: &'a dyn Fn(&mut Self) -> Result<Option<Record>> = &Self::record_default;

        let record_middleware: Vec<RecordMiddleware<'a>> = {
            let mut middleware: Vec<RecordMiddleware<'a>> = vec![];
            if parser.should_relax_column_count_less {
                middleware.push(&Self::record_relax_columns_less);
            };
            if parser.should_relax_column_count_more {
                middleware.push(&Self::record_relax_columns_more);
            };
            middleware.push(match parser.should_detect_columns {
                true => &Self::record_column_detect,
                false => &Self::record_column_default,
            });
            if parser.should_skip_rows_with_error {
                middleware.push(&Self::record_skip_on_error);
            }
            middleware
        };

        let read_line: &'a dyn Fn(&mut Self) -> Result<Option<usize>> =
            if parser.should_skip_empty_rows {
                &Self::read_line_no_empty
            } else {
                &Self::read_line_default
            };

        Records {
            parser,
            csv: Lines::new(BufReader::new(csv_source), parser.newline.as_ref()),
            curr_line: vec![],
            columns: parser.columns.clone(),
            parse_quote,
            parse_text,
            parse_record,
            record_middleware,
            read_line,
        }
    }

    fn read_line(&mut self) -> Result<Option<usize>> {
        (self.read_line)(self)
    }

    fn read_line_no_empty(&mut self) -> Result<Option<usize>> {
        match self.csv.next() {
            Some(Ok(line)) => {
                if line.trim().is_empty() {
                    self.read_line_no_empty()
                } else {
                    self.curr_line.extend(line.chars().rev());
                    Ok(Some(line.len()))
                }
            }
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    fn read_line_default(&mut self) -> Result<Option<usize>> {
        self.csv.next().map_or(Ok(None), |line_or_err| {
            line_or_err.and_then(|line| {
                self.curr_line.extend(line.chars().rev());
                Ok(Some(line.len()))
            })
        })
    }

    // Record
    fn record(&mut self) -> Result<Option<Record>> {
        let record = (self.parse_record)(self);
        let columns = &mut self.columns;
        self.record_middleware
            .iter()
            .fold(record, move |record, mw| mw(record, columns))
    }

    fn record_default(&mut self) -> Result<Option<Record>> {
        let mut fields = Vec::with_capacity(self.columns.as_ref().map_or(1, |cols| cols.len()));

        loop {
            let field = self.field()?;

            fields.push(field);

            match self.curr_line.last() {
                Some(&c) if c == self.parser.separator => {
                    self.curr_line.pop();
                    continue;
                }
                Some(_c) => unreachable!(),
                None => break,
            }
        }

        Ok(Some(fields))
    }

    fn record_skip_on_error(
        record: Result<Option<Record>>,
        _columns: &mut Option<Record>,
    ) -> Result<Option<Record>> {
        record.or_else(|err| match &err {
            ErrorKind::BadField(..) | ErrorKind::UnequalNumFields { .. } => Ok(None),
            _ => Err(err),
        })
    }

    fn record_relax_columns_less(
        mut record: Result<Option<Record>>,
        columns: &mut Option<Record>,
    ) -> Result<Option<Record>> {
        match (&mut record, &columns) {
            (Ok(Some(record)), Some(columns)) if record.len() < columns.len() => {
                record.resize(columns.len(), String::from(""));
            }
            _ => (),
        };
        record
    }

    fn record_relax_columns_more(
        mut record: Result<Option<Record>>,
        columns: &mut Option<Record>,
    ) -> Result<Option<Record>> {
        match (&mut record, &columns) {
            (Ok(Some(record)), Some(columns)) if record.len() > columns.len() => {
                record.truncate(columns.len());
            }
            _ => (),
        };
        record
    }

    fn record_column_default(
        record: Result<Option<Record>>,
        columns: &mut Option<Record>,
    ) -> Result<Option<Record>> {
        if let Ok(Some(record)) = record {
            match columns {
                Some(columns) if columns.len() == record.len() => Ok(Some(record)),
                Some(columns) => Err(ErrorKind::UnequalNumFields {
                    expected_num: columns.len(),
                    num: record.len(),
                }),
                None => {
                    let standin_columns = vec![String::from(""); record.len()];
                    columns.replace(standin_columns);
                    Ok(Some(record))
                }
            }
        } else {
            record
        }
    }

    fn record_column_detect(
        record: Result<Option<Record>>,
        columns: &mut Option<Record>,
    ) -> Result<Option<Record>> {
        if let Ok(Some(record)) = record {
            match columns {
                Some(columns) if columns.len() == record.len() => Ok(Some(record)),
                Some(columns) => Err(ErrorKind::UnequalNumFields {
                    expected_num: columns.len(),
                    num: record.len(),
                }),
                None => {
                    columns.replace(record);
                    Ok(None)
                }
            }
        } else {
            record
        }
    }

    // Field
    fn field(&mut self) -> Result<Field> {
        match self.curr_line.last() {
            Some(&c) if c == self.parser.quote => (self.parse_quote)(self),
            _ => self.parse_text(),
        }
    }

    // Field - Quote
    fn quote_default(&mut self) -> Result<Field> {
        // Remove initial quotation mark.
        self.curr_line.pop();

        let mut field = String::with_capacity(self.curr_line.len());

        loop {
            if self.curr_line.len() == 0 {
                self.read_line()?;
                field.push_str(&self.parser.newline);
            }

            match self.curr_line.pop() {
                Some(c) if c == self.parser.quote => {
                    let next = self.curr_line.last();
                    match next {
                        Some(&c) if c == self.parser.quote => {
                            field.push(self.parser.quote);
                            self.curr_line.pop();
                        }
                        Some(&c) if c != self.parser.separator => {
                            return Err(ErrorKind::BadField(String::from(
                                "Quoted fields cannot contain trailing unquoted values",
                            )))
                        }
                        _ => return Ok(field),
                    }
                }
                Some(c) => field.push(c),
                None => break,
            }
        }

        Err(ErrorKind::BadField(String::from(
            "Quoted field is missing closing quotation",
        )))
    }

    // Field - Text
    fn parse_text(&mut self) -> Result<Field> {
        (self.parse_text)(self).map(|field_chars| field_chars.iter().collect())
    }

    fn text_default(&mut self) -> Result<Vec<char>> {
        let mut field = Vec::with_capacity(self.curr_line.len());

        loop {
            match self.curr_line.last() {
                Some(&c) if c == self.parser.quote => {
                    return Err(ErrorKind::BadField(format!(
                        "Unquoted fields cannot contain quote character: `{}`",
                        self.parser.quote
                    )))
                }
                Some(&c) if c != self.parser.separator => field.push(c),
                _ => break,
            }
            self.curr_line.pop();
        }

        Ok(field)
    }

    fn text_ltrim(&mut self) -> Result<Vec<char>> {
        while self.curr_line.last().map_or(false, |c| c.is_whitespace()) {
            self.curr_line.pop();
        }
        self.text_default()
    }

    fn text_rtrim(&mut self) -> Result<Vec<char>> {
        self.text_default().map(|mut field_chars| {
            while field_chars.last().map_or(false, |c| c.is_whitespace()) {
                field_chars.pop();
            }
            field_chars
        })
    }

    fn text_trim(&mut self) -> Result<Vec<char>> {
        self.text_ltrim().map(|mut field_chars| {
            while field_chars.last().map_or(false, |c| c.is_whitespace()) {
                field_chars.pop();
            }
            field_chars
        })
    }
}

impl<'a, R> Iterator for Records<'a, R>
where
    R: Read,
{
    type Item = std::result::Result<Record, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.curr_line.clear();

        let bytes_read = match self.read_line() {
            Ok(bytes_read) => bytes_read,
            Err(kind) => return Some(Err(Error::new(self.csv.line_count, 0, kind))),
        };

        match bytes_read {
            None => None,
            Some(_n) => match self.record() {
                Ok(Some(record)) => Some(Ok(record)),
                Ok(None) => self.next(),
                Err(kind) => {
                    return Some(Err(Error::new(
                        self.csv.line_count,
                        self.csv.last_lines_len - self.curr_line.len() + 1,
                        kind,
                    )));
                }
            },
        }
    }
}

struct Lines<'a, B> {
    buf: B,
    newline: &'a [u8],
    last: u8,
    last_lines_len: usize,
    line_count: usize,
}

impl<'a, B> Lines<'a, B> {
    fn new(buf: B, newline: &'a str) -> Self {
        let newline = newline.as_bytes();
        let last = *newline.last().expect("newline cannot be empty") as u8;
        Lines {
            buf,
            newline,
            last,
            last_lines_len: 0,
            line_count: 0,
        }
    }
}

impl<'a, B: BufRead> Iterator for Lines<'a, B> {
    type Item = Result<Field>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = Vec::with_capacity(self.last_lines_len);
        loop {
            match self.buf.read_until(self.last, &mut buf) {
                Ok(0) if buf.len() == 0 => return None,
                Ok(0) => break,
                Ok(_n) if buf.ends_with(self.newline) => {
                    buf.truncate(buf.len() - self.newline.len());
                    break;
                }
                Ok(_n) => continue,
                Err(e) => return Some(Err(ErrorKind::Io(e))),
            };
        }
        return match String::from_utf8(buf) {
            Ok(s) => {
                self.last_lines_len = s.len();
                self.line_count += 1;
                Some(Ok(s))
            }
            Err(e) => Some(Err(ErrorKind::Utf8(e))),
        };
    }
}
*/
