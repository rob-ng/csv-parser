use crate::error::{Error, ErrorKind};
use crate::Parser;
use std::io::{BufRead, BufReader, Read};

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
            // TODO MAKE THIS PART OF READING AND ACCOUNT FOR UNHANDLEABLE ERRORS
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
        self.csv.next().map_or(Ok(None), |line_or_err| {
            line_or_err.and_then(|line| {
                if line.trim().is_empty() {
                    self.curr_line.clear();
                    self.read_line_no_empty()
                } else {
                    self.curr_line.extend(line.chars().rev());
                    Ok(Some(line.len()))
                }
            })
        })
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
        let mut fields = vec![];

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
        // TODO Only skip parsing errors, not IO or UTF8
        record.or(Ok(None))
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

        let mut field = String::new();

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
        let mut field = vec![];

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
