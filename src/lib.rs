use std::io::{BufRead, BufReader, Read};

struct CSVReader<R>
where
    R: Read,
{
    buf: BufReader<R>,
}

impl<R> CSVReader<R>
where
    R: Read,
{
    pub fn new(to_read: R) -> Self {
        CSVReader {
            buf: BufReader::new(to_read),
        }
    }
}

impl<R> IntoIterator for CSVReader<R>
where
    R: Read,
{
    type Item = char;
    type IntoIter = CSVReaderIterator<R>;

    fn into_iter(self) -> Self::IntoIter {
        CSVReaderIterator::new(self)
    }
}

struct CSVReaderIterator<R>
where
    R: Read,
{
    buf: BufReader<R>,
    curr_line: String,
}

impl<R> CSVReaderIterator<R>
where
    R: Read,
{
    pub fn new(mut csv_reader: CSVReader<R>) -> Self {
        let mut curr_line = String::new();
        csv_reader.buf.read_line(&mut curr_line);
        CSVReaderIterator {
            buf: csv_reader.buf,
            curr_line,
        }
    }
}

impl<R> Iterator for CSVReaderIterator<R>
where
    R: Read,
{
    type Item = char;
    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_line.len() == 0 {
            self.buf.read_line(&mut self.curr_line);
        }
        let next = self.curr_line.chars().next();
        if next.is_some() {
            self.curr_line.drain(0..1);
        }
        next
    }
}

pub struct RecordIterator<'a, R>
where
    R: Read,
{
    parser: &'a CSV,
    csv: std::iter::Peekable<CSVReaderIterator<R>>,
}

impl<'a, R> RecordIterator<'a, R>
where
    R: Read,
{
    pub fn new(csv_source: R, parser: &'a CSV) -> Self {
        let csv_reader = CSVReader::new(csv_source);
        RecordIterator {
            parser,
            csv: csv_reader.into_iter().peekable(),
        }
    }
}

type Record = Vec<String>;

impl<'a, R> Iterator for RecordIterator<'a, R>
where
    R: Read,
{
    type Item = Result<Record, String>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.csv.peek().is_none() {
            None
        } else {
            Some(self.parser.record(&mut self.csv))
        }
    }
}

pub struct CSV {
    separator: char,
    quote: char,
    headers: Option<Vec<String>>,
}

impl CSV {
    pub fn new() -> Self {
        CSV {
            separator: ',',
            quote: '"',
            headers: None,
        }
    }

    pub fn separator(&mut self, separator: char) -> &mut Self {
        self.separator = separator;
        self
    }

    pub fn quote(&mut self, quote: char) -> &mut Self {
        self.quote = quote;
        self
    }

    pub fn headers(&mut self, headers: Vec<String>) -> &mut Self {
        self.headers.replace(headers);
        self
    }

    pub fn parse<R>(&self, csv_source: R) -> RecordIterator<R>
    where
        R: Read,
    {
        RecordIterator::new(csv_source, self)
    }

    fn record<R>(&self, csv: &mut std::iter::Peekable<R>) -> Result<Vec<String>, String>
    where
        R: Iterator<Item = char>,
    {
        let mut fields = vec![];

        while let field = self.field(csv) {
            match field {
                Ok(field) => {
                    fields.push(field);
                    match csv.peek() {
                        Some(&c) if c == self.separator => {
                            csv.next();
                            // TODO This needs to be configurable
                            while let Some(' ') = csv.peek() {
                                csv.next();
                            }
                            continue;
                        }
                        Some(&c) if c == '\n' => {
                            csv.next();
                            break;
                        }
                        Some(c) => unreachable!(),
                        None => break,
                    }
                }
                Err(msg) => return Err(format!("Malformed field: {}", msg)),
            }
        }

        match self.headers.as_ref() {
            None => Ok(fields),
            Some(headers) if headers.len() == fields.len() => Ok(fields),
            Some(headers) => Err(format!(
                "Number of fields {} does not match number of headers {}",
                fields.len(),
                headers.len()
            )),
        }
    }

    fn string<R>(&self, csv: &mut std::iter::Peekable<R>) -> Result<String, String>
    where
        R: Iterator<Item = char>,
    {
        // Remove initial quoation mark.
        csv.next();

        let mut field = String::new();

        while let Some(c) = csv.next() {
            if c == self.quote {
                let next = csv.peek();
                match next {
                    Some(&c) if c == self.quote => {
                        field.push(self.quote);
                        csv.next();
                    }
                    Some(&c) if c == ',' || c == '\n' => {
                        return Ok(field);
                    }
                    Some(&c) => {
                        return Err(String::from(
                            "String fields must be quoted in their entirety",
                        ))
                    }
                    None => {
                        return Ok(field);
                    }
                }
            } else {
                field.push(c);
            }
        }

        Err(String::from("String is missing closing quotation"))
    }

    fn text<'a, R>(&self, csv: &mut std::iter::Peekable<R>) -> Result<String, String>
    where
        R: Iterator<Item = char>,
    {
        let mut field = String::new();

        loop {
            match csv.peek() {
                Some(&c) if c == self.quote => {
                    return Err(String::from(
                        "Unquoted fields cannot contain quatation marks.",
                    ));
                }
                Some(&c) if c == self.separator || c == '\n' => {
                    return Ok(field);
                }
                Some(&c) => field.push(c),
                None => break,
            }
            csv.next();
        }

        for c in csv {
            if c == self.quote {
                return Err(String::from(
                    "Unquoted fields cannot contain quatation marks.",
                ));
            }
            if c == self.separator || c == '\n' {
                return Ok(field);
            }
            field.push(c);
        }

        Ok(field)
    }

    fn field<'a, R>(&self, csv: &mut std::iter::Peekable<R>) -> Result<String, String>
    where
        R: Iterator<Item = char>,
    {
        match csv.peek() {
            Some(&c) if c == self.quote => self.string(csv),
            _ => self.text(csv),
        }
    }
}

#[cfg(test)]
use jestr::*;

#[cfg(test)]
describe!(csv_tests, {
    pub use super::*;
    describe!(when_csv_is_wellformed, {
        use super::*;
        it!(should_correctly_parse_files, {
            let tests = [(
                "a,b,c\nd,e,f\ng,h,i\n",
                vec![
                    vec!["a", "b", "c"],
                    vec!["d", "e", "f"],
                    vec!["g", "h", "i"],
                ],
            )];
            verify_all!(tests.iter().map(|(given, expected)| {
                let mut csv = CSV::new();
                let csv = csv.separator(',').quote('"');
                let found: Vec<Vec<String>> =
                    csv.parse(given.as_bytes()).filter_map(|v| v.ok()).collect();
                let expected: Vec<Vec<String>> = expected
                    .iter()
                    .map(|v| v.iter().map(|v| v.to_string()).collect())
                    .collect();
                that!(found).will_equal(expected)
            }))
        });

        it!(should_correctly_parse_records, {
            let tests = [
                ("a,b,c", vec!["a", "b", "c"]),
                (",,,", vec!["", "", "", ""]),
                ("abc\ndef", vec!["abc"]),
                ("\"abc\ndef\"", vec!["abc\ndef"]),
                ("\"abc,def\"", vec!["abc,def"]),
                ("\"abc\n\"\ndef\n", vec!["abc\n"]),
                (
                    "abc,\"def\n\"\"ghi\"\"\",jkl\nmno",
                    vec!["abc", "def\n\"ghi\"", "jkl"],
                ),
            ];
            verify_all!(tests.iter().map(|(given, expected)| {
                let mut csv = CSV::new();
                let csv = csv.separator(',').quote('"');
                let csvreader = CSVReader::new(given.as_bytes());
                let found = csv.record(&mut csvreader.into_iter().peekable());
                let expected = expected.iter().map(|v| v.to_string()).collect();
                that!(found).will_unwrap_to(expected)
            }));
        });

        it!(should_correctly_parse_fields, {
            let tests = [
                (",", ""),
                ("abc", "abc"),
                ("abc,def", "abc"),
                ("abc\ndef", "abc"),
                ("\"abc,def\"", "abc,def"),
                ("\"abc\ndef\"", "abc\ndef"),
                ("\"abc\ndef\"\n", "abc\ndef"),
                ("\"\"\"abc\"\"def\"", "\"abc\"def"),
                (
                    "\"Quote \"\"Inner quote and \"\"even more inner quote\"\"\"\"\"",
                    "Quote \"Inner quote and \"even more inner quote\"\"",
                ),
            ];
            verify_all!(tests.iter().map(|&(given, expected)| {
                let mut csv = CSV::new();
                let csv = csv.separator(',').quote('"');
                let csvreader = CSVReader::new(given.as_bytes());
                let found = csv.field(&mut csvreader.into_iter().peekable());
                that!(found).will_unwrap_to(String::from(expected))
            }));
        });
    });

    describe!(when_csv_is_malformed, {
        use super::*;
        pub const MALFORMED_FIELDS: &[(&str, &str)] = &[
            ("ab\"cd", "Non-string fields cannot contain quotation marks"),
            (
                "\"abc\"def",
                "String fields must be quoted in their entirety",
            ),
            (
                "\"def\n\"\"ghi\"\"",
                "String fields must include both open and closing quotations",
            ),
        ];

        describe!(because_row_is_malformed, {
            describe!(because_field_is_malformed, {
                use crate::csv_tests::when_csv_is_malformed::*;
                it!(should_return_err, {
                    let tests: Vec<(String, &str)> = MALFORMED_FIELDS
                        .iter()
                        .map(|&(field, reason)| (format!("first,{},last", field), reason))
                        .collect();
                    verify_all!(tests.iter().map(|(given, reason)| {
                        let mut csv = CSV::new();
                        let csv = csv.separator(',').quote('"');
                        let csvreader = CSVReader::new(given.as_bytes());
                        let found = csv.record(&mut csvreader.into_iter().peekable());
                        that!(found).will_be_err().because(reason)
                    }));
                });
            });

            describe!(because_num_fields_doesnt_match_num_headers, {
                use crate::csv_tests::when_csv_is_malformed::*;
                it!(should_return_err, {
                    let mut csv = CSV::new();
                    let csv = csv
                        .separator(',')
                        .quote('"')
                        .headers(vec![String::from("h1"), String::from("h2")]);
                    let too_many_fields = "a,b,c";
                    let csvreader = CSVReader::new(too_many_fields.as_bytes());
                    let found = csv.record(&mut csvreader.into_iter().peekable());
                    verify!(that!(found).will_be_err().because("Should return Err when number of fields in record does not match number of headers"));
                });
            });
        });

        describe!(because_field_is_malformed, {
            use crate::csv_tests::when_csv_is_malformed::*;
            it!(should_return_err_when_field_is_malformed, {
                verify_all!(MALFORMED_FIELDS.iter().map(|&(given, reason)| {
                    let mut csv = CSV::new();
                    let csv = csv.separator(',').quote('"');
                    let csvreader = CSVReader::new(given.as_bytes());
                    let found = csv.field(&mut csvreader.into_iter().peekable());
                    that!(found).will_be_err().because(reason)
                }));
            });
        });
    });
});
