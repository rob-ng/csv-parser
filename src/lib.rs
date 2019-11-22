use std::io::{BufRead, BufReader, Read};

pub struct Parser {
    separator: char,
    quote: char,
    columns: Option<Vec<String>>,
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            separator: ',',
            quote: '"',
            columns: None,
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

    pub fn columns(&mut self, columns: Vec<String>) -> &mut Self {
        self.columns.replace(columns);
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

        loop {
            let field = match self.field(csv) {
                Ok(field) => field,
                Err(msg) => return Err(format!("Malformed field: {}", msg)),
            };

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
                Some(_) => unreachable!(),
                None => break,
            }
        }

        match self.columns.as_ref() {
            None => Ok(fields),
            Some(columns) if columns.len() == fields.len() => Ok(fields),
            Some(columns) => Err(format!(
                "Number of fields {} does not match number of columns {}",
                fields.len(),
                columns.len()
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
                    Some(&c) if c == self.separator || c == '\n' => {
                        return Ok(field);
                    }
                    Some(_) => {
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

pub struct RecordIterator<'a, R>
where
    R: Read,
{
    parser: &'a Parser,
    csv: std::iter::Peekable<SourceIterator<R>>,
}

impl<'a, R> RecordIterator<'a, R>
where
    R: Read,
{
    pub fn new(csv_source: R, parser: &'a Parser) -> Self {
        let csv_reader = Source::new(csv_source);
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

// Private
struct Source<R>(BufReader<R>);

impl<R> Source<R>
where
    R: Read,
{
    pub fn new(source: R) -> Self {
        Source(BufReader::new(source))
    }
}

impl<R> IntoIterator for Source<R>
where
    R: Read,
{
    type Item = char;
    type IntoIter = SourceIterator<R>;

    fn into_iter(self) -> Self::IntoIter {
        SourceIterator::new(self)
    }
}

struct SourceIterator<R> {
    source: Source<R>,
    curr_line: Vec<char>,
}

impl<R> SourceIterator<R>
where
    R: Read,
{
    pub fn new(source: Source<R>) -> Self {
        SourceIterator {
            source,
            curr_line: vec![],
        }
    }
}

impl<R> Iterator for SourceIterator<R>
where
    R: Read,
{
    type Item = char;
    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_line.len() == 0 {
            let mut curr_line = String::new();
            self.source.0.read_line(&mut curr_line);
            self.curr_line = curr_line.chars().rev().collect();
        }
        self.curr_line.pop()
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
                let mut csv = Parser::new();
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
                let mut csv = Parser::new();
                let csv = csv.separator(',').quote('"');
                let csvreader = Source::new(given.as_bytes());
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
                let mut csv = Parser::new();
                let csv = csv.separator(',').quote('"');
                let csvreader = Source::new(given.as_bytes());
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
                        let mut csv = Parser::new();
                        let csv = csv.separator(',').quote('"');
                        let csvreader = Source::new(given.as_bytes());
                        let found = csv.record(&mut csvreader.into_iter().peekable());
                        that!(found).will_be_err().because(reason)
                    }));
                });
            });

            describe!(because_num_fields_doesnt_match_num_columns, {
                use crate::csv_tests::when_csv_is_malformed::*;
                it!(should_return_err, {
                    let mut csv = Parser::new();
                    let csv = csv
                        .separator(',')
                        .quote('"')
                        .columns(vec![String::from("h1"), String::from("h2")]);
                    let too_many_fields = "a,b,c";
                    let csvreader = Source::new(too_many_fields.as_bytes());
                    let found = csv.record(&mut csvreader.into_iter().peekable());
                    verify!(that!(found).will_be_err().because("Should return Err when number of fields in record does not match number of columns"));
                });
            });
        });

        describe!(because_field_is_malformed, {
            use crate::csv_tests::when_csv_is_malformed::*;
            it!(should_return_err_when_field_is_malformed, {
                verify_all!(MALFORMED_FIELDS.iter().map(|&(given, reason)| {
                    let mut csv = Parser::new();
                    let csv = csv.separator(',').quote('"');
                    let csvreader = Source::new(given.as_bytes());
                    let found = csv.field(&mut csvreader.into_iter().peekable());
                    that!(found).will_be_err().because(reason)
                }));
            });
        });
    });
});
