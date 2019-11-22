use std::io::{BufRead, BufReader, Read};

pub struct Parser<'a> {
    separator: char,
    quote: char,
    columns: Option<Vec<String>>,

    should_ltrim: bool,
    ignore_before_field: &'a Fn(char) -> bool,
    //rtrim: bool,
    //skip_empty_rows: bool,
    //max_record_size: usize
}

impl<'a> Parser<'a> {
    pub fn new() -> Self {
        Parser {
            separator: ',',
            quote: '"',
            columns: None,
            should_ltrim: false,
            ignore_before_field: &|_| false,
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

    pub fn ltrim(&mut self) -> &mut Self {
        self.should_ltrim = true;
        self.ignore_before_field = &|c| c.is_whitespace();
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

    fn field<R>(&self, csv: &mut std::iter::Peekable<R>) -> Result<String, String>
    where
        R: Iterator<Item = char>,
    {
        loop {
            match csv.peek() {
                Some(&c) if (self.ignore_before_field)(c) => csv.next(),
                _ => break,
            };
        }

        match csv.peek() {
            Some(&c) if c == self.quote => self.string(csv),
            _ => self.text(csv),
        }
    }

    fn string<R>(&self, csv: &mut std::iter::Peekable<R>) -> Result<String, String>
    where
        R: Iterator<Item = char>,
    {
        // Remove initial quotation mark.
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

    fn text<R>(&self, csv: &mut std::iter::Peekable<R>) -> Result<String, String>
    where
        R: Iterator<Item = char>,
    {
        let mut field = String::new();

        loop {
            match csv.peek() {
                Some(&c) if c == self.quote => {
                    // TODO Format to include `Parser`'s quotation mark
                    return Err(String::from(
                        "Unquoted fields cannot contain quotation marks.",
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
}

pub struct RecordIterator<'a, R>
where
    R: Read,
{
    parser: &'a Parser<'a>,
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
describe!(parser_tests, {
    pub use super::*;

    pub fn run_tests_pass(parser: &Parser, tests: &[(&str, Vec<Vec<&str>>, &str)]) {
        verify_all!(tests.iter().map(|(given, expected, reason)| {
            let found: Result<Vec<Vec<String>>, String> = parser.parse(given.as_bytes()).collect();
            match &found {
                Ok(found) => {
                    let expected: Vec<Vec<String>> = expected
                        .iter()
                        .map(|v| v.iter().map(|v| v.to_string()).collect())
                        .collect();
                    that!(found).will_equal(&expected).because(reason)
                }
                Err(_) => that!(found).will_be_ok(),
            }
        }));
    }

    pub fn run_tests_fail(parser: &Parser, tests: &[(&str, &str)]) {
        verify_all!(tests.iter().map(|(given, reason)| {
            let found: Result<Vec<Vec<String>>, String> = parser.parse(given.as_bytes()).collect();
            that!(found).will_be_err().because(reason)
        }));
    }

    describe!(configuration, {
        describe!(ltrim, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(should_ignore_whitespace_to_left_of_fields, {
                    let tests = [(
                        "   a,  \u{A0}b,   \u{3000}c\n d,   e,f\n   \"g\",\t\"h\",\t  \"i\"\n",
                        vec![
                            vec!["a", "b", "c"],
                            vec!["d", "e", "f"],
                            vec!["g", "h", "i"],
                        ],
                        "Turning on `ltrim` should remove all types of whitespace before fields",
                    )];
                    let mut parser = Parser::new();
                    let parser = parser.separator(',').quote('"').ltrim();
                    run_tests_pass(parser, &tests);
                });
            });

            describe!(when_off, {
                use crate::parser_tests::*;
                it!(should_keep_whitespace_to_left_of_fields, {
                    let tests = [(
                        "   a,  \u{A0}b,   \u{3000}c\n d,   e,f\n   g,\th,\t  i\n",
                        vec![
                            vec!["   a", "  \u{A0}b", "   \u{3000}c"],
                            vec![" d", "   e", "f"],
                            vec!["   g", "\th", "\t  i"],
                        ],
                        "Whitespace before fields should not be removed when `ltrim` is off (default)",
                    )];
                    let mut parser = Parser::new();
                    let parser = parser.separator(',').quote('"');
                    run_tests_pass(parser, &tests);
                });
            });
        });
    });

    describe!(when_csv_is_wellformed, {
        use super::*;
        it!(should_correctly_parse_files, {
            let tests = [
                (
                    "a,b,c\nd,e,f\ng,h,i\n",
                    vec![
                        vec!["a", "b", "c"],
                        vec!["d", "e", "f"],
                        vec!["g", "h", "i"],
                    ],
                    "Should parse entire CSV successfully when all records are well-formed",
                ),
                (
                    "\"abc\n\"\ndef\n",
                    vec![vec!["abc\n"], vec!["def"]],
                    "Should ignore newlines inside quoted fields",
                ),
                (
                    "\"abc,\"\ndef\n",
                    vec![vec!["abc,"], vec!["def"]],
                    "Should ignore separators inside quoted fields",
                ),
                (
                    "abc,\"def\n\"\"ghi\"\"\",jkl\nmno",
                    vec![vec!["abc", "def\n\"ghi\"", "jkl"], vec!["mno"]],
                    "Should handle combinations of quoted and unquoted fields",
                ),
                (
                    "\"\"\"\"\"\"\"\"\"a\"\"\"\"\"\"\"\"\",b,c\nd,e,f",
                    vec![vec!["\"\"\"\"a\"\"\"\"", "b", "c"], vec!["d", "e", "f"]],
                    "Should handle arbitrary numbers of escaped inner quotes",
                ),
                (
                    ",,,\na,b,c,d",
                    vec![vec!["", "", "", ""], vec!["a", "b", "c", "d"]],
                    "Should allow empty fields",
                ),
            ];
            let mut parser = Parser::new();
            let parser = parser.separator(',').quote('"');
            run_tests_pass(parser, &tests);
        });
    });

    describe!(when_csv_is_malformed, {
        describe!(because_a_field_is_malformed, {
            pub use crate::parser_tests::*;
            it!(should_return_an_err_when_parse_results_are_collected, {
                let tests = [
                    ("ab\"cd", "Non-quoted fields cannot contain quotation marks"),
                    (
                        "\"abc\"def",
                        "Quoted fields cannot contain trailing unquoted values",
                    ),
                    (
                        "\"def\n\"\"ghi\"\"",
                        "Quoted fields must include both open and closing quotations",
                    ),
                ];
                let mut parser = Parser::new();
                let parser = parser.separator(',').quote('"');
                run_tests_fail(parser, &tests);
            });

            describe!(
                because_a_record_has_more_or_fewer_fields_than_number_of_columns,
                {
                    pub use crate::parser_tests::*;
                    it!(should_return_err, {
                        let tests = [
                            (
                                "a,b\nd",
                                "Records cannot have fewer fields than there are columns",
                            ),
                            (
                                "a,b\nd,e,f",
                                "Records cannot have more fields than there are columns",
                            ),
                        ];
                        let mut parser = Parser::new();
                        let parser = parser
                            .separator(',')
                            .quote('"')
                            .columns(vec![String::from("h1"), String::from("h2")]);
                        run_tests_fail(parser, &tests);
                    });
                }
            );
        });
    });
});
