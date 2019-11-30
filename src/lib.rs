use std::io::{BufRead, BufReader, Read};

const SEPARATOR: char = ',';
const QUOTE: char = '"';

type Result<T> = std::result::Result<T, String>;

pub struct Parser<'a> {
    separator: char,
    quote: char,
    ignore_before_field: &'a dyn Fn(char) -> bool,
    ignore_after_field: &'a dyn Fn(char) -> bool,
    should_detect_columns: bool,
    columns: Option<Vec<String>>
}

impl<'a> Parser<'a> {
    pub fn new() -> Self {
        Parser {
            separator: SEPARATOR,
            quote: QUOTE,
            ignore_before_field: &|_| false,
            ignore_after_field: &|_| false,
            should_detect_columns: false,
            columns: None
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

    pub fn ltrim(&mut self) -> &mut Self {
       self.ignore_before_field = &|c| c.is_whitespace();
       self
    }

    pub fn rtrim(&mut self) -> &mut Self {
       self.ignore_after_field = &|c| c.is_whitespace();
       self
    }

    pub fn detect_columns(&mut self) -> &mut Self {
        self.should_detect_columns = true;
        self
    }

    pub fn columns(&mut self, columns: Vec<String>) -> &mut Self {
        self.columns.replace(columns);
        self
    }

   pub fn parse<R>(&self, csv_source: R) -> ParserIterator<R>
    where
        R: Read,
    {
        ParserIterator::new(csv_source, self)
    }
}

pub struct ParserIterator<'a, R>
where
    R: Read,
{
    parser: &'a Parser<'a>,
    csv: std::iter::Peekable<SourceIterator<R>>,
    columns: Option<Vec<String>>
}

impl<'a, R> ParserIterator<'a, R>
where
    R: Read,
{
    pub fn new(csv_source: R, parser: &'a Parser<'a>) -> Self {
        let csv_reader = Source::new(csv_source);
        ParserIterator {
            parser,
            csv: csv_reader.into_iter().peekable(),
            columns: parser.columns.clone()
        }
    }

    fn record(&mut self) -> Result<Option<Vec<String>>>
    {
        let mut fields = vec![];

        loop {
            let field = match self.field() {
                Ok(field) => field,
                Err(msg) => return Err(format!("Malformed field: {}", msg)),
            };

            fields.push(field);
            match self.csv.peek() {
                Some(&c) if c == self.parser.separator => {
                    self.csv.next();
                    continue;
                }
                Some(&c) if c == '\n' => {
                    self.csv.next();
                    break;
                }
                Some(_) => unreachable!(),
                None => break,
            }
        }

        match self.columns.as_mut() {
            None if self.parser.should_detect_columns => {
                self.columns.replace(fields);
                Ok(None)
            },
            None => {
                let standin_columns = vec![String::from(""); fields.len()];
                self.columns.replace(standin_columns);
                Ok(Some(fields))
            },
            Some(columns) if columns.len() == fields.len() => Ok(Some(fields)),
            Some(columns) => Err(format!(
                "Number of fields {} does not match number of columns {}",
                fields.len(),
                columns.len()
            )),
        }
    }

    fn field(&mut self) -> Result<String>
    {
        match self.csv.peek() {
            Some(&c) if c == self.parser.quote => self.string(),
            _ => self.text(),
        }
    }

    fn string(&mut self) -> Result<String>
    {
        // Remove initial quotation mark.
        self.csv.next();

        let mut field = String::new();

        while let Some(c) = self.csv.next() {
            if c == self.parser.quote {
                let next = self.csv.peek();
                match next {
                    Some(&c) if c == self.parser.quote => {
                        field.push(self.parser.quote);
                        self.csv.next();
                    }
                    Some(&c) if c != self.parser.separator && c != '\n' => return Err(String::from(
                        "String fields must be quoted in their entirety",
                    )),     
                    _ => return Ok(field)
                }
            } else {
                field.push(c);
            }
        }

        Err(String::from("String is missing closing quotation"))
    }

    fn text(&mut self) -> Result<String>
    {
        let mut field = vec![];

        loop {
            match self.csv.peek() {
                Some(&c) if (self.parser.ignore_before_field)(c) => self.csv.next(),
                _ => break
            };
        }

        loop {
            match self.csv.peek() {
                Some(&c) if c == self.parser.quote => return Err(format!("Unquoted fields cannot contain quote character: `{}`", self.parser.quote)),
                Some(&c) if c != self.parser.separator && c != '\n' => field.push(c),
                _ => break
            }
            self.csv.next();
        }

        loop {
            match field.last() {
                Some(&c) if (self.parser.ignore_after_field)(c) => field.pop(),
                _ => break
            };
        }

        let field: String = field.iter().collect();
        Ok(field)
    }
} 

type Record = Vec<String>;

impl<'a, R> Iterator for ParserIterator<'a, R>
where
    R: Read,
{
    type Item = Result<Record>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.csv.peek().is_none() {
            None
        } else {
            match self.record() {
                Ok(Some(record)) => Some(Ok(record)),
                Ok(None) => self.next(),
                Err(msg) => Some(Err(msg))
            }
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

    pub fn run_tests_pass(parser: Parser, tests: &[(&str, Vec<Vec<&str>>, &str)]) {
        verify_all!(tests.iter().map(|(given, expected, reason)| {
            let found: Result<Vec<Vec<String>>> = parser.parse(given.as_bytes()).collect();
            let reason = format!("{}. Given: {}", reason, given);
            match &found {
                Ok(found) => {
                    let expected: Vec<Vec<String>> = expected
                        .iter()
                        .map(|v| v.iter().map(|v| v.to_string()).collect())
                        .collect();
                    that!(found).will_equal(&expected).because(&reason) 
                }
                Err(_) => that!(found).will_be_ok().because(&reason),
            }
        }));
    }

    pub fn run_tests_fail(parser: Parser, tests: &[(&str, &str)]) {
        verify_all!(tests.iter().map(|(given, reason)| {
            let found: Result<Vec<Vec<String>>> = parser.parse(given.as_bytes()).collect();
            let reason = format!("{}. Given: {}", reason, given);
            that!(found).will_be_err().because(&reason)
        }));
    }

    describe!(configuration, {
        describe!(detect_columns, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(should_treat_first_row_in_csv_as_header_instead_of_record, {
                    let tests = [(
                        "a,b,c\nd,e,f\ng,h,i",
                        vec![
                            vec!["d", "e", "f"],
                            vec!["g", "h", "i"],
                        ],
                        "Turning on `detect_columns` should prevent first row from being returned as a record",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').detect_columns();
                    run_tests_pass(parser, &tests);
                });

                describe!(and_explicit_columns_have_also_been_given, {
                    use crate::parser_tests::*;
                    it!(should_treat_first_row_as_record, {
                        let tests = [(
                            "a,b,c\nd,e,f\ng,h,i",
                            vec![
                                vec!["a", "b", "c"],
                                vec!["d", "e", "f"],
                                vec!["g", "h", "i"],
                            ],
                            "First row should be treated as a record when 'detect_columns' is off (default)",
                        )];
                        let mut parser = Parser::new();
                        parser.separator(',').quote('"').rtrim().detect_columns().columns(vec![String::from("a"), String::from("b"), String::from("c")]);
                        run_tests_pass(parser, &tests);
                    });
                });
            });

            describe!(when_off, {
                use crate::parser_tests::*;
                it!(should_treat_first_row_as_record, {
                    let tests = [(
                        "a,b,c\nd,e,f\ng,h,i",
                        vec![
                            vec!["a", "b", "c"],
                            vec!["d", "e", "f"],
                            vec!["g", "h", "i"],
                        ],
                        "First row should be treated as a record when 'detect_columns' is off (default)",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"');
                    run_tests_pass(parser, &tests);
                });
            });
        });

        describe!(rtrim, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(should_ignore_whitespace_to_left_of_fields, {
                    let tests = [(
                        "a   ,b  \u{A0},c   \u{3000}\nd ,e   ,f\ng \t\t,h,i \u{A0}\u{3000}\t",
                        vec![
                            vec!["a", "b", "c"],
                            vec!["d", "e", "f"],
                            vec!["g", "h", "i"],
                        ],
                        "Turning on `ltrim` should remove all types of whitespace before fields",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').rtrim();
                    run_tests_pass(parser, &tests);
                });
            });

            describe!(when_off, {
                use crate::parser_tests::*;
                it!(should_keep_whitespace_to_left_of_fields, {
                    let tests = [(
                        "a   ,b  \u{A0},c   \u{3000}\nd ,e   ,f\ng \t\t,h,i \u{A0}\u{3000}\t",
                        vec![
                            vec!["a   ", "b  \u{A0}", "c   \u{3000}"],
                            vec!["d ", "e   ", "f"],
                            vec!["g \t\t", "h", "i \u{A0}\u{3000}\t"],
                        ],
                        "Whitespace before fields should not be removed when `ltrim` is off (default)",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"');
                    run_tests_pass(parser, &tests);
                });
            });
        });

        describe!(ltrim, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(should_ignore_whitespace_to_left_of_fields, {
                    let tests = [(
                        "   a,  \u{A0}b,   \u{3000}c\n d,   e,f\n \t\tg,h, \u{A0}\u{3000}\ti",
                        vec![
                            vec!["a", "b", "c"],
                            vec!["d", "e", "f"],
                            vec!["g", "h", "i"],
                        ],
                        "Turning on `ltrim` should remove all types of whitespace before fields",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').ltrim();
                    run_tests_pass(parser, &tests);
                });
            });

            describe!(when_off, {
                use crate::parser_tests::*;
                it!(should_keep_whitespace_to_left_of_fields, {
                    let tests = [(
                        "   a,  \u{A0}b,   \u{3000}c\n d,   e,f\n \t\tg,h, \u{A0}\u{3000}\ti",
                        vec![
                            vec!["   a", "  \u{A0}b", "   \u{3000}c"],
                            vec![" d", "   e", "f"],
                            vec![" \t\tg", "h", " \u{A0}\u{3000}\ti"],
                        ],
                        "Whitespace before fields should not be removed when `ltrim` is off (default)",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"');
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
                    "abc,\"def\n\"\"ghi\"\"\",jkl\nm,n,o",
                    vec![vec!["abc", "def\n\"ghi\"", "jkl"], vec!["m", "n", "o"]],
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
            parser.separator(',').quote('"');
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
                parser.separator(',').quote('"');
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
                        parser
                            .separator(',')
                            .quote('"');
                        run_tests_fail(parser, &tests);
                    });
                }
            );
        });
    });
});
