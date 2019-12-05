use std::io::{BufRead, BufReader, Read};
use std::iter::Peekable;

const SEPARATOR: char = ',';
const QUOTE: char = '"';

type Result<T> = std::result::Result<T, String>;

macro_rules! config {
    ($name:ident, $field:ident) => {
        pub fn $name(&mut self, value: bool) -> &mut Self {
            self.$field = value;
            self
        }
    };

    ($name:ident, $field:ident, $value_type:ty) => {
        pub fn $name(&mut self, value: $value_type) -> &mut Self {
            self.$field = value;
            self
        }
    };
}

pub struct Parser {
    // Special characters
    quote: char,
    separator: char,
    newline: String,
    // Behavior
    should_detect_columns: bool,
    columns: Option<Vec<String>>,
    should_ltrim_fields: bool,
    should_rtrim_fields: bool,
    should_skip_rows_with_error: bool,
    should_skip_empty_rows: bool,
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            quote: QUOTE,
            separator: SEPARATOR,
            newline: String::from("\n"),
            should_detect_columns: false,
            columns: None,
            should_ltrim_fields: false,
            should_rtrim_fields: false,
            should_skip_rows_with_error: false,
            should_skip_empty_rows: true,
        }
    }

    config!(separator, separator, char);

    config!(quote, quote, char);

    config!(newline, newline, String);

    config!(ltrim, should_ltrim_fields);

    config!(rtrim, should_rtrim_fields);

    config!(detect_columns, should_detect_columns);

    pub fn columns(&mut self, columns: Vec<String>) -> &mut Self {
        self.columns.replace(columns);
        self
    }

    config!(skip_empty_rows, should_skip_empty_rows);

    config!(skip_rows_with_error, should_skip_rows_with_error);

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
    csv: Peekable<Lines<'a, BufReader<R>>>,
    curr_line: Vec<char>,
    columns: Option<Vec<String>>,
    parser: &'a Parser,
    parse_quote: &'a dyn Fn(&mut Self) -> Result<String>,
    parse_text: &'a dyn Fn(&mut Self) -> Result<String>,
    parse_record: &'a dyn Fn(&mut Self) -> Result<Option<Vec<String>>>,
    record_middleware: Vec<
        &'a dyn Fn(
            Result<Option<Vec<String>>>,
            &mut Option<Vec<String>>,
        ) -> Result<Option<Vec<String>>>,
    >,
    read_line: &'a dyn Fn(&mut Self) -> Option<usize>,
}

impl<'a, R> ParserIterator<'a, R>
where
    R: Read,
{
    fn new(csv_source: R, parser: &'a Parser) -> Self {
        let parse_quote = &|s: &mut Self| ParserIterator::quote_default(s);

        let parse_text: &'a dyn Fn(&mut Self) -> Result<String> =
            match (parser.should_ltrim_fields, parser.should_rtrim_fields) {
                (true, true) => &|s: &mut Self| ParserIterator::text_trim(s),
                (true, false) => &|s: &mut Self| ParserIterator::text_ltrim(s),
                (false, true) => &|s: &mut Self| ParserIterator::text_rtrim(s),
                (false, false) => &|s: &mut Self| ParserIterator::text_default(s),
            };

        let parse_record: &'a dyn Fn(&mut Self) -> Result<Option<Vec<String>>> =
            &|s: &mut Self| ParserIterator::record_default(s);

        let mut record_middleware: Vec<
            &'a dyn Fn(
                Result<Option<Vec<String>>>,
                &mut Option<Vec<String>>,
            ) -> Result<Option<Vec<String>>>,
        > = vec![];
        let skip_rows_with_error: &'a dyn Fn(
            Result<Option<Vec<String>>>,
            &mut Option<Vec<String>>,
        ) -> Result<Option<Vec<String>>> = match parser.should_detect_columns {
            true => &|record, columns| ParserIterator::<R>::record_column_detect(record, columns),
            false => &|record, columns| ParserIterator::<R>::record_column_default(record, columns),
        };
        record_middleware.push(skip_rows_with_error);
        if parser.should_skip_rows_with_error {
            let skip_rows_with_error: &'a dyn Fn(
                Result<Option<Vec<String>>>,
                &mut Option<Vec<String>>,
            ) -> Result<Option<Vec<String>>> = &|record, _| record.or(Ok(None));
            record_middleware.push(skip_rows_with_error);
        }

        let read_line: &'a dyn Fn(&mut Self) -> Option<usize> = if parser.should_skip_empty_rows {
            &|slf| ParserIterator::read_line_no_empty(slf)
        } else {
            &|slf| ParserIterator::read_line_default(slf)
        };

        ParserIterator {
            parser,
            csv: Lines::new(BufReader::new(csv_source), parser.newline.as_ref()).peekable(),
            curr_line: vec![],
            columns: parser.columns.clone(),
            parse_quote,
            parse_text,
            parse_record,
            record_middleware,
            read_line,
        }
    }

    fn read_line(&mut self) -> Option<usize> {
        (self.read_line)(self)
    }

    fn read_line_no_empty(&mut self) -> Option<usize> {
        match self.csv.next() {
            Some(line) => match line {
                Ok(line) => {
                    if line.trim().is_empty() {
                        self.curr_line.clear();
                        self.read_line_no_empty()
                    } else {
                        self.curr_line.extend(line.chars().rev());
                        Some(line.len())
                    }
                }
                Err(msg) => panic!("Failed to read line from CSV file: {}", msg),
            },
            _ => None,
        }
    }

    fn read_line_default(&mut self) -> Option<usize> {
        match self.csv.next() {
            Some(line) => match line {
                Ok(line) => {
                    self.curr_line.extend(line.chars().rev());
                    Some(line.len())
                }
                Err(msg) => panic!("Failed to read line from CSV file: {}", msg),
            },
            _ => None,
        }
    }

    // Record
    fn record(&mut self) -> Result<Option<Vec<String>>> {
        let record = (self.parse_record)(self);
        let columns = &mut self.columns;
        self.record_middleware
            .iter()
            .fold(record, move |record, mw| mw(record, columns))
    }

    fn record_default(&mut self) -> Result<Option<Vec<String>>> {
        let mut fields = vec![];

        loop {
            let field = match self.field() {
                Ok(field) => field,
                Err(msg) => {
                    self.curr_line.clear();
                    return Err(format!("Malformed field: {}", msg));
                }
            };

            fields.push(field);

            match self.curr_line.last() {
                Some(&c) if c == self.parser.separator => {
                    self.curr_line.pop();
                    continue;
                }
                Some(&c) if c == '\n' => {
                    self.curr_line.pop();
                    break;
                }
                Some(_) => unreachable!(),
                None => break,
            }
        }

        Ok(Some(fields))
    }

    fn record_column_default(
        fields: Result<Option<Vec<String>>>,
        columns: &mut Option<Vec<String>>,
    ) -> Result<Option<Vec<String>>> {
        fields.and_then(|fields_option| match fields_option {
            Some(fields) => match columns.as_mut() {
                Some(columns) if columns.len() == fields.len() => Ok(Some(fields)),
                Some(columns) => Err(format!(
                    "Number of fields {} does not match number of columns {}",
                    fields.len(),
                    columns.len()
                )),
                None => {
                    let standin_columns = vec![String::from(""); fields.len()];
                    columns.replace(standin_columns);
                    Ok(Some(fields))
                }
            },
            _ => Ok(fields_option),
        })
    }

    fn record_column_detect(
        fields: Result<Option<Vec<String>>>,
        columns: &mut Option<Vec<String>>,
    ) -> Result<Option<Vec<String>>> {
        fields.and_then(|fields_option| match fields_option {
            Some(fields) => match columns.as_mut() {
                Some(columns) if columns.len() == fields.len() => Ok(Some(fields)),
                Some(columns) => Err(format!(
                    "Number of fields {} does not match number of columns {}",
                    fields.len(),
                    columns.len()
                )),
                None => {
                    columns.replace(fields);
                    Ok(None)
                }
            },
            _ => Ok(fields_option),
        })
    }

    // Field
    fn field(&mut self) -> Result<String> {
        match self.curr_line.last() {
            Some(&c) if c == self.parser.quote => (self.parse_quote)(self),
            _ => (self.parse_text)(self),
        }
    }

    // Field - Quote
    fn quote_default(&mut self) -> Result<String> {
        // Remove initial quotation mark.
        self.curr_line.pop();

        let mut field = String::new();

        loop {
            if self.curr_line.len() == 0 {
                self.read_line();
                field.push_str(self.parser.newline.as_ref());
            }

            match self.curr_line.pop() {
                Some(c) => {
                    if c == self.parser.quote {
                        let next = self.curr_line.last();
                        match next {
                            Some(&c) if c == self.parser.quote => {
                                field.push(self.parser.quote);
                                self.curr_line.pop();
                            }
                            Some(&c) if c != self.parser.separator => {
                                return Err(String::from(
                                    "String fields must be quoted in their entirety",
                                ))
                            }
                            _ => return Ok(field),
                        }
                    } else {
                        field.push(c);
                    }
                }
                None => break,
            }
        }

        Err(String::from("String is missing closing quotation"))
    }

    // Field - Text
    fn text_default(&mut self) -> Result<String> {
        self.text_base()
            .map(|field_chars| field_chars.iter().collect())
    }

    fn text_ltrim(&mut self) -> Result<String> {
        loop {
            match self.curr_line.last() {
                Some(&c) if self.parser.should_ltrim_fields && c.is_whitespace() => {
                    self.curr_line.pop()
                }
                _ => break,
            };
        }
        self.text_base()
            .map(|field_chars| field_chars.iter().collect())
    }

    fn text_rtrim(&mut self) -> Result<String> {
        self.text_base().map(|mut field_chars| {
            loop {
                match field_chars.last() {
                    Some(&c) if self.parser.should_rtrim_fields && c.is_whitespace() => {
                        field_chars.pop()
                    }
                    _ => break,
                };
            }
            field_chars.iter().collect()
        })
    }

    fn text_trim(&mut self) -> Result<String> {
        loop {
            match self.curr_line.last() {
                Some(&c) if self.parser.should_ltrim_fields && c.is_whitespace() => {
                    self.curr_line.pop()
                }
                _ => break,
            };
        }
        self.text_rtrim()
    }

    fn text_base(&mut self) -> Result<Vec<char>> {
        let mut field = vec![];

        loop {
            match self.curr_line.last() {
                Some(&c) if c == self.parser.quote => {
                    return Err(format!(
                        "Unquoted fields cannot contain quote character: `{}`",
                        self.parser.quote
                    ))
                }
                Some(&c) if c != self.parser.separator && c != '\n' => field.push(c),
                _ => break,
            }
            self.curr_line.pop();
        }
        Ok(field)
    }
}

impl<'a, R> Iterator for ParserIterator<'a, R>
where
    R: Read,
{
    type Item = Result<Vec<String>>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.read_line() {
            None => None,
            Some(_) => match self.record() {
                Ok(Some(record)) => Some(Ok(record)),
                Ok(None) => self.next(),
                Err(msg) => Some(Err(msg)),
            },
        }
    }
}

struct Lines<'a, B> {
    buf: B,
    newline: &'a [u8],
    last: u8,
    last_lines_len: usize,
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
        }
    }
}

impl<'a, B: BufRead> Iterator for Lines<'a, B> {
    type Item = Result<String>;

    // TODO Handle different kinds of errors
    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = Vec::with_capacity(self.last_lines_len);
        loop {
            match self.buf.read_until(self.last, &mut buf) {
                Ok(0) => {
                    if buf.len() == 0 {
                        return None;
                    }
                    break;
                }
                Ok(_n) => {
                    if buf.ends_with(self.newline) {
                        buf.truncate(buf.len() - self.newline.len());
                        break;
                    }
                }
                Err(_e) => return Some(Err(String::from("Failed to read line"))),
            };
        }
        return match String::from_utf8(buf) {
            Ok(s) => {
                self.last_lines_len = s.len();
                Some(Ok(s))
            }
            Err(_e) => Some(Err(String::from("Not valid UTF-8"))),
        };
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
            let reason = format!("{}.\nGiven:\n{}", reason, given);
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
            let reason = format!("{}.\nGiven:\n{}", reason, given);
            that!(found).will_be_err().because(&reason)
        }));
    }

    describe!(configuration, {
        describe!(newline, {
            use crate::parser_tests::*;
            it!(should_parse_csv_using_given_string_as_newline_terminator, {
                let tests = [(
                    "a,b,cNEWLINEd,\"eNEWLINE\",fNEWLINEg,h,iNEWLINE",
                    vec![
                        vec!["a", "b", "c"],
                        vec!["d", "eNEWLINE", "f"],
                        vec!["g", "h", "i"],
                    ],
                    "Should work when newline is '\\r\\n'",
                )];
                let mut parser = Parser::new();
                parser
                    .separator(',')
                    .quote('"')
                    .newline(String::from("NEWLINE"));
                run_tests_pass(parser, &tests);
            });
        });

        describe!(skip_rows_with_error, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(should_skip_lines_with_an_error, {
                    let tests = [(
                        "a\",b,c\nd,e,f\ng,h\n",
                        vec![
                            vec!["d", "e", "f"],
                        ],
                        "Turning on `skip_rows_with_error` should skip any rows with a field that fails to parse.",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').skip_rows_with_error(true);
                    run_tests_pass(parser, &tests);
                });
            });

            describe!(when_off, {
                use crate::parser_tests::*;
                it!(should_not_ignore_empty_rows, {
                    let tests = [(
                        "a\",b,c\nd,e,f\ng,h\n",
                        "Turning off `skip_rows_with_error` should cause parser to return `Err` after encountering an error.",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').skip_rows_with_error(false);
                    run_tests_fail(parser, &tests);
                });
            });
        });

        describe!(skip_empty_rows, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(should_ignore_empty_rows, {
                    let tests = [(
                        "\n\n\na,b,c\n\n\nd,e,f\n\n\ng,h,i\n\n\n\n",
                        vec![
                            vec!["a", "b", "c"],
                            vec!["d", "e", "f"],
                            vec!["g", "h", "i"],
                        ],
                        "Turning on `skip_empty_rows` should skip empty rows in CSV file.",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').skip_empty_rows(true);
                    run_tests_pass(parser, &tests);
                });
            });

            describe!(when_off, {
                use crate::parser_tests::*;
                it!(should_not_ignore_empty_rows, {
                    let tests = [(
                        "\n  \n\t\n",
                        vec![
                            vec![""],
                            vec!["  "],
                            vec!["\t"],
                        ],
                        "Turning *off* `skip_empty_rows` should cause empty rows *not* to be skipped",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').skip_empty_rows(false);
                    run_tests_pass(parser, &tests);
                });
            });
        });

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
                    parser.separator(',').quote('"').detect_columns(true);
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
                            "Proving columns via `columns` should override `detect_columns` and cause the first row to be treated as a record",
                        )];
                        let mut parser = Parser::new();
                        parser
                            .separator(',')
                            .quote('"')
                            .detect_columns(true)
                            .columns(vec![
                                String::from("a"),
                                String::from("b"),
                                String::from("c"),
                            ]);
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
                        "Turning off `detect_columns` should cause the first row to be treated as a record",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').detect_columns(false);
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
                        "Turning on `rtrim` should remove all types of whitespace after fields",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').rtrim(true);
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
                        "Turning *off* `rtrim` should *not* remove whitespace after fields",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').rtrim(false);
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
                        "Turning on `ltrim` should remove all whitespace before fields",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').ltrim(true);
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
                        "Turning *off* `ltrim` should *not* remove whitespace before fields",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').ltrim(false);
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
                        parser.separator(',').quote('"');
                        run_tests_fail(parser, &tests);
                    });
                }
            );
        });
    });
});
