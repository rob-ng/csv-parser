use std::io::{BufRead, BufReader, Read};

const SEPARATOR: char = ',';
const QUOTE: char = '"';

type Result<T> = std::result::Result<T, String>;
type Field = String;
type FieldResult = Result<Field>;
type Record = Vec<Field>;
type RecordResult = Result<Option<Record>>;

pub struct Parser {
    // Special characters
    quote: char,
    separator: char,
    // Behavior
    should_detect_columns: bool,
    columns: Option<Record>,
    should_ltrim_fields: bool,
    should_rtrim_fields: bool,
    should_skip_empty_rows: bool
}

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

impl<'a> Parser {
    pub fn new() -> Self {
        Parser {
            separator: SEPARATOR,
            quote: QUOTE,
            should_ltrim_fields: false,
            should_rtrim_fields: false,
            should_detect_columns: false,
            columns: None,
            should_skip_empty_rows: false
        }
    }

    config!(separator, separator, char);

    config!(quote, quote, char);

    config!(ltrim, should_ltrim_fields);

    config!(rtrim, should_rtrim_fields);

    config!(detect_columns, should_detect_columns);

    pub fn columns(&mut self, columns: Vec<String>) -> &mut Self {
        self.columns.replace(columns);
        self
    }

    config!(skip_empty_rows, should_skip_empty_rows);

    pub fn parse<R>(&self, csv_source: R) -> ParserIterator<R>
    where
        R: Read,
    {
        let csv_source = SourceIterator::new(csv_source, self.should_skip_empty_rows);
        ParserIterator::new(csv_source, self)
    }
}

type TextStrategy<'a, R> = dyn Fn(&mut ParserIterator<'a, R>) -> FieldResult;
type OnRecordStrategy<'a, R> = dyn Fn(&mut ParserIterator<'a, R>, Vec<Field>) -> RecordResult;

pub struct ParserIterator<'a, R>
where
    R: Read,
{
    parser: &'a Parser,
    csv: std::iter::Peekable<SourceIterator<'a, R>>,
    columns: Option<Vec<String>>,

    text_strat: &'a TextStrategy<'a, R>,
    on_record_strat: &'a OnRecordStrategy<'a, R>
}

impl<'a, R> ParserIterator<'a, R>
where
    R: Read,
{
    fn new(csv_source: SourceIterator<'a, R>, parser: &'a Parser) -> Self {
        let text_strat: &'a TextStrategy<'a, R> = if parser.should_ltrim_fields {
            &|s: &mut Self| ParserIterator::text_ltrim(s)
        } else if parser.should_rtrim_fields {
            &|s: &mut Self| ParserIterator::text_rtrim(s)
        } else {
            &|s: &mut Self| ParserIterator::text(s)
        };
        let on_record_strat: &'a OnRecordStrategy<'a, R> = if parser.should_detect_columns {
            &|s: &mut Self, fields: Vec<Field>| ParserIterator::on_record_detect_columns(s, fields)
        } else {
            &|s: &mut Self, fields: Vec<Field>| ParserIterator::on_record_default(s, fields)
        };
        ParserIterator {
            parser,
            csv: csv_source.peekable(),
            columns: parser.columns.clone(),
            text_strat,
            on_record_strat
        }
    }

    fn record(&mut self) -> RecordResult
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

        (self.on_record_strat)(self, fields)
    }

    fn field(&mut self) -> FieldResult
    {
        match self.csv.peek() {
            Some(&c) if c == self.parser.quote => self.string(),
            _ => (self.text_strat)(self),
        }
    }

    fn string(&mut self) -> FieldResult
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
}

impl<'a, R> ParserIterator<'a, R>
where
    R: Read,
{
    fn on_record_default(&mut self, fields: Vec<Field>) -> RecordResult {
        match self.columns.as_mut() {
            Some(columns) if columns.len() == fields.len() => Ok(Some(fields)),
            Some(columns) => Err(format!(
                "Number of fields {} does not match number of columns {}",
                fields.len(),
                columns.len()
            )),
            None => {
                let standin_columns = vec![String::from(""); fields.len()];
                self.columns.replace(standin_columns);
                Ok(Some(fields))
            }
        }
    }

    fn on_record_detect_columns(&mut self, fields: Vec<Field>) -> RecordResult {
        match self.columns.as_mut() {
            Some(columns) if columns.len() == fields.len() => Ok(Some(fields)),
            Some(columns) => Err(format!(
                "Number of fields {} does not match number of columns {}",
                fields.len(),
                columns.len()
            )),
            None => {
                self.columns.replace(fields);
                Ok(None)
            }
        }
    }
}

impl<'a, R> ParserIterator<'a, R>
where
    R: Read,
{
    fn text(&mut self) -> Result<String> {
        self.text_base().map(|field_chars| field_chars.iter().collect())
    }

    fn text_base(&mut self) -> Result<Vec<char>>
    {
        let mut field = vec![];

        loop {
            match self.csv.peek() {
                Some(&c) if c == self.parser.quote => return Err(format!("Unquoted fields cannot contain quote character: `{}`", self.parser.quote)),
                Some(&c) if c != self.parser.separator && c != '\n' => field.push(c),
                _ => break
            }
            self.csv.next();
        }

        Ok(field)
    }

    fn text_ltrim(&mut self) -> Result<String> {
        loop {
            match self.csv.peek() {
                Some(&c) if self.parser.should_ltrim_fields && c.is_whitespace() => self.csv.next(),
                _ => break
            };
        }
        self.text_base().map(|field_chars| field_chars.iter().collect())
    }

    fn text_rtrim(&mut self) -> Result<String> {
        self.text_base().map(|mut field_chars| {
            loop {
                match field_chars.last() {
                    Some(&c) if self.parser.should_rtrim_fields && c.is_whitespace() => field_chars.pop(),
                    _ => break
                };
            }
            field_chars.iter().collect()
        })
    }
} 

impl<'a, R> Iterator for ParserIterator<'a, R>
where
    R: Read,
{
    type Item = Result<Record>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.csv.peek() {
            None => None,
            Some(_) => match self.record() {
                Ok(Some(record)) => Some(Ok(record)),
                Ok(None) => self.next(),
                Err(msg) => Some(Err(msg))
            }
        }
    }
}

// Private
// SHOULD BE ABLE TO CONFIGURE THIS AS WELL:
// Example Configuration:
// 1. Skip empty lines
// 2. Row delimiter (with read_until)
// 3. From line
// 4. To line
struct SourceIterator<'a, R> {
    source: BufReader<R>,
    curr_line: Vec<char>,
    next_strat: &'a NextStrategy<Self, char>
}

type NextStrategy<Slf, Item> = dyn Fn(&mut Slf) -> Option<Item>;

impl<'a, R> SourceIterator<'a, R>
where
    R: Read,
{
    pub fn new(source: R, should_skip_empty_rows: bool) -> Self {
        let next_strat: &NextStrategy<Self, char> = if should_skip_empty_rows {
            &|slf: &mut Self| SourceIterator::next_skip_empty_lines(slf)
        } else {
            &|slf: &mut Self| SourceIterator::next_default(slf)
        };
        SourceIterator {
            source: BufReader::new(source),
            curr_line: vec![],
            next_strat
        }
    }

    fn next_default(&mut self) -> Option<char> {
        if self.curr_line.len() == 0 {
            let mut curr_line = String::new();
            self.source.read_line(&mut curr_line);
            self.curr_line = curr_line.chars().rev().collect();
        }
        self.curr_line.pop()
    }

    fn next_skip_empty_lines(&mut self) -> Option<char> {
        if self.curr_line.len() == 0 {
            let mut curr_line = String::new();
            while curr_line.trim().is_empty() {
                curr_line.clear();
                match self.source.read_line(&mut curr_line) {
                    Ok(read) if read == 0 => break,
                    _ => continue
                }
            }
            self.curr_line = curr_line.chars().rev().collect();
        }
        self.curr_line.pop()
    }
}

impl<'a, R> Iterator for SourceIterator<'a, R>
where
    R: Read,
{
    type Item = char;
    fn next(&mut self) -> Option<Self::Item> {
        (self.next_strat)(self)
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
                        parser.separator(',').quote('"').detect_columns(true).columns(vec![String::from("a"), String::from("b"), String::from("c")]);
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
