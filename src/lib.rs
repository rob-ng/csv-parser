pub mod error;
pub mod parser_iterator;
use parser_iterator::ParserIterator;

use std::io::Read;

pub const SEPARATOR: char = ',';
pub const QUOTE: char = '"';
pub const NEWLINE: &str = "\n";

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
    should_relax_column_count_less: bool,
    should_relax_column_count_more: bool,
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            quote: QUOTE,
            separator: SEPARATOR,
            newline: String::from(NEWLINE),
            should_detect_columns: false,
            columns: None,
            should_ltrim_fields: false,
            should_rtrim_fields: false,
            should_skip_rows_with_error: false,
            should_skip_empty_rows: true,
            should_relax_column_count_less: false,
            should_relax_column_count_more: false,
        }
    }

    config!(separator, separator, char);

    config!(quote, quote, char);

    config!(newline, newline, String);

    config!(ltrim, should_ltrim_fields);

    config!(rtrim, should_rtrim_fields);

    pub fn trim(&mut self, should_trim: bool) -> &mut Self {
        self.should_ltrim_fields = should_trim;
        self.should_rtrim_fields = should_trim;
        self
    }

    config!(detect_columns, should_detect_columns);

    pub fn columns(&mut self, columns: Vec<String>) -> &mut Self {
        self.columns.replace(columns);
        self
    }

    config!(relax_column_count_less, should_relax_column_count_less);

    config!(relax_column_count_more, should_relax_column_count_more);

    pub fn relax_column_count(&mut self, should_relax: bool) -> &mut Self {
        self.should_relax_column_count_less = should_relax;
        self.should_relax_column_count_more = should_relax;
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

#[cfg(test)]
use {crate::error::Error, jestr::*};

#[cfg(test)]
describe!(parser_tests, {
    pub use super::*;

    pub fn run_tests_pass(parser: Parser, tests: &[(&str, Vec<Vec<&str>>, &str)]) {
        verify_all!(tests.iter().map(|(given, expected, reason)| {
            let found: std::result::Result<Vec<Vec<String>>, Error> =
                parser.parse(given.as_bytes()).collect();
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
            let found: std::result::Result<Vec<Vec<String>>, Error> =
                parser.parse(given.as_bytes()).collect();
            let reason = format!("{}.\nGiven:\n{}", reason, given);
            that!(found).will_be_err().because(&reason)
        }));
    }

    describe!(configuration, {
        describe!(relax_column_count, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(
                    should_allow_and_account_for_records_with_too_many_or_too_few_fields,
                    {
                        let tests = [(
                            "a,b,c\nd,e,f,g\nh\n",
                            vec![vec!["a", "b", "c"], vec!["d", "e", "f"], vec!["h", "", ""]],
                            "Turning on `relax_column_count` should handle records with either too many or too few fields.",
                        )];
                        let mut parser = Parser::new();
                        parser.separator(',').quote('"').relax_column_count(true);
                        run_tests_pass(parser, &tests);
                    }
                );
            });

            describe!(when_off, {
                use crate::parser_tests::*;
                it!(
                    should_cause_records_with_too_many_fields_to_result_in_an_err,
                    {
                        let tests = [(
                            "a,b,c\nd,e,f,g\nh\n",
                            "Turning off `relax_column_count` should cause records with too many or too few fields to return Errs.",
                        )];
                        let mut parser = Parser::new();
                        parser.separator(',').quote('"').relax_column_count(false);
                        run_tests_fail(parser, &tests);
                    }
                );
            });
        });

        describe!(relax_column_count_more, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(should_allow_for_records_with_missing_fields_and_give_said_fields_default_values, {
                    let tests = [(
                        "a,b,c\nd,e,f,g\nh,i,j,k,l\n",
                        vec![
                            vec!["a", "b", "c"],
                            vec!["d", "e", "f"],
                            vec!["h", "i", "j"],
                        ],
                        "Turning on `relax_column_count_more` should ignore any extra fields.",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').relax_column_count_more(true);
                    run_tests_pass(parser, &tests);
                });
            });

            describe!(when_off, {
                use crate::parser_tests::*;
                it!(
                    should_cause_records_with_too_many_fields_to_result_in_an_err,
                    {
                        let tests = [(
                            "a,b,c\nd,e,f,g\nh,i,j,k,l\n",
                            "Turning off `relax_column_count_more` should cause records with too many fields to return Errs.",
                        )];
                        let mut parser = Parser::new();
                        parser
                            .separator(',')
                            .quote('"')
                            .relax_column_count_more(false);
                        run_tests_fail(parser, &tests);
                    }
                );
            });
        });

        describe!(relax_column_count_less, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(should_allow_for_records_with_missing_fields_and_give_said_fields_default_values, {
                    let tests = [(
                        "a,b,c\nd,e\ng\n",
                        vec![
                            vec!["a", "b", "c"],
                            vec!["d", "e", ""],
                            vec!["g", "", ""],
                        ],
                        "Turning on `relax_column_count_less` should fill any missing fields with empty string.",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').relax_column_count_less(true);
                    run_tests_pass(parser, &tests);
                });
            });

            describe!(when_off, {
                use crate::parser_tests::*;
                it!(
                    should_cause_records_with_too_few_fields_to_result_in_an_err,
                    {
                        let tests = [(
                        "a,b,c\nd,e\ng\n",
                        "Turning off `relax_column_count_less` should cause records with too few fields to return Errs.",
                    )];
                        let mut parser = Parser::new();
                        parser
                            .separator(',')
                            .quote('"')
                            .relax_column_count_less(false);
                        run_tests_fail(parser, &tests);
                    }
                );
            });
        });

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

        describe!(trim, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(should_ignore_whitespace_to_left_and_right_of_fields, {
                    let tests = [(
                        "   a   ,  \u{A0}b  \u{A0},   \u{3000}c   \u{3000}\n d ,   e   ,f\n \t\tg \t\t,h, \u{A0}\u{3000}\ti\u{A0}\u{3000}\t",
                        vec![
                            vec!["a", "b", "c"],
                            vec!["d", "e", "f"],
                            vec!["g", "h", "i"],
                        ],
                        "Turning on `trim` should remove all whitespace before and after fields",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').trim(true);
                    run_tests_pass(parser, &tests);
                });
            });

            describe!(when_off, {
                use crate::parser_tests::*;
                it!(should_keep_whitespace_to_left_and_right_of_fields, {
                    let tests = [(
                        "   a   ,  \u{A0}b  \u{A0},   \u{3000}c   \u{3000}\n d ,   e   ,f\n \t\tg \t\t,h, \u{A0}\u{3000}\ti \u{A0}\u{3000}\t",
                        vec![
                            vec!["   a   ", "  \u{A0}b  \u{A0}", "   \u{3000}c   \u{3000}"],
                            vec![" d ", "   e   ", "f"],
                            vec![" \t\tg \t\t", "h", " \u{A0}\u{3000}\ti \u{A0}\u{3000}\t"],
                        ],
                        "Turning *off* `trim` should *not* remove whitespace before and after fields",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(',').quote('"').trim(false);
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
