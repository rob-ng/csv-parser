use std::io::Read;
mod error;
mod records;
pub use error::{Error, ErrorKind};
use records::Config;
pub use records::{Record, Records};

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

#[derive(Default)]
pub struct Parser {
    config: Config,
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            config: Default::default(),
        }
    }

    /// Sets maximum record size. Records longer than this value will result in an error.
    config!(max_record_size, max_record_size, usize);

    /// Sets newline terminator to given bytes.
    pub fn newline(&mut self, newline: &[u8]) -> &mut Self {
        self.config.newline = newline.to_vec();
        self
    }

    /// Sets field separator to given byte.
    config!(separator, separator, u8);

    /// Sets quote marker to given byte.
    config!(quote, quote, u8);

    /// Determines whether fields should be left-trimmed of whitespace.
    config!(ltrim, should_ltrim_fields);

    /// Determines whether fields should be right-trimmed of whitespace.
    config!(rtrim, should_rtrim_fields);

    /// Determines whether fields should be left *and* right trimmed of whitespace.
    pub fn trim(&mut self, should_trim: bool) -> &mut Self {
        self.config.should_ltrim_fields = should_trim;
        self.config.should_rtrim_fields = should_trim;
        self
    }

    /// Determines whether first row should be treated as column names for subsequent rows.
    config!(detect_columns, should_detect_columns);

    /// Sets column names for rows to given values. Overrides `detect_columns`.
    pub fn columns(&mut self, columns: Vec<String>) -> &mut Self {
        self.config.columns.replace(columns);
        self
    }

    /// Determines whether records should be allowed to have fewer fields than the number of columns.
    config!(relax_column_count_less, should_relax_column_count_less);

    /// Determines whether records should be allowed to have more fields than the number of columns.
    config!(relax_column_count_more, should_relax_column_count_more);

    /// Determines whether records should be allowed to have more *or* fewer fields than the number of columns.
    pub fn relax_column_count(&mut self, should_relax: bool) -> &mut Self {
        self.config.should_relax_column_count_less = should_relax;
        self.config.should_relax_column_count_more = should_relax;
        self
    }

    /// Determines whether rows of only whitespace should be skipped. Does not apply to rows in quoted fields.
    config!(skip_empty_rows, should_skip_empty_rows);

    /// Determines whether parsing errors from malformed CSV should be skipped. Other kinds of errors (e.g. IO) are not affected.
    config!(skip_rows_with_error, should_skip_rows_with_error);

    /// Returns an iterator of `Record`s from a readable source of CSV.
    pub fn records<R>(&self, csv_source: R) -> Records<R>
    where
        R: Read,
    {
        let config = self.config.clone();
        Records::new(csv_source, config)
    }
}

#[cfg(test)]
use jestr::*;

#[cfg(test)]
describe!(parser_tests, {
    pub use super::*;

    pub fn run_tests_pass(parser: Parser, tests: &[(&str, Vec<Vec<&str>>, &str)]) {
        verify_all!(tests.iter().map(|(given, expected, reason)| {
            let found: std::result::Result<Vec<Record>, Error> =
                parser.records(given.as_bytes()).collect();
            let reason = format!("{}.\nGiven:\n{}", reason, given);
            match &found {
                Ok(found) => {
                    let found: Vec<Vec<&str>> = found.iter().map(|f| f.fields()).collect();
                    that!(&found).will_equal(expected).because(&reason)
                }
                Err(_) => that!(found).will_be_ok().because(&reason),
            }
        }));
    }

    pub fn run_tests_fail(parser: Parser, tests: &[(&str, &str)]) {
        verify_all!(tests.iter().map(|(given, reason)| {
            let found: std::result::Result<Vec<Record>, Error> =
                parser.records(given.as_bytes()).collect();
            let reason = format!("{}.\nGiven:\n{}", reason, given);
            that!(found).will_be_err().because(&reason)
        }));
    }

    describe!(configuration, {
        describe!(max_record_size, {
            use crate::parser_tests::*;
            it!(
                should_prevent_the_parser_from_reading_more_than_n_bytes_per_record,
                {
                    let tests = [(
                        "a,b,c,d,e,f,g,h,i,j,k",
                        "Setting `max_record_size` should result in error when record is too large",
                    ),
                    (
                        "\"a,b,c,\nd,e,f\n,g,h,i\n,j,k\"",
                        "Setting `max_record_size` should work with multiline quotes",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(b',').quote(b'"').max_record_size(10);
                    run_tests_fail(parser, &tests);
                }
            );

            it!(should_apply_on_a_per_record_basis, {
                let tests = [(
                    "a,b,c\nd,e,f\ng,h,i",
                    vec![
                        vec!["a", "b", "c"],
                        vec!["d", "e", "f"],
                        vec!["g", "h", "i"],
                    ],
                    "Setting `max_record_size` should affect bytes read per record, *not* total bytes read",
                )];
                let mut parser = Parser::new();
                parser.separator(b',').quote(b'"').max_record_size(6);
                run_tests_pass(parser, &tests);
            });
        });

        describe!(relax_column_count, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(
                    should_allow_and_account_for_records_with_too_many_or_too_few_fields,
                    {
                        let tests = [(
                            "a,b,c\nd,e,f,g\nh\n",
                            vec![vec!["a", "b", "c"], vec!["d", "e", "f"], vec!["h", "", ""]],
                            "Turning on `relax_column_count` should handle records with either too many or too few fields",
                        )];
                        let mut parser = Parser::new();
                        parser.separator(b',').quote(b'"').relax_column_count(true);
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
                            "Turning off `relax_column_count` should cause records with too many or too few fields to return Errs",
                        )];
                        let mut parser = Parser::new();
                        parser.separator(b',').quote(b'"').relax_column_count(false);
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
                        "Turning on `relax_column_count_more` should ignore any extra fields",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(b',').quote(b'"').relax_column_count_more(true);
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
                            "Turning off `relax_column_count_more` should cause records with too many fields to return Errs",
                        )];
                        let mut parser = Parser::new();
                        parser
                            .separator(b',')
                            .quote(b'"')
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
                        "Turning on `relax_column_count_less` should fill any missing fields with empty string",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(b',').quote(b'"').relax_column_count_less(true);
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
                        "Turning off `relax_column_count_less` should cause records with too few fields to return Errs",
                    )];
                        let mut parser = Parser::new();
                        parser
                            .separator(b',')
                            .quote(b'"')
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
                parser.separator(b',').quote(b'"').newline(b"NEWLINE");
                run_tests_pass(parser, &tests);
            });
        });

        describe!(skip_rows_with_error, {
            describe!(when_on, {
                describe!(and_the_parser_finds_a_malformed_line, {
                    use crate::parser_tests::*;
                    it!(should_skip_the_line, {
                        let tests = [(
                            "a\",b,c\nd,e,f\ng,h\n",
                            vec![
                                vec!["d", "e", "f"],
                            ],
                            "Turning on `skip_rows_with_error` should skip any rows with a field that fails to parse",
                        )];
                        let mut parser = Parser::new();
                        parser
                            .separator(b',')
                            .quote(b'"')
                            .skip_rows_with_error(true);
                        run_tests_pass(parser, &tests);
                    });
                });

                describe!(and_the_parser_encounters_any_other_error, {
                    #[derive(Copy, Clone)]
                    enum BadReader {
                        OnRead,
                        InvalidUtf8,
                        None,
                    }
                    impl Read for BadReader {
                        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                            let res = match self {
                                BadReader::OnRead => Err(std::io::Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    "error",
                                )),
                                BadReader::InvalidUtf8 => {
                                    // some invalid bytes, in a vector
                                    let bytes = vec![0, 159, 146, 150];
                                    buf[0..bytes.len()].copy_from_slice(&bytes);
                                    Ok(bytes.len())
                                }
                                BadReader::None => Ok(0),
                            };
                            *self = BadReader::None;
                            res
                        }
                    }
                    use crate::parser_tests::*;
                    it!(should_not_skip_the_error, {
                        let tests: [(BadReader, &str); 2] = [
                            (
                                BadReader::OnRead,
                                "Turning on `skip_rows_with_error` should not skip IO errors",
                            ),
                            (
                                BadReader::InvalidUtf8,
                                "Turning on `skip_rows_with_error` should not skip UTF-8 errors",
                            ),
                        ];
                        let mut parser = Parser::new();
                        parser
                            .separator(b',')
                            .quote(b'"')
                            .skip_rows_with_error(true);
                        verify_all!(tests.iter().map(|&(given, reason)| {
                            let found: std::result::Result<Vec<Record>, Error> =
                                parser.records(given).collect();
                            that!(found).will_be_err().because(&reason)
                        }));
                    });
                });
            });

            describe!(when_off, {
                use crate::parser_tests::*;
                it!(should_not_ignore_empty_rows, {
                    let tests = [(
                        "a\",b,c\nd,e,f\ng,h\n",
                        "Turning off `skip_rows_with_error` should cause parser to return `Err` after encountering an error",
                    )];
                    let mut parser = Parser::new();
                    parser
                        .separator(b',')
                        .quote(b'"')
                        .skip_rows_with_error(false);
                    run_tests_fail(parser, &tests);
                });
            });
        });

        describe!(skip_empty_rows, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(should_ignore_empty_rows, {
                    let tests = [(
                        "     \n\n\na,b,c\n\t\t\n\nd,e,f\n\n\ng,h,i\n\n\n\n",
                        vec![
                            vec!["a", "b", "c"],
                            vec!["d", "e", "f"],
                            vec!["g", "h", "i"],
                        ],
                        "Turning on `skip_empty_rows` should skip rows in CSV containing only whitespace",
                    ),
                    (
                        "a\n\"  \t\n\"\nb",
                        vec![
                            vec!["a"],
                            vec!["  \t\n"],
                            vec!["b"],
                        ],
                        "Turning on `skip_empty_rows` should *not* affect empty rows in quoted fields",
                    )];
                    let mut parser = Parser::new();
                    parser
                        .separator(b',')
                        .quote(b'"')
                        .skip_empty_rows(true)
                        // Disable trim so fields of only whitespace aren't cleared.
                        .trim(false);
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
                    parser
                        .separator(b',')
                        .quote(b'"')
                        .skip_empty_rows(false)
                        // Disable trim so fields of only whitespace aren't cleared.
                        .trim(false);
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
                    parser.separator(b',').quote(b'"').detect_columns(true);
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
                            .separator(b',')
                            .quote(b'"')
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
                    parser.separator(b',').quote(b'"').detect_columns(false);
                    run_tests_pass(parser, &tests);
                });
            });
        });

        describe!(ltrim, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(should_ignore_whitespace_to_left_of_fields, {
                    let tests = [(
                        "   \"a\",  \u{A0}b,   \u{3000}c\n d,   e,f\n \t\tg,h, \u{A0}\u{3000}\ti",
                        vec![
                            vec!["a", "b", "c"],
                            vec!["d", "e", "f"],
                            vec!["g", "h", "i"],
                        ],
                        "Turning on `ltrim` should remove all whitespace before fields (quoted and unquoted)",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(b',').quote(b'"').ltrim(true).rtrim(false);
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
                    parser.separator(b',').quote(b'"').ltrim(false).rtrim(false);
                    run_tests_pass(parser, &tests);
                });
            });
        });

        describe!(rtrim, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(should_ignore_whitespace_to_right_of_fields, {
                    let tests = [(
                        "\"a\"   ,b  \u{A0},c   \u{3000}\nd ,e   ,f\ng \t\t,h,i \u{A0}\u{3000}\t",
                        vec![
                            vec!["a", "b", "c"],
                            vec!["d", "e", "f"],
                            vec!["g", "h", "i"],
                        ],
                        "Turning on `rtrim` should remove all types of whitespace after fields (quoted and unquoted)",
                    )];
                    let mut parser = Parser::new();
                    parser.separator(b',').quote(b'"').ltrim(false).rtrim(true);
                    run_tests_pass(parser, &tests);
                });
            });

            describe!(when_off, {
                use crate::parser_tests::*;
                it!(should_keep_whitespace_to_right_of_fields, {
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
                    parser.separator(b',').quote(b'"').ltrim(false).rtrim(false);
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
                    parser.separator(b',').quote(b'"').trim(true);
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
                    parser.separator(b',').quote(b'"').trim(false);
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
            parser.separator(b',').quote(b'"').trim(false);
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
                parser.separator(b',').quote(b'"');
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
                        parser.separator(b',').quote(b'"');
                        run_tests_fail(parser, &tests);
                    });
                }
            );
        });
    });
});
