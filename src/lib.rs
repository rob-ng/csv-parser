mod error;
mod parser;
mod record;
pub use error::{Error, ErrorKind};
pub use parser::{Parser, ParserBuilder};
pub use record::{Record, Records};

#[cfg(test)]
use {jestr::*, std::io::Read};

#[cfg(test)]
describe!(parser_tests, {
    pub use super::*;

    pub fn run_tests_pass(parser_builder: ParserBuilder, tests: &[(&str, Vec<Vec<&str>>, &str)]) {
        verify_all!(tests.iter().map(|(given, expected, reason)| {
            let found: std::result::Result<Vec<Record>, Error> = parser_builder
                .from_reader(given.as_bytes())
                .records()
                .collect();
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

    pub fn run_tests_fail(parser_builder: ParserBuilder, tests: &[(&str, &str)]) {
        verify_all!(tests.iter().map(|(given, reason)| {
            let found: std::result::Result<Vec<Record>, Error> = parser_builder
                .from_reader(given.as_bytes())
                .records()
                .collect();
            let reason = format!("{}.\nGiven:\n{}", reason, given);
            that!(found).will_be_err().because(&reason)
        }));
    }

    describe!(headers, {
        describe!(when_parser_has_not_read_from_csv, {
            use crate::parser_tests::*;
            it!(should_read_first_row_from_csv_and_return_its_fields, {
                let csv = "a,b,c\nd,e,f\n";
                let mut parser = ParserBuilder::new().from_reader(csv.as_bytes());
                let headers = parser.headers();
                verify!(that!(headers).will_unwrap_to(&[
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string()
                ]));
                let remaining_records: Vec<Record> = parser.records().map(|r| r.unwrap()).collect();
                let remaining_records: Vec<Vec<&str>> =
                    remaining_records.iter().map(|r| r.fields()).collect();
                verify!(that!(remaining_records).will_equal(vec![vec!["d", "e", "f"]]));
            });

            describe!(and_reading_from_csv_returns_none, {
                use crate::parser_tests::*;
                use std::io::BufReader;

                it!(should_return_empty_slice, {
                    let mut reader = BufReader::new(b"".as_ref());
                    let mut buf = vec![];
                    // Empty reader.
                    let _ = reader.read_to_end(&mut buf);
                    let mut parser = ParserBuilder::new().from_reader(reader);
                    let headers = parser.headers();
                    verify!(that!(headers)
                        .will_unwrap_to(&[])
                        .because("should return an empty slice if reader is empty"));
                });
            });

            describe!(and_reading_from_csv_returns_error, {
                use crate::parser_tests::*;

                struct ErrorReader {}
                impl Read for ErrorReader {
                    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "error",
                        ))
                    }
                }

                it!(should_return_error, {
                    let reader = ErrorReader {};
                    // Empty reader.
                    let mut parser = ParserBuilder::new().from_reader(reader);
                    let headers = parser.headers();
                    verify!(that!(headers)
                        .will_be_err()
                        .because("should return any errors caused by parsing the first record"));
                });
            });
        });

        describe!(when_parser_has_already_read_from_csv, {
            use crate::parser_tests::*;

            it!(should_return_fields_of_first_row_read, {
                let csv = "a,b,c\nd,e,f\n";
                let mut parser = ParserBuilder::new().from_reader(csv.as_bytes());
                let _ = parser.headers();
                // Second call should not perform additional reads.
                let headers = parser.headers();
                verify!(that!(headers).will_unwrap_to(&[
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string()
                ]));
                let remaining_records: Vec<Record> = parser.records().map(|r| r.unwrap()).collect();
                let remaining_records: Vec<Vec<&str>> =
                    remaining_records.iter().map(|r| r.fields()).collect();
                verify!(that!(remaining_records).will_equal(vec![vec!["d", "e", "f"]]));
            });
        });
    });

    describe!(configuration, {
        describe!(comment, {
            use crate::parser_tests::*;
            it!(
                should_cause_parser_to_skip_lines_starting_with_comment_indicator,
                {
                    let tests = [
                    ("### This is a comment\na,b,c\n### Another comment", vec![vec!["a", "b", "c"]], "Setting `comment` should cause lines starting with that value to be skipped")
                ];
                    let mut reader = ParserBuilder::new();
                    reader.comment(b"###");
                    run_tests_pass(reader, &tests);
                }
            );
        });

        describe!(separator, {
            use crate::parser_tests::*;
            it!(should_change_separator_character, {
                let tests = [(
                    "a b c\nd e f\ng h i",
                    vec![
                        vec!["a", "b", "c"],
                        vec!["d", "e", "f"],
                        vec!["g", "h", "i"],
                    ],
                    "Setting `separator` should change field separator",
                )];
                let mut reader = ParserBuilder::new();
                reader.separator(b' ');
                run_tests_pass(reader, &tests);
            });
        });

        describe!(escape, {
            use crate::parser_tests::*;
            it!(should_change_escape_character_used_for_nested_quotes, {
                let tests = [(
                    "\"a,b,c\\\"d,e,f\\\"g,h,i\"",
                    vec![vec!["a,b,c\"d,e,f\"g,h,i"]],
                    "Setting `escape` should change escape character used with inner quotes",
                )];
                let mut reader = ParserBuilder::new();
                reader.separator(b',').quote(b'"').escape(b'\\');
                run_tests_pass(reader, &tests);
            });
        });

        describe!(max_record_size, {
            use crate::parser_tests::*;
            it!(
                should_prevent_the_reader_from_reading_more_than_n_bytes_per_record,
                {
                    let tests = [(
                        "a,b,c,d,e,f,g,h,i,j,k",
                        "Setting `max_record_size` should result in error when record is too large",
                    ),
                    (
                        "\"a,b,c,\nd,e,f\n,g,h,i\n,j,k\"",
                        "Setting `max_record_size` should work with multiline quotes",
                    )];
                    let mut reader = ParserBuilder::new();
                    reader.separator(b',').quote(b'"').max_record_size(10);
                    run_tests_fail(reader, &tests);
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
                let mut reader = ParserBuilder::new();
                reader.separator(b',').quote(b'"').max_record_size(6);
                run_tests_pass(reader, &tests);
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
                        let mut reader = ParserBuilder::new();
                        reader.separator(b',').quote(b'"').relax_column_count(true);
                        run_tests_pass(reader, &tests);
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
                        let mut reader = ParserBuilder::new();
                        reader.separator(b',').quote(b'"').relax_column_count(false);
                        run_tests_fail(reader, &tests);
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
                    let mut reader = ParserBuilder::new();
                    reader.separator(b',').quote(b'"').relax_column_count_more(true);
                    run_tests_pass(reader, &tests);
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
                        let mut reader = ParserBuilder::new();
                        reader
                            .separator(b',')
                            .quote(b'"')
                            .relax_column_count_more(false);
                        run_tests_fail(reader, &tests);
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
                    let mut reader = ParserBuilder::new();
                    reader.separator(b',').quote(b'"').relax_column_count_less(true);
                    run_tests_pass(reader, &tests);
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
                        let mut reader = ParserBuilder::new();
                        reader
                            .separator(b',')
                            .quote(b'"')
                            .relax_column_count_less(false);
                        run_tests_fail(reader, &tests);
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
                let mut reader = ParserBuilder::new();
                reader.separator(b',').quote(b'"').newline(b"NEWLINE");
                run_tests_pass(reader, &tests);
            });
        });

        describe!(skip_rows_with_error, {
            describe!(when_on, {
                describe!(and_the_reader_finds_a_malformed_line, {
                    use crate::parser_tests::*;
                    it!(should_skip_the_line, {
                        let tests = [(
                            "a\",b,c\nd,e,f\ng,h\n",
                            vec![
                                vec!["d", "e", "f"],
                            ],
                            "Turning on `skip_rows_with_error` should skip any rows with a field that fails to parse",
                        )];
                        let mut reader = ParserBuilder::new();
                        reader
                            .separator(b',')
                            .quote(b'"')
                            .skip_rows_with_error(true);
                        run_tests_pass(reader, &tests);
                    });
                });

                describe!(and_the_reader_encounters_any_other_error, {
                    #[derive(Copy, Clone)]
                    enum BadParserBuilder {
                        OnRead,
                        InvalidUtf8,
                        None,
                    }
                    impl Read for BadParserBuilder {
                        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                            let res = match self {
                                BadParserBuilder::OnRead => Err(std::io::Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    "error",
                                )),
                                BadParserBuilder::InvalidUtf8 => {
                                    // some invalid bytes, in a vector
                                    let bytes = vec![0, 159, 146, 150];
                                    buf[0..bytes.len()].copy_from_slice(&bytes);
                                    Ok(bytes.len())
                                }
                                BadParserBuilder::None => Ok(0),
                            };
                            *self = BadParserBuilder::None;
                            res
                        }
                    }
                    use crate::parser_tests::*;
                    it!(should_not_skip_the_error, {
                        let tests: [(BadParserBuilder, &str); 2] = [
                            (
                                BadParserBuilder::OnRead,
                                "Turning on `skip_rows_with_error` should not skip IO errors",
                            ),
                            (
                                BadParserBuilder::InvalidUtf8,
                                "Turning on `skip_rows_with_error` should not skip UTF-8 errors",
                            ),
                        ];
                        let mut parser_builder = ParserBuilder::new();
                        parser_builder
                            .separator(b',')
                            .quote(b'"')
                            .skip_rows_with_error(true);
                        verify_all!(tests.iter().map(|&(given, reason)| {
                            let found: std::result::Result<Vec<Record>, Error> =
                                parser_builder.from_reader(given).records().collect();
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
                        "Turning off `skip_rows_with_error` should cause reader to return `Err` after encountering an error",
                    )];
                    let mut reader = ParserBuilder::new();
                    reader
                        .separator(b',')
                        .quote(b'"')
                        .skip_rows_with_error(false);
                    run_tests_fail(reader, &tests);
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
                    let mut reader = ParserBuilder::new();
                    reader
                        .separator(b',')
                        .quote(b'"')
                        .skip_empty_rows(true)
                        // Disable trim so fields of only whitespace aren't cleared.
                        .trim(false);
                    run_tests_pass(reader, &tests);
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
                    let mut reader = ParserBuilder::new();
                    reader
                        .separator(b',')
                        .quote(b'"')
                        .skip_empty_rows(false)
                        // Disable trim so fields of only whitespace aren't cleared.
                        .trim(false);
                    run_tests_pass(reader, &tests);
                });
            });
        });

        describe!(detect_headers, {
            describe!(when_on, {
                use crate::parser_tests::*;
                it!(should_treat_first_row_in_csv_as_header_instead_of_record, {
                    let tests = [(
                        "a,b,c\nd,e,f\ng,h,i",
                        vec![
                            vec!["d", "e", "f"],
                            vec!["g", "h", "i"],
                        ],
                        "Turning on `detect_headers` should prevent first row from being returned as a record",
                    )];
                    let mut reader = ParserBuilder::new();
                    reader.separator(b',').quote(b'"').detect_headers(true);
                    run_tests_pass(reader, &tests);
                });

                describe!(and_explicit_headers_have_also_been_given, {
                    use crate::parser_tests::*;
                    it!(should_treat_first_row_as_record, {
                        let tests = [(
                            "a,b,c\nd,e,f\ng,h,i",
                            vec![
                                vec!["a", "b", "c"],
                                vec!["d", "e", "f"],
                                vec!["g", "h", "i"],
                            ],
                            "Proving headers via `headers` should override `detect_headers` and cause the first row to be treated as a record",
                        )];
                        let mut reader = ParserBuilder::new();
                        reader
                            .separator(b',')
                            .quote(b'"')
                            .detect_headers(true)
                            .headers(vec![
                                String::from("a"),
                                String::from("b"),
                                String::from("c"),
                            ]);
                        run_tests_pass(reader, &tests);
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
                        "Turning off `detect_headers` should cause the first row to be treated as a record",
                    )];
                    let mut reader = ParserBuilder::new();
                    reader.separator(b',').quote(b'"').detect_headers(false);
                    run_tests_pass(reader, &tests);
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
                    let mut reader = ParserBuilder::new();
                    reader.separator(b',').quote(b'"').ltrim(true).rtrim(false);
                    run_tests_pass(reader, &tests);
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
                    let mut reader = ParserBuilder::new();
                    reader.separator(b',').quote(b'"').ltrim(false).rtrim(false);
                    run_tests_pass(reader, &tests);
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
                    let mut reader = ParserBuilder::new();
                    reader.separator(b',').quote(b'"').ltrim(false).rtrim(true);
                    run_tests_pass(reader, &tests);
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
                    let mut reader = ParserBuilder::new();
                    reader.separator(b',').quote(b'"').ltrim(false).rtrim(false);
                    run_tests_pass(reader, &tests);
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
                    let mut reader = ParserBuilder::new();
                    reader.separator(b',').quote(b'"').trim(true);
                    run_tests_pass(reader, &tests);
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
                    let mut reader = ParserBuilder::new();
                    reader.separator(b',').quote(b'"').trim(false);
                    run_tests_pass(reader, &tests);
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
                (
                    "这是一个,\"例子\"\"。\n\"\"它\",由多个\n句子,组成,。",
                    vec![
                        vec!["这是一个", "例子\"。\n\"它", "由多个"],
                        vec!["句子", "组成", "。"],
                    ],
                    "Should work with non-ascii strings",
                ),
            ];
            let mut reader = ParserBuilder::new();
            reader.separator(b',').quote(b'"').trim(false);
            run_tests_pass(reader, &tests);
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
                let mut reader = ParserBuilder::new();
                reader.separator(b',').quote(b'"').trim(false);
                run_tests_fail(reader, &tests);
            });

            describe!(
                because_a_record_has_more_or_fewer_fields_than_number_of_headers,
                {
                    pub use crate::parser_tests::*;
                    it!(should_return_err, {
                        let tests = [
                            (
                                "a,b\nd",
                                "Records cannot have fewer fields than there are headers",
                            ),
                            (
                                "a,b\nd,e,f",
                                "Records cannot have more fields than there are headers",
                            ),
                        ];
                        let mut reader = ParserBuilder::new();
                        reader.separator(b',').quote(b'"');
                        run_tests_fail(reader, &tests);
                    });
                }
            );
        });
    });
});
