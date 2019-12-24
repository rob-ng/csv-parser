use csv::*;
use std::fs::File;

fn main() {
    for _i in 0..10000 {
        let csv = File::open("./data/test_no_whitespace.csv").unwrap();
        let mut parser_builder = ParserBuilder::new();
        let parser = parser_builder
            .trim(false)
            .detect_headers(true)
            .skip_empty_rows(true)
            .relax_column_count(true)
            //.comment(b"#")
            .from_reader(csv);
        for _record in parser.records() {}
    }
}
