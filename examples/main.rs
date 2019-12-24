use csv::*;
use std::fs::File;

fn main() {
    let csv = File::open("./data/test.csv").unwrap();
    let mut parser_builder = ParserBuilder::new();
    let parser = parser_builder
        .trim(true)
        .detect_headers(true)
        .skip_empty_rows(true)
        .relax_column_count(true)
        .comment(b"#")
        .from_reader(csv);
    for _record in parser.records() {}
}
