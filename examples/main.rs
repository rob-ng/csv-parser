use csv::*;
use std::fs::File;

fn main() {
    let csv = File::open("./test.csv").unwrap();
    let mut parser = Parser::new();
    parser
        .trim(true)
        .detect_columns(true)
        .skip_empty_rows(true)
        .relax_column_count(true);
    for _record in parser.records(csv) {}
}
