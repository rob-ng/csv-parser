use criterion::{criterion_group, criterion_main, Criterion};
use csv::*;
use std::fs::File;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fib 20", |b| {
        b.iter(|| {
            let csv = File::open("./data/test_no_whitespace.csv").unwrap();
            let mut parser_builder = ParserBuilder::new();
            let parser = parser_builder
                .trim(false)
                .detect_headers(false)
                .skip_empty_rows(false)
                .relax_column_count(false)
                //.comment(b"#")
                .from_reader(csv);
            for _record in parser.records() {}
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = criterion_benchmark
}
criterion_main!(benches);
