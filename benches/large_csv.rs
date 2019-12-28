use criterion::{criterion_group, criterion_main, Criterion};
use csv::*;
use std::fs::File;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut parser_builder = ParserBuilder::new();
    let parser = parser_builder
        .trim(true)
        .detect_headers(true)
        .skip_empty_rows(false)
        .relax_field_count(true)
        .comment(b"#");
    c.bench_function("fib 20", |b| {
        b.iter(|| {
            let csv = File::open("./data/test_no_whitespace.csv").unwrap();
            let parser = parser.from_reader(csv);
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
