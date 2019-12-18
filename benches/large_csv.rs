use criterion::{criterion_group, criterion_main, Criterion};
use csv::*;
use std::fs::File;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fib 20", |b| {
        b.iter(|| {
            let csv = File::open("./test.csv").unwrap();
            let mut parser_builder = ParserBuilder::new();
            let parser = parser_builder
                .trim(true)
                .detect_columns(true)
                .skip_empty_rows(true)
                .relax_column_count(true)
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
