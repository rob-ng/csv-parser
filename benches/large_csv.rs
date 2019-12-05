use criterion::{criterion_group, criterion_main, Criterion};
use csv::*;
use std::fs::File;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fib 20", |b| {
        b.iter(|| {
            let csv = File::open("./large.csv").unwrap();
            let mut parser = Parser::new();
            parser.trim(true).detect_columns(true).skip_empty_rows(true);
            for _record in parser.parse(csv) {}
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = criterion_benchmark
}
criterion_main!(benches);
