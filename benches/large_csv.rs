use criterion::{criterion_group, criterion_main, Criterion};
use csv::*;
use std::fs::File;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fib 20", |b| {
        b.iter(|| {
            let csv = File::open("./large.csv").unwrap();
            let mut parser = Parser::new();
            let parser = parser.ltrim();
            for _record in parser.parse(csv) {}
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
