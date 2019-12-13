use criterion::{criterion_group, criterion_main, Criterion};
use std::fs::File;
use std::io::BufRead;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fib 20", |b| {
        b.iter(|| {
            let csv = File::open("./large.csv").unwrap();
            for _record in std::io::BufReader::new(csv).lines() {}
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = criterion_benchmark
}
criterion_main!(benches);
