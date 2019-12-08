use burntcsv::Reader;
use criterion::{criterion_group, criterion_main, Criterion};
use std::fs::File;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fib 20", |b| {
        b.iter(|| {
            let csv = File::open("./large.csv").unwrap();
            let mut rdr = Reader::from_reader(std::io::BufReader::new(csv));
            for _result in rdr.records() {}
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = criterion_benchmark
}
criterion_main!(benches);
