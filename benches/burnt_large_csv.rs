use burntcsv::{ReaderBuilder, Trim};
use criterion::{criterion_group, criterion_main, Criterion};
use std::fs::File;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fib 20", |b| {
        b.iter(|| {
            let csv = File::open("./data/test_no_whitespace.csv").unwrap();
            let mut rdr = ReaderBuilder::new();
            let mut rdr = rdr
                //.comment(Some(b'#'))
                //.trim(Trim::All)
                //.has_headers(true)
                //.flexible(true)
                .from_reader(std::io::BufReader::new(csv));
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
