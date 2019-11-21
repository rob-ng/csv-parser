use std::io::{BufRead, BufReader, Read};

struct CSVReader<R>
where
    R: Read,
{
    buf: BufReader<R>,
}

impl<R> CSVReader<R>
where
    R: Read,
{
    pub fn new(to_read: R) -> Self {
        CSVReader {
            buf: BufReader::new(to_read),
        }
    }
}

impl<R> IntoIterator for CSVReader<R>
where
    R: Read,
{
    type Item = char;
    type IntoIter = CSVReaderIterator<R>;

    fn into_iter(self) -> Self::IntoIter {
        CSVReaderIterator::new(self)
    }
}

struct CSVReaderIterator<R>
where
    R: Read,
{
    buf: BufReader<R>,
    curr_line: String,
}

impl<R> CSVReaderIterator<R>
where
    R: Read,
{
    pub fn new(mut csv_reader: CSVReader<R>) -> Self {
        let mut curr_line = String::new();
        csv_reader.buf.read_line(&mut curr_line);
        CSVReaderIterator {
            buf: csv_reader.buf,
            curr_line,
        }
    }
}

impl<R> Iterator for CSVReaderIterator<R>
where
    R: Read,
{
    type Item = char;
    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_line.len() == 0 {
            self.buf.read_line(&mut self.curr_line);
        }
        let next = self.curr_line.chars().next();
        if next.is_some() {
            self.curr_line.drain(0..1);
        }
        next
    }
}

struct CSV {
    separator: char,
    quote: char,
}

impl CSV {
    pub fn new() -> Self {
        CSV {
            separator: ',',
            quote: '"',
        }
    }

    pub fn separator(&mut self, separator: char) -> &mut Self {
        self.separator = separator;
        self
    }

    pub fn quote(&mut self, quote: char) -> &mut Self {
        self.quote = quote;
        self
    }

    fn string<R>(&self, csv: &mut std::iter::Peekable<R>) -> Result<String, String>
    where
        R: Iterator<Item = char>,
    {
        // Remove initial quoation mark.
        csv.next();

        let mut field = String::new();

        while let Some(c) = csv.next() {
            if c == self.quote {
                let next = csv.peek();
                match next {
                    Some(&c) if c == self.quote => {
                        field.push(self.quote);
                        csv.next();
                    }
                    Some(&c) => {
                        return Ok(field);
                    }
                    None => {
                        return Ok(field);
                    }
                }
            } else {
                field.push(c);
            }
        }

        Ok(field)
    }

    fn text<'a, R>(&self, csv: &mut std::iter::Peekable<R>) -> Result<String, String>
    where
        R: Iterator<Item = char>,
    {
        let mut field = String::new();

        for c in csv {
            if c == self.quote {
                return Err(String::from(
                    "Unquoted fields cannot contain quatation marks.",
                ));
            }
            if c == self.separator || c == '\n' {
                return Ok(field);
            }
            field.push(c);
        }

        Ok(field)
    }

    fn field<'a, R>(&self, csv: &mut std::iter::Peekable<R>) -> Result<String, String>
    where
        R: Iterator<Item = char>,
    {
        match csv.peek() {
            Some(&c) if c == self.quote => self.string(csv),
            _ => self.text(csv),
        }
    }
}

#[cfg(test)]
use jestr::*;

#[cfg(test)]
describe!(field_tests, {
    pub use super::*;
    describe!(when_csv_is_wellformed, {
        use super::*;
        it!(should_correctly_parse_fields, {
            let tests = [
                (",", ""),
                ("abc", "abc"),
                ("abc,def", "abc"),
                ("abc\ndef", "abc"),
                ("\"abc\ndef\"", "abc\ndef"),
                ("\"\"\"abc\"\"def\"", "\"abc\"def"),
                (
                    "\"Quote \"\"Inner quote and \"\"even more inner quote\"\"\"\"\"",
                    "Quote \"Inner quote and \"even more inner quote\"\"",
                ),
            ];
            verify_all!(tests.iter().map(|&(given, expected)| {
                let mut csv = CSV::new();
                let csv = csv.separator(',').quote('"');
                let csvreader = CSVReader::new(given.as_bytes());
                let found = csv.field(&mut csvreader.into_iter().peekable());
                that!(found).will_unwrap_to(String::from(expected))
            }));
        });
    });

    describe!(when_csv_is_malformed, {
        use super::*;
        it!(should_return_err, {
            let tests = ["ab\"cd"];
            verify_all!(tests.iter().map(|&given| {
                let mut csv = CSV::new();
                let csv = csv.separator(',').quote('"');
                let csvreader = CSVReader::new(given.as_bytes());
                let found = csv.field(&mut csvreader.into_iter().peekable());
                that!(found).will_be_err()
            }));
        });
    });
});
