use common::Result;
use lo::Lo;
use postgres::Connection;
use postgres::rows;
use postgres::rows::{Row, Rows};
use postgres::types::Oid;
use serialize::hex::FromHex;

pub struct LoRetriever<'a> {
    rows: Rows<'a>
}

impl<'a> LoRetriever<'a> {
    pub fn new(conn: &'a Connection, max_results: i64) -> Result<Self> {
        assert!(max_results > 0);
        let rows = conn.query("SELECT hash, data, size FROM _nice_binary where sha2 is NULL LIMIT $1", &[&max_results])?;

        Ok(LoRetriever {
            rows: rows
        })     
    }

    pub fn iter<'b>(&'b self) -> Iter<'b> {
        Iter {
            rows: self.rows.iter()
        }
    }
}

pub struct Iter<'a> {
    rows: rows::Iter<'a>
}

impl<'a> IntoIterator for &'a LoRetriever<'a> {
    type Item = Lo;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

impl<'a> Iter<'a> {
    fn extract_lo(row: Row) -> Lo {
        let sha1: String = row.get(0);
        let sha1 = sha1.from_hex().unwrap(); // FIXME: filter invalid hashes? Or return a `Result`
        let oid: Oid = row.get(1);
        let size: i64 = row.get(2);

        Lo::new(sha1, oid, size)
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Lo;

    fn next(&mut self) -> Option<Lo> {
        match self.rows.next() {
            Some(v) => Some(Iter::extract_lo(v)),
            None => None
        }
    }
}
