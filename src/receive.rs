//! Fetching Largo Objects from Postgres

use error::Result;
use lo::{Data, Lo};
use mktemp::Temp;
use postgres::Connection;
use postgres_large_object::{LargeObjectTransactionExt, Mode};
use digest::Digest;
use std::fs;
use std::io;
use std::io::Read;

impl Lo {
    /// Retrieve Large Object data
    ///
    /// Retrieve Large Object from Postgres and store in memory if its size is less or equal
    /// `size_threshold` or write it to a temporary file if larger.
    ///
    /// If Large Object has already been retrieved `size_threshold` is ignored and a reference
    /// to the already existing [`Data`] is returned.
    pub fn retrieve_lo_data<D>(&mut self, conn: &Connection, size_threshold: i64) -> Result<&Data>
        where D: Digest + Default
    {
        if self.lo_data().is_none() {
            let data = self.retrieve_lo_data_internal::<D>(conn, size_threshold)?;
            self.set_lo_data(data);
        };
        Ok(self.lo_data())
    }

    fn retrieve_lo_data_internal<D>(&mut self,
                                    conn: &Connection,
                                    size_threshold: i64)
                                    -> Result<Data>
        where D: Digest + Default
    {
        let trans = conn.transaction()?;
        let mut large_object = trans.open_large_object(self.oid(), Mode::Read)?;
        let mut sha2_reader: DigestReader<D> = DigestReader::new(&mut large_object);

        let (data, size) = if self.size() <= size_threshold {
            // keep binary data in memory
            #[cfg(feature = "try_from")]
            let size = self.size().try_into().unwrap();

            #[cfg(not(feature = "try_from"))]
            #[allow(cast_possible_truncation)]
            #[allow(cast_sign_loss)]
            let size = self.size() as usize;

            let mut data = Vec::with_capacity(size);
            let size = io::copy(&mut sha2_reader, &mut data)?;
            (Data::Vector(data), size)
        } else {
            // keep binary data in temporary file
            let temp_file = Temp::new_file()?;
            let mut file = fs::File::create(&temp_file)?;
            let size = io::copy(&mut sha2_reader, &mut file)?;
            (Data::File(temp_file), size)
        };

        #[cfg_attr(feature = "clippy", allow(cast_possible_wrap))]
        #[cfg_attr(feature = "clippy", allow(cast_sign_loss))]
        let expected_size = self.size() as u64;
        if expected_size as u64 != size {
            warn!("size of binary read ({} bytes) differs from size according to \
                   _nice_binary.size ({} bytes).",
                  size,
                  self.size());
        };

        self.set_sha2(sha2_reader.hash());
        Ok(data)
    }
}

/// Reader that wraps another reader and calculates the hash of the data passed through it.
struct DigestReader<'a, D>
    where D: Digest
{
    hasher: D,
    inner: &'a mut Read,
}

impl<'a, D> DigestReader<'a, D>
    where D: Digest + Default
{
    fn new<T>(inner: &'a mut T) -> Self
        where T: Read
    {
        DigestReader {
            hasher: Default::default(),
            inner: inner,
        }
    }

    /// Returns the hash of all data passed through the reader
    ///
    /// This operation consumes the reader.
    fn hash(self) -> Vec<u8> {
        self.hasher.result().into_iter().collect()
    }
}

impl<'a, D> Read for DigestReader<'a, D>
    where D: Digest
{
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let size = self.inner.read(&mut buf)?;
        self.hasher.input(&buf[..size]);
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    extern crate base64;
    extern crate postgres;

    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn sha2_reader_partially_stale_buffer() {
        let data = b"123456789";
        let mut inner_reader = &data[..];
        let mut sha2_reader: DigestReader<Sha256> = DigestReader::new(&mut inner_reader);
        let mut buf = [0; 5];
        assert_eq!(sha2_reader.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf, b"12345");
        assert_eq!(sha2_reader.read(&mut buf).unwrap(), 4);
        assert_eq!(&buf[..4], b"6789");
        assert_hash_correct(&sha2_reader.hash(), data);
    }

    fn assert_hash_correct(hash: &[u8], data: &[u8]) {
        let mut hasher = Sha256::new();
        hasher.input(data);
        assert_eq!(hash[..], hasher.result()[..]);
    }

    #[test]
    fn receive_vec_or_file() {
        let conn = postgres::Connection::connect("postgresql://postgres@localhost/postgres",
                                                 postgres::TlsMode::None)
            .unwrap();
        conn.execute("CREATE DATABASE src_receive_tests_receive", &[]).unwrap();

        let conn = postgres::Connection::connect("postgresql:\
                                                  //postgres@localhost/src_receive_tests_receive",
                                                 postgres::TlsMode::None)
            .unwrap();
        conn.batch_execute(include_str!("../tests/clean_data.sql")).unwrap();

        // keep object in memory
        let mut lo = Lo::new(base64::decode("43fe96d43c21d1f86780f47b28fe24f142c395d9").unwrap(),
                             198485881,
                             6842,
                             "text/test".to_string());
        let data = lo.retrieve_lo_data::<Sha256>(&conn, 6842).unwrap();
        assert!(if let Data::Vector(_) = *data {
            true
        } else {
            false
        });

        // keep object in temporary file
        let mut lo = Lo::new(base64::decode("43fe96d43c21d1f86780f47b28fe24f142c395d9").unwrap(),
                             198485881,
                             6842,
                             "text/test".to_string());
        let data = lo.retrieve_lo_data::<Sha256>(&conn, 6483).unwrap();
        assert!(if let Data::File(_) = *data {
            true
        } else {
            false
        });
    }
}
