//! Fetching Large Objects from Postgres

use error::{MigrationError, Result};
use lo::{Data, Lo};
use mkstemp::TempFile;
use postgres::Connection;
use postgres_large_object::{LargeObjectTransactionExt, Mode};
use digest::Digest;
#[cfg(feature = "try_from")]
use std::convert::TryInto;
use std::env;
use std::io;
use std::io::{Read, Write};
use sha1::Sha1;
use serialize::hex::ToHex;

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
            self.retrieve_lo_data_internal::<D>(conn, size_threshold)?;
        };
        Ok(self.lo_data())
    }

    fn retrieve_lo_data_internal<D>(&mut self,
                                    conn: &Connection,
                                    size_threshold: i64)
                                    -> Result<()>
        where D: Digest + Default
    {
        let trans = conn.transaction()?;
        let mut large_object = trans.open_large_object(self.oid(), Mode::Read)?;
        let mut sha_reader: DigestReader<D> = DigestReader::new(&mut large_object);

        let (data, size) = if self.size() <= size_threshold {
            // keep binary data in memory
            #[cfg(feature = "try_from")]
            let size = self.size().try_into().expect("size limit exceeded");

            #[cfg(not(feature = "try_from"))]
            #[cfg_attr(feature = "clippy", allow(cast_possible_truncation))]
            #[cfg_attr(feature = "clippy", allow(cast_sign_loss))]
            let size = self.size() as usize;

            let mut data = Vec::with_capacity(size);
            let size = io::copy(&mut sha_reader, &mut data)?;
            (Data::Vector(data), size)
        } else {
            // keep binary data in temporary file
            let mut temp_path = env::temp_dir();
            temp_path.push("lo_migrate.XXXXXX");
            let mut temp_file =
                TempFile::new(temp_path.to_str().expect("tempdir not a UTF-8 path"), true)?;
            let size = io::copy(&mut sha_reader, &mut temp_file)?;
            temp_file.flush()?;
            (Data::File(temp_file), size)
        };

        #[cfg_attr(feature = "clippy", allow(cast_possible_wrap))]
        #[cfg_attr(feature = "clippy", allow(cast_sign_loss))]
        let expected_size = self.size() as u64;
        let (sha1, new_hash) = sha_reader.hashes();
        if expected_size == size && &sha1 == self.sha1() {
            self.set_sha2(new_hash);
            self.set_lo_data(data);
            Ok(())
        } else {
            Err(MigrationError::InvalidObject(format!("Expected object with hash {} of size {} bytes but found {:?}", sha1.to_hex(), size, self)))
        }
    }
}

/// Reader that wraps another reader and calculates the hash of the data passed through it.
struct DigestReader<'a, D>
    where D: Digest
{
    hasher: D,
    sha1_hasher: Sha1,
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
            sha1_hasher: Default::default(),
            inner: inner,
        }
    }

    /// Returns the hashes of all data passed through the reader
    ///
    /// Return a tuble with the legacy sha1 hash and the new sha2 hash.
    fn hashes(self) -> (Vec<u8>, Vec<u8>) {
        let old = self.sha1_hasher.result().into_iter().collect();
        let new = self.hasher.result().into_iter().collect();
        (old, new)
    }
}

impl<'a, D> Read for DigestReader<'a, D>
    where D: Digest
{
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let size = self.inner.read(&mut buf)?;
        self.hasher.input(&buf[..size]);
        self.sha1_hasher.input(&buf[..size]);
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    extern crate postgres;
    extern crate rand;
    extern crate sha2;

    use super::*;
    use self::sha2::{Digest, Sha256};

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
        let (sha1, sha2) = sha2_reader.hashes();
        assert_hash_correct::<Sha1>(&sha1, data);
        assert_hash_correct::<Sha256>(&sha2, data);
    }

    fn assert_hash_correct<Hasher>(hash: &[u8], data: &[u8])
        where Hasher: Digest
    {
        let mut hasher: Hasher = Default::default();
        hasher.input(data);
        assert_eq!(hash[..], hasher.result()[..]);
    }

    #[test]
    #[cfg(feature = "postgres_tests")]
    fn receive_vec_or_file() {
        use serialize::hex::FromHex;
        use self::rand::Rng;

        let db_name: String = rand::thread_rng().gen_ascii_chars().take(63).collect();

        let conn = postgres::Connection::connect("postgresql://postgres@localhost/postgres",
                                                 postgres::TlsMode::None)
            .unwrap();
        conn.batch_execute(&format!("CREATE DATABASE \"{}\";", db_name))
            .unwrap();

        let conn = postgres::Connection::connect(format!("postgresql://postgres@localhost/{}",
                                                         db_name),
                                                 postgres::TlsMode::None)
            .unwrap();
        conn.batch_execute(include_str!("../tests/clean_data.sql")).unwrap();

        // keep object in memory
        let mut lo = Lo::new("43fe96d43c21d1f86780f47b28fe24f142c395d9".from_hex().unwrap(),
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
        let mut lo = Lo::new("43fe96d43c21d1f86780f47b28fe24f142c395d9".from_hex().unwrap(),
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
