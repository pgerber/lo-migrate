use postgres::Connection;
use postgres::types::Oid;
use postgres_large_object::{LargeObjectTransactionExt, Mode};
use std::fmt;
use std::fs;
use std::io;
use std::io::Read;
use std::mem;
#[cfg(feature = "try_from")]
use std::convert::TryInto;
use serialize::hex::ToHex;
use common::Result;
use mktemp::Temp;
use sha2::{Digest, Sha256};
use base64;

pub enum Data { // TODO: look at memory footprint
    Vector(Vec<u8>),
    File(Temp),
    None
}

impl fmt::Debug for Data {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Data::Vector(ref v) => write!(fmt, "Vector(0x{}...)", &v[..4].to_hex()),
            &Data::File(ref f) => write!(fmt, "File({:?})", f.as_ref()),
            &Data::None => write!(fmt, "None")
        }
    }
}

impl Data {
    fn is_none(&self) -> bool {
        if let &Data::None = self {
            true
        } else {
            false
        }
    }
}

pub struct Lo {
    sha1: Vec<u8>,
    oid:  Oid,
    sha2: Option<Vec<u8>>,
    data: Data,
    size: i64,
}

impl Lo {
    pub fn new(sha1: Vec<u8>, oid: Oid, size: i64) -> Lo {
        Lo {
            sha1: sha1,
            oid:  oid,
            sha2: None,
            data: Data::None,
            size: size,
        }
    }

    pub fn sha1(&self) -> &Vec<u8> {
        &self.sha1
    }

    pub fn sha1_hex(&self) -> String {
        self.sha1.to_hex()
    }

    pub fn sha2(&self) -> Option<&Vec<u8>> {
        self.sha2.as_ref()
    }

    pub fn sha2_base64(&self) -> Option<String> {
        self.sha2.as_ref().map(|h| base64::encode(&h))
    }

    pub fn lo_size(&self) -> i64 {
        self.size
    }

    pub fn take_lo_data(&mut self) -> Data {
        mem::replace(&mut self.data, Data::None)
    }

    pub fn lo_data(&self) -> &Data {
        &self.data
    }

    pub fn retrieve_lo_data(&mut self, conn: &Connection, size_threshold: i64) -> Result<&Data> {
        if self.data.is_none() {
            self.data = self.retrieve_lo_data_internal(conn, size_threshold)?;
        };
        Ok(&self.data)
    }

    fn retrieve_lo_data_internal(&mut self, conn: &Connection, size_threshold: i64) -> Result<Data> {
        let trans = conn.transaction()?;
        let mut large_object = trans.open_large_object(self.oid, Mode::Read)?;
        let mut sha2_reader: DigestReader<Sha256> = DigestReader::new(&mut large_object);

        let data = if self.size <= size_threshold {
            // read to memory
            #[cfg(feature = "try_from")]
            let size = self.size.try_into().unwrap();

            #[cfg(not(feature = "try_from"))]
            let size = self.size as usize;

            let mut data = Vec::with_capacity(size);
            io::copy(&mut sha2_reader, &mut data)?;
            Data::Vector(data)
        } else {
            // write to temporary file
            let temp_file = Temp::new_file()?;
            let mut file = fs::File::create(&temp_file)?;
            io::copy(&mut sha2_reader, &mut file)?;
            Data::File(temp_file)
        };

        self.sha2 = Some(sha2_reader.sha2());
        Ok(data)
    }
}

impl fmt::Debug for Lo {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let size = format!("{} bytes", self.size);
        let sha1 = self.sha1.to_hex();
        let sha2 = match self.sha2 {
            Some(ref v) => {
                let mut hex = v[..5].to_hex();
                hex.reserve(3 + 4);
                hex.push_str("...");
                hex.push_str(&v[18..].to_hex());
                Some(hex)
            },
            None => None
        };

        fmt.debug_struct("Lo")
            .field("size", &size)
            .field("sha1", &sha1)
            .field("oid",  &self.oid)
            .field("sha2", &sha2)
            .field("data", &self.data)
            .finish()
    }
}

/// Wrap a Reader to be able to calculate the sha2 hash while reading
struct DigestReader<'a, D> where D: Digest {
    hasher: D,
    inner:  &'a mut Read
}

impl<'a, D> DigestReader<'a, D> where D: Digest + Default {
    fn new<T>(inner: &'a mut T) -> Self where T: Read {
        DigestReader {
            hasher: Default::default(),
            inner: inner
        }
    }

    fn sha2(self) -> Vec<u8> {
        self.hasher.result().into_iter().collect()  // FIXME: is there a better way than copying the result?
    }
}

impl<'a, D> Read for DigestReader<'a, D> where D: Digest {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let size = self.inner.read(&mut buf)?;
        self.hasher.input(&buf[..size]);
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
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
        assert_hash_correct(&sha2_reader.sha2(), data);
    }

    fn assert_hash_correct(hash: &[u8], data: &[u8]) {
        let mut hasher = Sha256::new();
        hasher.input(&data);
        assert_eq!(hash[..], hasher.result()[..]);
    }
}
