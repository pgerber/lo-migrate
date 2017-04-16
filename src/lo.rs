//! Definition and generic implementation of `Lo`

use postgres::types::Oid;
use std::fmt;
use std::mem;
use serialize::hex::ToHex;
use mkstemp::TempFile;

/// Large Object Stored in memory or in a temporary file
pub enum Data {
    /// Large Object stored in memory
    Vector(Vec<u8>),

    /// Large Object stored in a temporary file
    File(TempFile),

    /// Large Object not yet or no longer available
    ///
    /// See [`Lo::retrieve_lo_data`] for show to retrieve it.
    None,
}

impl fmt::Debug for Data {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Data::Vector(ref v) => {
                let repr = debug_fmt_slice(v);
                write!(fmt, "Vector({})", repr)
            }
            Data::File(ref f) => write!(fmt, "File({:?})", f.path()),
            Data::None => write!(fmt, "None"),
        }
    }
}

impl Data {
    pub fn is_none(&self) -> bool {
        if let Data::None = *self { true } else { false }
    }
}

/// Representation of a Large Object
pub struct Lo {
    /// sha1 hash of object
    sha1: Vec<u8>,

    /// Postgres object ID
    oid: Oid,

    /// sha2 hash
    ///
    /// Only available if Large Object has been retrieved. Set by `Lo::retrieve_lo_data`.
    sha2: Option<Vec<u8>>,

    /// Large Object binary data
    data: Data,

    /// Size of Large Object according to Nice2 database (column _nice_binary.size)
    size: i64,

    /// Mime type from _nice_binary.mime_type)
    mime_type: String,
}

impl Lo {
    /// create new [`Lo`].
    pub fn new(sha1: Vec<u8>, oid: Oid, size: i64, mime_type: String) -> Lo {
        Lo {
            sha1: sha1,
            oid: oid,
            sha2: None,
            data: Data::None,
            size: size,
            mime_type: mime_type,
        }
    }

    /// sha1 hash of Large Object
    pub fn sha1(&self) -> &Vec<u8> {
        &self.sha1
    }

    /// sha1 hash in lower-case hexadecimal representation.
    ///
    /// Example hash: `"da39a3ee5e6b4b0d3255bfef95601890afd80709"`
    pub fn sha1_hex(&self) -> String {
        self.sha1.to_hex()
    }

    /// sha2 256 bit hash
    pub fn sha2(&self) -> Option<&Vec<u8>> {
        self.sha2.as_ref()
    }

    // Set sha2 hash
    pub fn set_sha2(&mut self, data: Vec<u8>) {
        self.sha2 = Some(data);
    }

    /// sha2 hash encoded as hex
    ///
    /// Example hash: `"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"`
    pub fn sha2_hex(&self) -> Option<String> {
        self.sha2.as_ref().map(|h| h.to_hex())
    }

    /// Size of Large Object (as stored in _nice_binary.size)
    pub fn lo_size(&self) -> i64 {
        self.size
    }

    /// Take data and move ownership to caller.
    ///
    /// Data stored in [`Lo`] is replaced by [`Data::None`].
    pub fn take_lo_data(&mut self) -> Data {
        mem::replace(&mut self.data, Data::None)
    }

    /// Get reference to [`Data`]
    pub fn lo_data(&self) -> &Data {
        &self.data
    }

    /// Set [`Data`]
    pub fn set_lo_data(&mut self, data: Data) {
        self.data = data;
    }

    /// Get mime type as stored in _nice_binary.mime_type
    pub fn mime_type(&self) -> &str {
        &self.mime_type
    }

    /// Postgres Large Object ID
    pub fn oid(&self) -> Oid {
        self.oid
    }

    /// Size of object according to _nice_binary.size
    pub fn size(&self) -> i64 {
        self.size
    }
}

impl fmt::Debug for Lo {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let size = format!("{} bytes", self.size);

        let sha1 = debug_fmt_slice(&self.sha1);

        let sha2 = match self.sha2 {
            Some(ref v) => Some(debug_fmt_slice(v)),
            None => None,
        };

        fmt.debug_struct("Lo")
            .field("size", &size)
            .field("sha1", &sha1)
            .field("oid", &self.oid)
            .field("mime", &self.mime_type)
            .field("sha2", &sha2)
            .field("data", &self.data)
            .finish()
    }
}

fn debug_fmt_slice(slice: &[u8]) -> String {
    const TRUNCATE_AT: usize = 4; // show at most N bytes
    const STRING_CAPACITY: usize = TRUNCATE_AT * 2 + 5;
    let end = if slice.len() > TRUNCATE_AT {
        TRUNCATE_AT
    } else {
        slice.len()
    };

    if end > 0 {
        let mut repr = String::with_capacity(STRING_CAPACITY);
        repr.push_str("0x");
        repr.push_str(&slice[..end].to_hex());
        if slice.len() > TRUNCATE_AT {
            repr.push_str("...");
            debug_assert_eq!(repr.len(), STRING_CAPACITY);
        }
        debug_assert!(repr.len() <= STRING_CAPACITY);
        repr
    } else {
        "".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATA: [u8; 13] = [b'b'; 13];
    const SHA1: [u8; 20] = [233, 215, 31, 94, 231, 201, 45, 109, 201, 233, 47, 253, 173, 23, 184,
                            189, 73, 65, 143, 152];
    const SHA1_HEX: &str = "e9d71f5ee7c92d6dc9e92ffdad17b8bd49418f98";
    const SHA2: [u8; 32] = [62, 35, 232, 22, 0, 57, 89, 74, 51, 137, 79, 101, 100, 225, 177, 52,
                            139, 189, 122, 0, 136, 212, 44, 74, 203, 115, 238, 174, 213, 156, 0,
                            157];
    const SHA2_HEX: &str = "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d";

    #[test]
    fn initial_state() {
        let lo = Lo::new(SHA1[..].into(), 82, 159, "text/plain".to_string());
        assert_eq!(lo.sha1(), &SHA1);
        assert_eq!(lo.sha1_hex(), SHA1_HEX);
        assert_eq!(lo.oid(), 82);
        assert_eq!(lo.size(), 159);
        assert_eq!(lo.mime_type(), "text/plain");
    }

    #[test]
    fn sha2_unset() {
        let lo = Lo::new(SHA1[..].into(), 82, 159, "text/plain".to_string());
        assert_eq!(lo.sha2(), None);
        assert_eq!(lo.sha2_hex(), None);
    }

    #[test]
    fn sha2_set() {
        let mut lo = Lo::new(SHA1[..].into(), 82, 159, "text/plain".to_string());
        lo.set_sha2(SHA2[..].into());
        assert_eq!(lo.sha2().unwrap(), &SHA2);
        assert_eq!(lo.sha2_hex().unwrap(), SHA2_HEX);
    }

    #[test]
    fn set_data() {
        let mut lo = Lo::new(SHA1[..].into(), 82, 159, "text/plain".to_string());
        assert!(lo.lo_data().is_none());
        lo.set_lo_data(Data::Vector(DATA[..].into()));
        match *lo.lo_data() {
            Data::Vector(ref v) => assert_eq!(v[..], DATA[..]),
            _ => panic!(),
        }
        assert!(!lo.lo_data().is_none());
    }

    #[test]
    fn take_data() {
        let mut lo = Lo::new(SHA1[..].into(), 82, 159, "text/plain".to_string());
        lo.set_lo_data(Data::Vector(DATA[..].into()));
        assert!(!lo.lo_data().is_none());
        match lo.take_lo_data() {
            Data::Vector(ref v) => assert_eq!(v[..], DATA[..]),
            _ => panic!(),
        }
        assert!(lo.lo_data().is_none());
    }

    #[test]
    fn debug_fmt_slice_test() {
        assert_eq!("", debug_fmt_slice(&[]));
        assert_eq!("0x1234", debug_fmt_slice(&[0x12, 0x34]));
        assert_eq!("0x12345678", debug_fmt_slice(&[0x12, 0x34, 0x56, 0x78]));
        assert_eq!("0x12345678...",
                   debug_fmt_slice(&[0x12, 0x34, 0x56, 0x78, 0x9a]));
    }
}
