//! Definition and generic implementation of `Lo`

use postgres::types::Oid;
use std::fmt;
use std::mem;
#[cfg(feature = "try_from")]
use std::convert::TryInto;
use serialize::hex::ToHex;
use mktemp::Temp;
use base64;

/// Large Object Stored in memory or in a temporary file
pub enum Data {
    /// Large Object stored in memory
    Vector(Vec<u8>),

    /// Large Object stored in a temporary file
    File(Temp),

    /// Largo Object not yet or no longer available
    ///
    /// See [`Lo::retrieve_lo_data`] for show to retrieve it.
    None,
}

impl fmt::Debug for Data {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Data::Vector(ref v) => write!(fmt, "Vector(0x{}...)", &v[..4].to_hex()),
            Data::File(ref f) => write!(fmt, "File({:?})", f.as_ref()),
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

    /// sha2 hash encoded as base64
    ///
    /// Example hash: `"2jmj7l5rSw0yVb/vlWAYkK/YBwk="`
    pub fn sha2_base64(&self) -> Option<String> {
        self.sha2.as_ref().map(|h| base64::encode(h))
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

        let mut sha1 = self.sha1[..5].to_hex();
        sha1.push_str("...");

        let sha2 = match self.sha2 {
            Some(ref v) => {
                let mut hex = v[..5].to_hex();
                hex.push_str("...");
                Some(hex)
            }
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
