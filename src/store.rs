//! Storing of objects in S3

use error::Result;
use lo::{Data, Lo};
use rusoto_s3::{PutObjectRequest, S3, S3Client};
use rusoto_credential::ProvideAwsCredentials;
use hyper::client::Client;
use std::fs::File;
use std::io::Read;

impl Lo {
    /// Store Large Object on S3
    ///
    /// Store Large Object data on S3 using the sha2 hash as key. The data in memory or the
    /// temporary file held by [`Data`] is dropped.
    pub fn store<P>(&mut self, client: &S3Client<P, Client>, bucket: &str) -> Result<()>
    where
        P: ProvideAwsCredentials,
    {
        let lo_data = self.take_lo_data();
        match lo_data {
            Data::File(ref temp) => {
                let mut file = File::open(&temp.path())?;
                #[cfg_attr(feature = "clippy", allow(cast_sign_loss, cast_possible_truncation))]
                let mut data = Vec::with_capacity(self.size() as usize);
                file.read_to_end(&mut data)?;
                self.store_read_data(data, client, bucket)
            }
            Data::Vector(data) => self.store_read_data(data, client, bucket),
            Data::None => panic!("Large Object must be fetched first"),
        }
    }

    fn store_read_data<P>(&self,
                          data: Vec<u8>,
                          client: &S3Client<P, Client>,
                          bucket: &str)
                          -> Result<()>
        where P: ProvideAwsCredentials
    {
        let request = PutObjectRequest {
            key: self.sha2_hex().expect("Large Object must be fetched first"),
            bucket: bucket.to_string(),
            body: Some(data),
            content_type: Some(self.mime_type().to_string()),
            ..Default::default()
        };
        client.put_object(&request)?;
        Ok(())
    }
}
