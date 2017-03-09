//! Storing of objects in S3

use error::Result;
use lo::{Data, Lo};
use s3::s3client::S3Client;
use s3::object::PutObjectRequest;
use aws::common::credentials::AwsCredentialsProvider;
use memmap::{Mmap, Protection};
use hyper::client::Client;

impl Lo {
    /// Store Large Object on S3
    ///
    /// Store Large Object data on S3 using the sha2 hash as key. The data in memory or the
    /// temporary file held by [`Data`] is dropped.
    pub fn store<P>(&mut self, client: &S3Client<P, Client>, bucket: &str) -> Result<()>
        where P: AwsCredentialsProvider
    {
        let lo_data = &self.take_lo_data();
        match *lo_data {
            Data::File(ref temp) => {
                // TODO:
                // Our AWS S3 library takes `&[u8]` as object content, it really should use trait
                // `Read`. To avoid excessive memory use, the file is mapped into memory for now.
                let mapped_file = Mmap::open_path(temp.as_ref(), Protection::Read)?;

                let data = unsafe {
                    // This is considered unsafe because the mapped file may be altered
                    // concurrently, violating Rust's safety guarantees. However, this shouldn't
                    // happen unintentionally with a temporary file.
                    mapped_file.as_slice()
                };
                self.store_read_data(data, client, bucket)
            }
            Data::Vector(ref data) => self.store_read_data(data, client, bucket),
            Data::None => panic!("Large Object must have been fetched from Postgres")
        }
    }

    fn store_read_data<P>(&self,
                          data: &[u8],
                          client: &S3Client<P, Client>,
                          bucket: &str)
                          -> Result<()>
        where P: AwsCredentialsProvider
    {
        let request = PutObjectRequest {
            key: self.sha2_base64().expect("Largo Object must have been fetched from Postgres"),
            bucket: bucket.to_string(),
            body: Some(data),
            content_type: Some(self.mime_type().to_string()),
            ..Default::default()
        };
        client.put_object(&request, None)?;
        Ok(())
    }
}
