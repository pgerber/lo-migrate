use common::Result;
use lo::{Data, Lo};
use s3::s3client::S3Client;
use s3::bucket::BucketName;
use s3::endpoint::Endpoint;
use s3::object::PutObjectRequest;
use aws::common::credentials::AwsCredentialsProvider;
use memmap::{Mmap, Protection};
use hyper;

pub struct S3Manager<P> where P: AwsCredentialsProvider {
    client: S3Client<P, hyper::client::Client>,
    bucket: BucketName
}

impl<P> S3Manager<P> where P: AwsCredentialsProvider {
    pub fn new(credentials_provider: P, endpoint: Endpoint, bucket: String) -> Self
        where P: AwsCredentialsProvider
    {
        S3Manager {
            client: S3Client::new(credentials_provider, endpoint),
            bucket: bucket
        }
    }

    pub fn client(&self) -> &S3Client<P, hyper::client::Client> {
        &self.client
    }

    pub fn bucket(&self) -> BucketName {
        // TODO: For some reason the `s3` library wants a `String` and not a `&str`
        //       forcing us to copy the `String`. Should this be changed in the library?
        self.bucket.clone()
    }
}

impl Lo {
    /// Store Large Object on S3
    ///
    /// Store Large Object data on S3 using the sha2 hash as key, as returned by `[Lo::sha2_base64]`.
    /// The data in memory or the temporary file held by [`Data`] is dropped.
    ///
    /// TODO: Ignore already existing keys
    pub fn store<P>(&mut self, manager: &S3Manager<P>) -> Result<()>
        where P: AwsCredentialsProvider
    {
        let lo_data = &self.take_lo_data();
        match lo_data {
            &Data::File(ref temp) => {
                // TODO:
                // Our AWS S3 library takes `&[u8]` as object content, it really should use trait `Read`. To avoid excessive
                // memory use, the file is mapped into memory for now.
                let mapped_file = Mmap::open_path(temp.as_ref(), Protection::Read)?;

                let data = unsafe {
                    // This is considered unsafe because the mapped file may be altered concurrently,
                    // violating Rust's safety guarantees. However, this shouldn't happen on a temporary
                    // file without manual intervention.
                    mapped_file.as_slice()
                };
                self.store_read_data(&data, &manager)
            },
            &Data::Vector(ref data) => {
                self.store_read_data(&data, &manager)
            },
            &Data::None => panic!()  // FIXME: shouldn't be possible but should be handled anyway
        }
    }

    fn store_read_data<P>(&self, data: &[u8], manager: &S3Manager<P>) -> Result<()>
        where P: AwsCredentialsProvider
    {
        PutObjectRequest {
            key: self.sha2_base64().unwrap(),  // FIXME: hash could be missing
            bucket: manager.bucket(),
            body: Some(data),
            // TODO: do we need to set the content type?
            ..Default::default()
        };
        manager.client().put_object(&request, None)?;
        Ok(())
    }
}
