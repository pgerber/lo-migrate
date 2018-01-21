//! Storing of objects in S3

use error::Result;
use lo::{Data, Lo};
use rusoto_s3::{AbortMultipartUploadRequest, CompleteMultipartUploadRequest,
                CompletedMultipartUpload, CompletedPart, CreateMultipartUploadRequest,
                PutObjectRequest, S3, S3Client, UploadPartRequest};
use rusoto_credential::ProvideAwsCredentials;
use hyper::client::Client;
use std::fs::File;
use std::io::{self, Read};

impl Lo {
    /// Store Large Object on S3
    ///
    /// Store Large Object data on S3 using the sha2 hash as key. The data in memory or the
    /// temporary file held by [`Data`] is dropped.
    pub fn store<P>(
        &mut self,
        client: &S3Client<P, Client>,
        bucket: &str,
        chunk_size: usize,
    ) -> Result<()>
    where
        P: ProvideAwsCredentials,
    {
        let lo_data = self.take_lo_data();
        match lo_data {
            Data::File(ref temp) => {
                let mut file = File::open(&temp.path())?;
                if self.size() <= chunk_size as i64 {
                    #[cfg_attr(feature = "clippy", allow(cast_sign_loss, cast_possible_truncation))]
                    let mut data = Vec::with_capacity(self.size() as usize);
                    file.read_to_end(&mut data)?;
                    assert_eq!(
                        self.size(),
                        data.len() as i64,
                        "unexpected size ({}) of buffer file for {:?}",
                        data.len(),
                        self
                    );
                    self.upload(data, client, bucket)
                } else {
                    self.upload_multipart(&mut file, client, bucket, chunk_size)
                }
            }
            Data::Vector(data) => self.upload(data, client, bucket),
            Data::None => panic!("Large Object must be fetched first"),
        }
    }

    fn upload<P>(&self, data: Vec<u8>, client: &S3Client<P, Client>, bucket: &str) -> Result<()>
    where
        P: ProvideAwsCredentials,
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

    fn upload_multipart<D, P>(
        &self,
        data: &mut D,
        client: &S3Client<P, Client>,
        bucket: &str,
        chunk_size: usize,
    ) -> Result<()>
    where
        D: Read,
        P: ProvideAwsCredentials,
    {
        let key = self.sha2_hex().expect("Large Object must be fetched first");
        let upload = client.create_multipart_upload(&CreateMultipartUploadRequest {
            key: key.clone(),
            bucket: bucket.to_string(),
            content_type: Some(self.mime_type().to_string()),
            ..Default::default()
        })?;

        let upload_id = upload.upload_id.expect("Missing upload ID");
        let mut buffer = vec![0; chunk_size];
        let mut tot_len: i64 = 0;
        let mut parts = Vec::new();
        for part in 1.. {
            match data.read(&mut buffer) {
                Ok(0) => break,
                Ok(len) => {
                    tot_len += len as i64;
                    let part =
                        self.upload_part(client, bucket, &key, &upload_id, part, &buffer[..len])?;
                    parts.push(part);
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
                    debug!("Interrupt while reading from buffer file of {:?}", self);
                }
                Err(e) => return Err(e.into()),
            }
        }

        assert_eq!(
            self.size(),
            tot_len,
            "Unexpected size ({}) of buffer file for {:?}",
            tot_len,
            self
        );

        client.complete_multipart_upload(&CompleteMultipartUploadRequest {
            bucket: bucket.to_owned(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            multipart_upload: Some(CompletedMultipartUpload { parts: Some(parts) }),
            ..Default::default()
        })?;

        Ok(())
    }

    fn upload_part<P>(
        &self,
        client: &S3Client<P, Client>,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part: i64,
        data: &[u8],
    ) -> Result<CompletedPart>
    where
        P: ProvideAwsCredentials,
    {
        let resp = client.upload_part(&UploadPartRequest {
            bucket: bucket.to_string(),
            key: key.to_owned(),
            upload_id: upload_id.to_owned(),
            part_number: part,
            body: Some(data.to_vec()),
            ..Default::default()
        });

        match resp {
            Ok(r) => Ok(CompletedPart {
                e_tag: r.e_tag.clone(),
                part_number: Some(part),
            }),
            Err(e) => {
                self.abort_upload(client, bucket, &key, &upload_id);
                Err(e.into())
            }
        }
    }

    fn abort_upload<P>(
        &self,
        client: &S3Client<P, Client>,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) where
        P: ProvideAwsCredentials,
    {
        let status = client.abort_multipart_upload(&AbortMultipartUploadRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            upload_id: upload_id.to_owned(),
            ..Default::default()
        });

        if let Err(e) = status {
            error!("failed to abort multipart upload for {:?}: {}", self, e);
        }
    }
}
