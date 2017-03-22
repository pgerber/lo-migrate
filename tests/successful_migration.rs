extern crate aws_sdk_rust;
extern crate base64;
extern crate hyper;
extern crate lo_migrate;
extern crate postgres;
extern crate sha2;
extern crate two_lock_queue as queue;

mod common;
use common::*;

use aws_sdk_rust::aws::common::credentials::ParametersProvider;
use aws_sdk_rust::aws::s3::object::GetObjectRequest;
use aws_sdk_rust::aws::s3::s3client::S3Client;
use hyper::Client;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use lo_migrate::thread::{Committer, Observer, Storer, Receiver, ThreadStat};


// sha256 hashes of clean_data.sql sorted by OID (DB column data)
const SHA256_HASHES: [&str; 5] = ["uAGE/a7gZcsx4fJBe7FEEs64Gc9XpGJG7FtPjale8mg=",
                                  "6Gri/VBW/Ndfap7YhP3CPYgL5tXeYVmQHlZO0ZsSqgA=",
                                  "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
                                  "6Xpjw0uyKZ6XfsWuoWH0n/3TxqcZyIOFBOIPio24WuI=",
                                  "K/MNCySkrJayHaK/QB8uFgY4Oc7IJj5/M5e2+vusAq0="];

// mime types of clean_data.sql sorted by OID (DB column data)
const MIME_TYPES: [&str; 5] = ["", "octet/stream", "octet/stream", "text/plain", "octet/stream"];

/// Test complete migration from Postgres to S3
#[test]
fn migration() {
    let stats = ThreadStat::new();
    let pg_conn = postgres_conn();
    let s3_client = s3_conn();

    // create database
    pg_conn.batch_execute(include_str!("clean_data.sql")).unwrap();

    // get list of large objects
    let (rcv_tx, rcv_rx) = queue::unbounded();
    let observer = Observer::new(&stats, &pg_conn);
    observer.start_worker(Arc::new(rcv_tx), 1024).unwrap();
    assert_eq!(extract_stats(&stats), (Some(5), 5, 0, 0, 0));

    // fetch large objects from postgres
    let (str_tx, str_rx) = queue::unbounded();
    let receiver = Receiver::new(&stats, &pg_conn);
    receiver.start_worker::<Sha256>(Arc::new(rcv_rx), Arc::new(str_tx), 28).unwrap();
    assert_eq!(extract_stats(&stats), (Some(5), 5, 5, 0, 0));

    // store objects to S3
    let (cmt_tx, cmt_rx) = queue::unbounded();
    let storer = Storer::new(&stats);
    storer.start_worker(Arc::new(str_rx),
                      Arc::new(cmt_tx),
                      &s3_client,
                      "test_bucket")
        .unwrap();
    assert_eq!(extract_stats(&stats), (Some(5), 5, 5, 5, 0));

    // commit sha256 hashes to postgres
    let committer = Committer::new(&stats, &pg_conn);
    committer.start_worker(Arc::new(cmt_rx), 2).unwrap();
    assert_eq!(extract_stats(&stats), (Some(5), 5, 5, 5, 5));

    // verify sha256 hashes
    let sha2_hashes: Vec<String> = pg_conn.query("SELECT sha2 FROM _nice_binary WHERE sha2 <> \
                '0000000000000000000000000000000000000000000=' ORDER BY data",
               &[])
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(&sha2_hashes, &SHA256_HASHES);

    // verify S3 bucket content
    for (sha256, mime_type) in SHA256_HASHES.iter().zip(&MIME_TYPES) {
        assert_object_in_store(&s3_client, sha256, mime_type);
    }
}

fn assert_object_in_store(client: &S3Client<ParametersProvider, Client>,
                          expected_sha256: &str,
                          mime: &str) {
    let request = GetObjectRequest {
        bucket: "test_bucket".to_string(),
        key: expected_sha256.to_string(),
        ..Default::default()
    };
    let response = client.get_object(&request, None).unwrap();
    let mut actual_sha256 = Sha256::new();
    actual_sha256.input(response.get_body());

    assert_eq!(&expected_sha256, &base64::encode(&actual_sha256.result()));
    assert_eq!(&response.content_type, mime);
}

fn extract_stats(stats: &ThreadStat) -> (Option<u64>, u64, u64, u64, u64) {
    (stats.lo_total(), stats.lo_observed(), stats.lo_received(), stats.lo_stored(), stats.lo_committed())
}
