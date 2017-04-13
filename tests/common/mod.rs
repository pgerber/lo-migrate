extern crate aws_sdk_rust;
extern crate postgres;
extern crate rand;
extern crate hyper;

use aws_sdk_rust::aws::common::credentials::ParametersProvider;
use aws_sdk_rust::aws::common::region::Region;
use aws_sdk_rust::aws::s3::bucket::CreateBucketRequest;
use aws_sdk_rust::aws::s3::endpoint::{Endpoint, Signature};
use aws_sdk_rust::aws::s3::s3client::S3Client;
use hyper::{Client, Url};
use self::rand::Rng;

/// create connection to Postgres
#[cfg(feature = "postgres_tests")]
pub fn postgres_conn() -> postgres::Connection {
    let db_name: String = rand::thread_rng().gen_ascii_chars().take(63).collect();

    let create_conn = postgres::Connection::connect("postgresql://postgres@localhost/postgres",
                                                    postgres::TlsMode::None)
        .unwrap();
    create_conn.execute(&format!("CREATE DATABASE \"{}\"", db_name), &[]).unwrap();

    postgres::Connection::connect(format!("postgresql://postgres@localhost/{}", db_name),
                                  postgres::TlsMode::None)
        .unwrap()
}

/// create connection to S3
#[cfg(feature = "s3_tests")]
pub fn s3_conn() -> (S3Client<ParametersProvider, Client>, String) {
    let bucket_name: String = rand::thread_rng().gen_ascii_chars().take(63).collect();

    let endpoint = Endpoint {
        region: Region::ApNortheast1,
        signature: Signature::V2,
        endpoint: Some(Url::parse("http://localhost:8080").unwrap()),
        proxy: None,
        user_agent: None,
        is_bucket_virtual: true,
    };

    let provider = ParametersProvider::with_parameters("access_key", "secret_key", None).unwrap();
    let client = S3Client::new(provider, endpoint);

    let req = CreateBucketRequest { bucket: bucket_name.clone(), ..Default::default() };
    client.create_bucket(&req).unwrap();

    (client, bucket_name)
}
