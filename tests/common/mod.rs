extern crate hyper;
extern crate postgres;
extern crate rand;
extern crate rusoto_core;
extern crate rusoto_credential;
extern crate rusoto_s3;

use self::hyper::Client;
use self::rand::Rng;
use self::rusoto_core::region::Region;
use self::rusoto_credential::StaticProvider;
use self::rusoto_s3::{CreateBucketRequest, S3, S3Client};
use lo_migrate::thread::ThreadStat;

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
pub fn s3_conn() -> (S3Client<StaticProvider, Client>, String) {
    let bucket_name: String = rand::thread_rng().gen_ascii_chars().take(63).collect();

    let region = Region::Custom { name: "eu-east-3".to_owned(), endpoint: "http://localhost:8080".to_owned() };
    let provider = StaticProvider::new_minimal("access_key".to_owned(), "secret_key".to_owned());
    let http_client = Client::new();
    let client = S3Client::new(http_client, provider, region);

    let req = CreateBucketRequest { bucket: bucket_name.clone(), ..Default::default() };
    client.create_bucket(&req).unwrap();

    (client, bucket_name)
}

pub fn extract_stats(stats: &ThreadStat) -> (Option<u64>, Option<u64>, u64, u64, u64, u64, u64) {
    (stats.lo_total(),
     stats.lo_remaining(),
     stats.lo_observed(),
     stats.lo_received(),
     stats.lo_stored(),
     stats.lo_committed(),
     stats.lo_failed())
}
