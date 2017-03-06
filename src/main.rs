#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

extern crate lo_migrate;
extern crate postgres;
extern crate two_lock_queue;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate aws_sdk_rust;
extern crate url;

use postgres::{Connection, TlsMode};
use lo_migrate::retrieve::LoRetriever;
use lo_migrate::store::S3Manager;
use lo_migrate::commit;
use two_lock_queue as queue;
use url::Url;
use aws_sdk_rust::aws::common::credentials::DefaultCredentialsProvider;
use aws_sdk_rust::aws::s3::endpoint::Endpoint;
use aws_sdk_rust::aws::s3::endpoint::Signature;
use aws_sdk_rust::aws::common::region::Region;
use aws_sdk_rust::aws::common::credentials::ParametersProvider;

fn main() {
    env_logger::init().unwrap();
    // let db = "nice2_ecap_pege_20170304_s3_migration";
    // let db = "nice2_master_pege_20170304_s3_migration_1";
    let db = "nice2_ecap";
    let password = "...";
    let url = format!("postgresql://postgres:{}@localhost/{}", password, db);
    let conn = Connection::connect(url, TlsMode::None).unwrap();
    let retriever = LoRetriever::new(&conn, 1024).unwrap();

  //  println!("{}", retriever.into_iter().count());
    let mut objects: Vec<_> = retriever.into_iter().collect();
    for mut lo in &mut objects {
        println!();
        println!("{:?}", lo);
        lo.retrieve_lo_data(&conn, (1024_i64).pow(2)).unwrap();
        println!("{:?}", lo);

        // let provider = DefaultCredentialsProvider::new(None).unwrap();
        let provider = ParametersProvider::with_parameters("abc", "abc", Some("abc".to_string())).unwrap();
        let endpoint = Endpoint {
            region: Region::ApNortheast1,
            signature: Signature::V4,
            endpoint: Some(Url::parse("http://172.17.0.2").unwrap()),
            proxy: None,
            user_agent: None,
            is_bucket_virtual: false,
        };
        let manager = S3Manager::new(provider, endpoint, "test".to_string());
        lo.store(&manager).unwrap();
    }

    commit::commit(&objects, &conn).unwrap();
}
