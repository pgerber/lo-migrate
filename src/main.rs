#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

extern crate lo_migrate;
extern crate postgres;
extern crate two_lock_queue;

use postgres::{Connection, TlsMode};
use lo_migrate::retrieve::LoRetriever;
use two_lock_queue as queue;

fn main() {
    // let db = "nice2_ecap_pege_20170304_s3_migration";
    // let db = "nice2_master_pege_20170304_s3_migration_1";
    let db = "nice2_ecap";
    let password = "...";
    let url = format!("postgresql://postgres:{}@localhost/{}", password, db);
    let conn = Connection::connect(url, TlsMode::None).unwrap();
    let retriever = LoRetriever::new(&conn, 1024).unwrap();

  //  println!("{}", retriever.into_iter().count());
    for mut lo in retriever.into_iter() {
        lo.retrieve_lo_data(&conn, (1024_i64).pow(2)).unwrap();
        println!("{:?}", lo)
    }
}
