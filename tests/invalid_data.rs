#![cfg(feature = "postgres_tests")]

extern crate lo_migrate;
extern crate log;
extern crate postgres;
extern crate sha2;
extern crate simple_logger;
extern crate two_lock_queue as queue;

mod common;
use common::*;

use sha2::Sha256;
use std::sync::Arc;
use lo_migrate::thread::{Counter, Observer, Receiver, ThreadStat};

/// Test complete migration from Postgres to S3
#[test]
fn invalid_data() {
   simple_logger::init().unwrap();

    let stats = ThreadStat::new();
    let pg_conn = postgres_conn();
    let (s3_client, bucket_name) = s3_conn();

    // create database
    pg_conn.batch_execute(include_str!("invalid_data.sql")).unwrap();

    // count large objects
    let counter = Counter::new(&stats, &pg_conn);
    counter.start_worker().unwrap();

    // get list of large objects
    let (rcv_tx, rcv_rx) = queue::unbounded();
    let observer = Observer::new(&stats, &pg_conn);
    observer.start_worker(Arc::new(rcv_tx), 1024).unwrap();
    assert_eq!(extract_stats(&stats), (Some(4), Some(4), 2, 0, 0, 0, 2));

    // fetch large objects from postgres
    let (str_tx, str_rx) = queue::unbounded();
    let receiver = Receiver::new(&stats, &pg_conn);
    receiver.start_worker::<Sha256>(Arc::new(rcv_rx), Arc::new(str_tx), 28).unwrap();
    assert_eq!(extract_stats(&stats), (Some(4), Some(4), 2, 0, 0, 0, 4));
}
