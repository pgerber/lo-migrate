#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

extern crate lo_migrate;
extern crate postgres;
extern crate env_logger;
extern crate aws_sdk_rust;
extern crate url;
extern crate hyper;
extern crate two_lock_queue;

use postgres::{Connection, TlsMode};
use url::Url;
use aws_sdk_rust::aws::s3::s3client::S3Client;
use aws_sdk_rust::aws::s3::endpoint::Endpoint;
use aws_sdk_rust::aws::s3::endpoint::Signature;
use aws_sdk_rust::aws::common::region::Region;
use aws_sdk_rust::aws::common::credentials::ParametersProvider;
use hyper::client::Client;
use lo_migrate::thread::{Committer, Monitor, Observer, Receiver, Storer, ThreadStat};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use lo_migrate::error::Result;

fn connect_to_postgres(url: &str, count: usize) -> Vec<Connection> {
    let mut conns = Vec::with_capacity(count);
    for _ in 0..count {
        conns.push(Connection::connect(url, TlsMode::None).unwrap()); // FIXME
    }
    conns
}

fn connect_to_s3(access_key: &str,
                 secret_key: &str,
                 endpoint: &Endpoint,
                 count: usize)
                 -> Vec<S3Client<ParametersProvider, Client>> {
    let mut conns = Vec::with_capacity(count);
    for _ in 0..count {
        let credentials = ParametersProvider::with_parameters(access_key,
                                                              secret_key,
                                                              Some(access_key.to_string()))
            .unwrap(); // FIXME
        conns.push(S3Client::new(credentials, endpoint.clone()))
    }
    conns
}

fn print_thread_result(result: &Result<()>, thread_stat: &ThreadStat) {
    match *result {
        Err(ref e) if e.is_cancelled() => (),
        Err(ref e) if e.is_queue_hangup() => (),
        Err(ref e) => {
            println!("error in thread: {:?}", e);
            thread_stat.cancel();
        }
        Ok(_) => (),
    };
}

fn main() {
    env_logger::init().unwrap();

    let pg_db_name = "nice2_ecap";
    let pg_password = "";
    let pg_url = format!("postgresql://postgres:{}@localhost/{}",
                         pg_password,
                         pg_db_name);

    let s3_endpoint = Endpoint {
        region: Region::ApNortheast1,
        signature: Signature::V4,
        endpoint: Some(Url::parse("http://172.17.0.2").unwrap()),
        proxy: None,
        user_agent: None,
        is_bucket_virtual: false,
    };
    let s3_access_key = "abc";
    let s3_secret_key = "abc";
    let s3_bucket_name = "test";

    let receive_queue_size = 8196;
    let store_queue_size = 1024;
    let commit_queue_size = 8196;

    let receiver_thread_count = 50;
    let storer_thread_count = 50;
    let committer_thread_count = 2;

    let observer_pg_conns = connect_to_postgres(&pg_url, 1 /* multiple threads not supperted */);
    let receiver_pg_conns = connect_to_postgres(&pg_url, receiver_thread_count);
    let storer_s3_conns = connect_to_s3(s3_access_key, s3_secret_key, &s3_endpoint,
                                        storer_thread_count);
    let committer_pg_conns = connect_to_postgres(&pg_url, committer_thread_count);

    let thread_stat = ThreadStat::new();

    // all threads that have been started
    let mut threads = Vec::new();

    // Block is needed so the `Arc<Send<Lo>>s and `Arc<Receive<Lo>>s are dropped, hanging up the
    // queues
    {
        // queue between observer and receiver threads
        let (rcv_tx, rcv_rx) = two_lock_queue::channel(receive_queue_size);
        let (rcv_tx, rcv_rx) = (Arc::new(rcv_tx), Arc::new(rcv_rx));

        // queue between receiver and storer threads
        let (str_tx, str_rx) = two_lock_queue::channel(store_queue_size);
        let (str_tx, str_rx) = (Arc::new(str_tx), Arc::new(str_rx));

        // queue between committer thread and committer thread
        let (cmt_tx, cmt_rx) = two_lock_queue::channel(commit_queue_size);
        let (cmt_tx, cmt_rx) = (Arc::new(cmt_tx), Arc::new(cmt_rx));

        // create observer thread
        for conn in observer_pg_conns {
            let thread_stat = thread_stat.clone();
            let tx = rcv_tx.clone();
            threads.push(thread::spawn(move || {
                let observer = Observer::new(&thread_stat, &conn);
                let result = observer.start_worker(tx, 1024);
                print_thread_result(&result, &thread_stat);
            }));
        }

        // create receiver threads
        for conn in receiver_pg_conns {
            let thread_stat = thread_stat.clone();
            let rx = rcv_rx.clone();
            let tx = str_tx.clone();
            threads.push(thread::spawn(move || {
                let receiver = Receiver::new(&thread_stat, &conn);

                // Buffer object larger than 1 MiB in a temporary file rather than in memory.
                let threshold = 1024_i64.pow(3);

                let result = receiver.start_worker(rx, tx, threshold);
                print_thread_result(&result, &thread_stat);
            }));
        }

        // create storer threads
        for conn in storer_s3_conns {
            let thread_stat = thread_stat.clone();
            let rx = str_rx.clone();
            let tx = cmt_tx.clone();
            let bucket_name = s3_bucket_name.to_string();
            threads.push(thread::spawn(move || {
                let storer = Storer::new(&thread_stat);
                let result = storer.start_worker(rx, tx, &conn, &bucket_name);
                print_thread_result(&result, &thread_stat);
            }));
        }

        // create committer thread
        for conn in committer_pg_conns {
            let thread_stat = thread_stat.clone();
            let rx = cmt_rx.clone();
            threads.push(thread::spawn(move || {
                let committer = Committer::new(&thread_stat, &conn);
                let result = committer.start_worker(rx, 100 /* commit 100 Lo at a time */);
                print_thread_result(&result, &thread_stat);
            }));
        }

        // create monitor thread

        // `Weak` references needed here because the other threads terminate when all `Sender`s
        // or `Receiver`s were terminated (hang-up queue). If we keep a strong reference the other
        // worker threads won't ever terminate.
        let rcv_rx_weak = Arc::downgrade(&rcv_rx);
        let str_rx_weak = Arc::downgrade(&str_rx);
        let cmt_rx_weak = Arc::downgrade(&cmt_rx);

        threads.push(thread::spawn(move || {
            let monitor = Monitor {
                stats: &thread_stat,
                receive_queue: rcv_rx_weak,
                receive_queue_size: receive_queue_size,
                store_queue: str_rx_weak,
                store_queue_size: store_queue_size,
                commit_queue: cmt_rx_weak,
                commit_queue_size: commit_queue_size,
            };
            monitor.start_worker(Duration::from_secs(10));
        }));

        // `Arc<_>` for the queues are dropped here!
    }

    for thread in threads {
        thread.join().unwrap();
    }
}
