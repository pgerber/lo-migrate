#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

extern crate clap;
extern crate lo_migrate;
extern crate postgres;
extern crate sha2;
extern crate env_logger;
extern crate aws_sdk_rust;
extern crate url;
extern crate hyper;
extern crate two_lock_queue;
extern crate hyper_rustls;

use postgres::{Connection, TlsMode};
use url::Url;
use aws_sdk_rust::aws::s3::s3client::S3Client;
use aws_sdk_rust::aws::s3::endpoint::Endpoint;
use aws_sdk_rust::aws::s3::endpoint::Signature;
use aws_sdk_rust::aws::common::region::Region;
use aws_sdk_rust::aws::common::credentials::ParametersProvider;
use hyper::client::{self, Client, RedirectPolicy};
use hyper::net::HttpsConnector;
use lo_migrate::thread::{Committer, Monitor, Observer, Receiver, Storer, ThreadStat};
use sha2::Sha256;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use lo_migrate::error::MigrationError;

#[derive(Debug)]
struct Args {
    s3_url: String,
    s3_access_key: String,
    s3_secret_key: String,
    s3_bucket_name: String,
    postgres_url: String,
    receiver_threads: usize,
    storer_threads: usize,
    committer_threads: usize,
    receiver_queue: usize,
    storer_queue: usize,
    committer_queue: usize,
    max_in_memory: i64,
    commit_chunk_size: usize,
    monitor_interval: u64,
}

impl Args {
    fn new_from_env() -> Self {
        use clap::*;

        let matches = App::new("Postgres Large Object to S3 Migrator")
            .arg(Arg::with_name("s3_url")
                .short("u")
                .long("s3-url")
                .value_name("URL")
                .help("URL to S3 endpoint")
                .required(true))
            .arg(Arg::with_name("s3_access_key")
                .short("k")
                .long("access-key")
                .value_name("KEY")
                .help("S3 access key")
                .required(true))
            .arg(Arg::with_name("s3_secret_key")
                .short("s")
                .long("secret-key")
                .value_name("KEY")
                .help("S3 secret key")
                .required(true))
            .arg(Arg::with_name("s3_bucket_name")
                .short("b")
                .long("bucket")
                .value_name("NAME")
                .help("Name of the S3 bucket")
                .required(true))
            .arg(Arg::with_name("postgres_url")
                .short("p")
                .long("pg-url")
                .value_name("URL")
                .help("Url to connect to postgres (USER:PASS@HOST/DB_NAME)")
                .required(true))
            .arg(Arg::with_name("receiver_threads")
                .long("receiver-threads")
                .value_name("INT")
                .help("Number of receiver threads"))
            .arg(Arg::with_name("storer_threads")
                .long("storer-threads")
                .value_name("INT")
                .help("Number of storer threads"))
            .arg(Arg::with_name("committer_threads")
                .long("committer-threads")
                .value_name("INT")
                .help("Number of committer threads"))
            .arg(Arg::with_name("receiver_queue")
                .long("receiver-queue")
                .value_name("INT")
                .help("Size of the receiver queue"))
            .arg(Arg::with_name("storer_queue")
                .long("storer-queue")
                .value_name("INT")
                .help("Size of the storer queue"))
            .arg(Arg::with_name("committer_queue")
                .long("committer-queue")
                .value_name("INT")
                .help("Size of the committer queue"))
            .arg(Arg::with_name("max_in_memory")
                .long("in-mem-max")
                .value_name("INT")
                .help("Max. size of Large Object to keep in memory (in KiB)"))
            .arg(Arg::with_name("commit_chunk_size")
                .long("commit-chunk")
                .value_name("INT")
                .help("Number of SHA2 hashes commited per DB transaction"))
            .arg(Arg::with_name("monitor_interval")
                .short("i")
                .long("interval")
                .value_name("SECS")
                .help("Interval in which stats are shown (in secs)"))
            .get_matches();

        Args {
            s3_url: matches.value_of("s3_url").unwrap().to_string(),
            s3_access_key: matches.value_of("s3_access_key").unwrap().to_string(),
            s3_secret_key: matches.value_of("s3_secret_key").unwrap().to_string(),
            s3_bucket_name: matches.value_of("s3_bucket_name").unwrap().to_string(),
            postgres_url: matches.value_of("postgres_url").unwrap().to_string(),
            receiver_threads: matches.value_of("receiver_threads")
                .map_or(2,
                        |i| usize::from_str(i).expect("receiver thread count invalid")),
            storer_threads: matches.value_of("storer_threads")
                .map_or(5,
                        |i| usize::from_str(i).expect("storer thread count invalid")),
            committer_threads: matches.value_of("committer_threads")
                .map_or(2,
                        |i| usize::from_str(i).expect("receiver committer count invalid")),
            receiver_queue: matches.value_of("receiver_queue")
                .map_or(8192,
                        |i| usize::from_str(i).expect("receiver queue size invalid")),
            storer_queue: matches.value_of("storer_queue")
                .map_or(1024,
                        |i| usize::from_str(i).expect("storer queue size invalid")),
            committer_queue: matches.value_of("committer_queue")
                .map_or(8192,
                        |i| usize::from_str(i).expect("committer queue size invalid")),
            max_in_memory: matches.value_of("max_in_memory")
                .map_or(1024,
                        |i| i64::from_str(i).expect("maximum in-memory size invalid")) *
                           1024,
            commit_chunk_size: matches.value_of("commit_chunk_size").map_or(100, |i| {
                usize::from_str(i).expect("commit check size invalid")
            }),
            monitor_interval: matches.value_of("monitor_interval")
                .map_or(10, |i| u64::from_str(i).expect("monitor interval invalid")),
        }
    }
}

impl fmt::Display for Args {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "**************** configuration ****************\n")?;
        write!(f, "  threads:\n")?;
        write!(f, "    receiver threads:  {:4}\n", self.receiver_threads)?;
        write!(f, "    storer threads:    {:4}\n", self.storer_threads)?;
        write!(f, "    committer threads: {:4}\n", self.committer_threads)?;
        write!(f, "  queues\n")?;
        write!(f,
               "    receiver queue size: {:6} objects\n",
               self.receiver_queue)?;
        write!(f,
               "    storer queue size:   {:6} objects\n",
               self.storer_queue)?;
        write!(f,
               "    committer threads:   {:6} objects\n",
               self.committer_queue)?;
        write!(f, "  other:\n")?;
        write!(f,
               "    max. in-memory size: {} KiB\n",
               self.max_in_memory / 1024)?;
        write!(f, "    DB commit chunk size: {}\n", self.commit_chunk_size)
    }
}

fn connect_to_postgres(url: &str, count: usize) -> Vec<Connection> {
    let mut conns = Vec::with_capacity(count);
    for _ in 0..count {
        conns.push(Connection::connect(url, TlsMode::None)
            .expect("Failed to connect to Postgres server"));
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
        let credentials = ParametersProvider::with_parameters(access_key, secret_key, None)
            .expect("Cannot connect to S3");
        let tls = hyper_rustls::TlsClient::new();
        let connector = HttpsConnector::new(tls);
        let pool = client::pool::Pool::with_connector(client::pool::Config { max_idle: 1 },
                                                      connector);
        let mut client = Client::with_connector(pool);
        client.set_redirect_policy(RedirectPolicy::FollowNone);
        conns.push(S3Client::with_request_dispatcher(client, credentials, endpoint.clone()));
    }
    conns
}

fn handle_thread_error(error: &MigrationError, thread_name: &str, thread_stat: &ThreadStat) {
    match *error {
        MigrationError::ThreadCancelled |
        MigrationError::SendError(_) => (),
        ref err => {
            panic!("ERROR: thread {}: {:?}", thread_name, err);
        }
    };
}

fn main() {
    type TargetDigest = Sha256;
    env_logger::init().unwrap();
    let args = Args::new_from_env();
    println!("{}", args);

    let s3_endpoint = Endpoint {
        region: Region::ApNortheast1,
        signature: Signature::V4,
        endpoint: Some(Url::parse(&args.s3_url).expect("S3 url invalid")),
        proxy: None,
        user_agent: None,
        is_bucket_virtual: false,
    };

    let observer_pg_conns = connect_to_postgres(&args.postgres_url,
                                                1 /* multiple threads not supperted */);
    let receiver_pg_conns = connect_to_postgres(&args.postgres_url, args.receiver_threads);
    let storer_s3_conns = connect_to_s3(&args.s3_access_key,
                                        &args.s3_secret_key,
                                        &s3_endpoint,
                                        args.storer_threads);
    let committer_pg_conns = connect_to_postgres(&args.postgres_url, args.committer_threads);

    let thread_stat = ThreadStat::new();

    // all threads that have been started
    let mut threads = Vec::new();

    // queue between observer and receiver threads
    let (rcv_tx, rcv_rx) = two_lock_queue::channel(args.receiver_queue);
    let (rcv_tx, rcv_rx) = (Arc::new(rcv_tx), Arc::new(rcv_rx));

    // queue between receiver and storer threads
    let (str_tx, str_rx) = two_lock_queue::channel(args.storer_queue);
    let (str_tx, str_rx) = (Arc::new(str_tx), Arc::new(str_rx));

    // queue between committer thread and committer thread
    let (cmt_tx, cmt_rx) = two_lock_queue::channel(args.committer_queue);
    let (cmt_tx, cmt_rx) = (Arc::new(cmt_tx), Arc::new(cmt_rx));

    // create observer thread
    {
        let conn = observer_pg_conns.into_iter().next().unwrap();
        let thread_stat = thread_stat.clone();
        let tx = rcv_tx.clone();
        threads.push(thread::Builder::new()
            .name("observer".to_string())
            .spawn(move || {
                let observer = Observer::new(&thread_stat, &conn);
                let result = observer.start_worker(tx, 1024);
                if let Err(e) = result {
                    handle_thread_error(&e, "observer", &thread_stat);
                };
            })
            .unwrap());
    }

    // create receiver threads
    for (no, conn) in receiver_pg_conns.into_iter().enumerate() {
        let thread_stat = thread_stat.clone();
        let rx = rcv_rx.clone();
        let tx = str_tx.clone();
        let max_in_memory = args.max_in_memory;
        let name = format!("receiver_{}", no);
        threads.push(thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                let receiver = Receiver::new(&thread_stat, &conn);
                let result = receiver.start_worker::<TargetDigest>(rx, tx, max_in_memory);
                if let Err(e) = result {
                    handle_thread_error(&e, &name, &thread_stat);
                };
            })
            .unwrap());
    }

    // create storer threads
    for (no, conn) in storer_s3_conns.into_iter().enumerate() {
        let thread_stat = thread_stat.clone();
        let rx = str_rx.clone();
        let tx = cmt_tx.clone();
        let bucket_name = args.s3_bucket_name.to_string();
        let name = format!("storer_{}", no);
        threads.push(thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                let storer = Storer::new(&thread_stat);
                let result = storer.start_worker(rx, tx, &conn, &bucket_name);
                if let Err(e) = result {
                    handle_thread_error(&e, &name, &thread_stat);
                };
            })
            .unwrap());
    }

    // create committer thread
    for (no, conn) in committer_pg_conns.into_iter().enumerate() {
        let thread_stat = thread_stat.clone();
        let rx = cmt_rx.clone();
        let commit_chunk_size = args.commit_chunk_size;
        let name = format!("committer_{}", no);
        threads.push(thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                let committer = Committer::new(&thread_stat, &conn);
                let result = committer.start_worker(rx, commit_chunk_size);
                if let Err(e) = result {
                    handle_thread_error(&e, &name, &thread_stat);
                };
            })
            .unwrap());
    }

    // create monitor thread
    {
        // `Weak` references needed here because the other threads terminate when all `Sender`s
        // or `Receiver`s were terminated (hang-up queue). If we keep a strong reference the
        // other worker threads won't ever terminate.
        let rcv_rx_weak = Arc::downgrade(&rcv_rx);
        drop(rcv_rx);
        drop(rcv_tx);
        let str_rx_weak = Arc::downgrade(&str_rx);
        drop(str_rx);
        drop(str_tx);
        let cmt_rx_weak = Arc::downgrade(&cmt_rx);
        drop(cmt_rx);
        drop(cmt_tx);
        let monitor_interval = args.monitor_interval;
        let thread_stat = thread_stat.clone();

        threads.push(thread::Builder::new()
            .name("monitor".to_string())
            .spawn(move || {
                let monitor = Monitor {
                    stats: &thread_stat,
                    receive_queue: rcv_rx_weak,
                    receive_queue_size: args.receiver_queue,
                    store_queue: str_rx_weak,
                    store_queue_size: args.storer_queue,
                    commit_queue: cmt_rx_weak,
                    commit_queue_size: args.committer_queue,
                };
                monitor.start_worker(Duration::from_secs(monitor_interval));
            })
            .unwrap());
    }

    let mut failure_count = 0;
    for thread in threads {
        let name = thread.thread().name().unwrap_or("UNNAMED").to_string();

        if let Err(ref e) = thread.join() {
            failure_count += 1;
            if let Some(e) = e.downcast_ref::<String>() {
                println!("ERROR: Thread {} panicked: {}", name, e);
            } else {
                println!("ERROR: Thread {} panicked: {:?}", name, e);
            }
        };
    }

    if failure_count > 0 {
        println!();
        println!("ERROR: At least one thread reported a failure, you must rerun the migration to \
                  ensure all binary are transfered to S3");
    }
}
