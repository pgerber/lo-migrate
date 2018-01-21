#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]
#![feature(box_patterns)]

extern crate clap;
extern crate env_logger;
extern crate hyper;
extern crate hyper_rustls;
extern crate lo_migrate;
extern crate log;
extern crate postgres;
extern crate rusoto_core;
extern crate rusoto_credential;
extern crate rusoto_s3;
extern crate sha2;
extern crate two_lock_queue;

use postgres::{Connection, TlsMode};
use postgres::error::Error as PgError;
use postgres::error::SqlState;
use log::LogLevelFilter;
use env_logger::LogBuilder;
use hyper::client::{self, Client, RedirectPolicy};
use hyper::net::HttpsConnector;
use lo_migrate::thread::{Committer, Counter, Monitor, Observer, Receiver, Storer, ThreadStat};
use rusoto_core::region::Region;
use rusoto_credential::StaticProvider;
use rusoto_s3::S3Client;
use sha2::Sha256;
use std::{env, fmt, process, thread};
use std::str::FromStr;
use std::sync::Arc;
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
    upload_chunk_size: usize,
    commit_chunk_size: usize,
    monitor_interval: u64,
    finalize: bool,
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
                .help("Url to connect to postgres (postgresql://USER:PASS@HOST/DB_NAME)")
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
                .value_name("SIZE")
                .help("Max. size of an object kept in memory (in kiB). Larger objects are \
                       written to a buffer file. Keep in mind that all objects in the storer queue \
                       use up to this much memory."))
            .arg(Arg::with_name("upload_chunk_size")
                .long("upload-chunk-size")
                .value_name("SIZE")
                 .help("Size, in kiB, of one part when using multipart upload. Beware that even \
                        when using file-based buffers, SIZE kIB are held in memory by every storer \
                        thread. Also, multipart upload is only enabled for object buffered in \
                        files. All other objects are already in memory anyway."))
            .arg(Arg::with_name("commit_chunk_size")
                .long("commit-chunk")
                .value_name("INT")
                .help("Number of SHA2 hashes committed per DB transaction"))
            .arg(Arg::with_name("monitor_interval")
                .short("i")
                .long("interval")
                .value_name("SECS")
                .help("Interval in which stats are shown (in secs)"))
            .arg(Arg::with_name("finalize")
                .short("f")
                .long("finalize")
                .help("Create UNIQUE INDEX and NOT NULL constraint"))
            .get_matches();

        Args {
            s3_url: matches.value_of("s3_url").unwrap().to_string(),
            s3_access_key: matches.value_of("s3_access_key").unwrap().to_string(),
            s3_secret_key: matches.value_of("s3_secret_key").unwrap().to_string(),
            s3_bucket_name: matches.value_of("s3_bucket_name").unwrap().to_string(),
            postgres_url: matches.value_of("postgres_url").unwrap().to_string(),
            receiver_threads: Self::expect_greater_zero(matches.value_of("receiver_threads"),
                                                        2,
                                                        "receiver thread count invalid"),
            storer_threads: Self::expect_greater_zero(matches.value_of("storer_threads"),
                                                      5,
                                                      "storer thread count invalid"),
            committer_threads: Self::expect_greater_zero(matches.value_of("committer_threads"),
                                                         2,
                                                         "receiver committer count invalid"),
            receiver_queue: Self::expect_greater_zero(matches.value_of("receiver_queue"),
                                                      8192,
                                                      "receiver queue size invalid"),
            storer_queue: Self::expect_greater_zero(matches.value_of("storer_queue"),
                                                    1024,
                                                    "storer queue size invalid"),
            committer_queue: Self::expect_greater_zero(matches.value_of("committer_queue"),
                                                       8192,
                                                       "committer queue size invalid"),
            max_in_memory: matches.value_of("max_in_memory")
                .map_or(1024,
                        |i| i64::from_str(i).expect("maximum in-memory size invalid")) *
                           1024,
            upload_chunk_size: matches.value_of("upload_chunk_size")
                .map_or(20_971_520, // 20 MiB
                        |i| {
                            let v = usize::from_str(i).expect("upload chunk size invalid") * 1024;
                            assert!(v >= 5_242_880,
                                    "upload chunk size must be at least 5 MiB but is only {}", v);
                            v
                        }),
            commit_chunk_size: Self::expect_greater_zero(matches.value_of("commit_chunk_size"),
                                                         100,
                                                         "commit check size invalid"),
            monitor_interval: Self::expect_greater_zero(matches.value_of("monitor_interval"),
                                                        10,
                                                        "monitor interval invalid"),
            finalize: matches.is_present("finalize"),
        }
    }

    fn expect_greater_zero<T>(string: Option<&str>, default: T, msg: &str) -> T
        where T: FromStr + PartialEq<T> + PartialOrd<T> + From<u8>,
              <T as std::str::FromStr>::Err: std::fmt::Debug
    {
        if let Some(string) = string {
            let value = FromStr::from_str(string).expect(&format!("{}: found {:?}", msg, string));
            if value <= From::from(0) {
                panic!(format!("{}: found {:?}", msg, string));
            }
            value
        } else {
            default
        }
    }
}

impl fmt::Display for Args {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "**************** configuration ****************")?;
        writeln!(f, "  threads:")?;
        writeln!(f, "    receiver threads:  {:4}", self.receiver_threads)?;
        writeln!(f, "    storer threads:    {:4}", self.storer_threads)?;
        writeln!(f, "    committer threads: {:4}", self.committer_threads)?;
        writeln!(f, "  queues")?;
        writeln!(f, "    receiver queue size: {:6} objects", self.receiver_queue)?;
        writeln!(f, "    storer queue size:   {:6} objects", self.storer_queue)?;
        writeln!(f, "    committer threads:   {:6} objects", self.committer_queue)?;
        writeln!(f, "  other:")?;
        writeln!(f, "    max. in-memory size: {} KiB", self.max_in_memory / 1024)?;
        writeln!(f, "    multipart upload part size: {} kiB", self.upload_chunk_size / 1024)?;
        writeln!(f, "    DB commit chunk size: {}", self.commit_chunk_size)
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
                 region: &Region,
                 count: usize)
                 -> Vec<S3Client<StaticProvider, Client>> {
    let mut conns = Vec::with_capacity(count);
    for _ in 0..count {
        let credentials = StaticProvider::new_minimal(access_key.to_owned(), secret_key.to_owned());
        let tls = hyper_rustls::TlsClient::new();
        let connector = HttpsConnector::new(tls);
        let pool = client::pool::Pool::with_connector(client::pool::Config { max_idle: 1 },
                                                      connector);
        let mut client = Client::with_connector(pool);
        client.set_redirect_policy(RedirectPolicy::FollowNone);
        conns.push(S3Client::new(client, credentials, region.clone()));
    }
    conns
}

fn handle_thread_error(error: &MigrationError, thread_name: &str) {
    match *error {
        MigrationError::ThreadCancelled |
        MigrationError::SendError(_) => (),
        ref err => {
            panic!("ERROR: thread {}: {:?}", thread_name, err);
        }
    };
}

fn add_sha2_column(pg_client: &Connection) -> Result<(), PgError> {
    match pg_client.batch_execute("ALTER TABLE _nice_binary ADD COLUMN sha2 CHAR(64)") {
        Err(PgError::Db(box ref db_err)) if db_err.code == SqlState::DuplicateColumn => {
            Ok(()) // ignore existing "sha2" column
        }
        r => r,
    }
}

fn add_constraints(pg_client: &Connection) -> Result<(), PgError> {
    pg_client.batch_execute("ALTER TABLE _nice_binary ALTER COLUMN sha2 set NOT NULL; \
    CREATE UNIQUE INDEX IF NOT EXISTS _nice_binary_sha2_key on _nice_binary (sha2);")
}

fn main() {
    type TargetDigest = Sha256;

    let mut log_builder = LogBuilder::new();
    log_builder.filter(Some("lo_migrate"), LogLevelFilter::Warn);
    if let Ok(env) = env::var("RUST_LOG") {
        log_builder.parse(&env);
    }
    log_builder.init().unwrap();

    let args = Args::new_from_env();
    println!("{}", args);


    let s3_region = Region::Custom { name: "eu-east-3".to_owned(), endpoint: args.s3_url.to_owned() };

    let observer_pg_conns = connect_to_postgres(&args.postgres_url,
                                                1 /* multiple threads not supported */);
    let receiver_pg_conns = connect_to_postgres(&args.postgres_url, args.receiver_threads);
    let storer_s3_conns = connect_to_s3(&args.s3_access_key,
                                        &args.s3_secret_key,
                                        &s3_region,
                                        args.storer_threads);
    let committer_pg_conns = connect_to_postgres(&args.postgres_url, args.committer_threads);
    let counter_pg_conns = connect_to_postgres(&args.postgres_url,
                                               1 /* multiple threads not supported */);

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

    // create sha2 column
    let conn = observer_pg_conns.into_iter().next().unwrap();
    add_sha2_column(&conn).expect("failed to add \"sha2\" column");
    lo_migrate::utils::check_batch_job_is_disabled(&conn).expect("check failed");

    // create observer thread
    {
        let thread_stat = thread_stat.clone();
        let tx = Arc::clone(&rcv_tx);
        threads.push(thread::Builder::new()
            .name("observer".to_string())
            .spawn(move || {
                let observer = Observer::new(&thread_stat, &conn);
                let result = observer.start_worker(tx, 1024);
                if let Err(e) = result {
                    handle_thread_error(&e, "observer");
                };
            })
            .unwrap());
    }

    // create receiver threads
    for (no, conn) in receiver_pg_conns.into_iter().enumerate() {
        let thread_stat = thread_stat.clone();
        let rx = Arc::clone(&rcv_rx);
        let tx = Arc::clone(&str_tx);
        let max_in_memory = args.max_in_memory;
        let name = format!("receiver_{}", no);
        threads.push(thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                let receiver = Receiver::new(&thread_stat, &conn);
                let result = receiver.start_worker::<TargetDigest>(rx, tx, max_in_memory);
                if let Err(e) = result {
                    handle_thread_error(&e, &name);
                };
            })
            .unwrap());
    }

    // create storer threads
    for (no, conn) in storer_s3_conns.into_iter().enumerate() {
        let thread_stat = thread_stat.clone();
        let rx = Arc::clone(&str_rx);
        let tx = Arc::clone(&cmt_tx);
        let bucket_name = args.s3_bucket_name.to_string();
        let name = format!("storer_{}", no);
        let upload_chunk_size = args.upload_chunk_size;
        threads.push(thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                let storer = Storer::new(&thread_stat, upload_chunk_size);
                let result = storer.start_worker(rx, tx, &conn, &bucket_name);
                if let Err(e) = result {
                    handle_thread_error(&e, &name);
                };
            })
            .unwrap());
    }

    // create committer thread
    for (no, conn) in committer_pg_conns.into_iter().enumerate() {
        let thread_stat = thread_stat.clone();
        let rx = Arc::clone(&cmt_rx);
        let commit_chunk_size = args.commit_chunk_size;
        let name = format!("committer_{}", no);
        threads.push(thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                let committer = Committer::new(&thread_stat, &conn);
                let result = committer.start_worker(rx, commit_chunk_size);
                if let Err(e) = result {
                    handle_thread_error(&e, &name);
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
        let receiver_queue = args.receiver_queue;
        let storer_queue = args.storer_queue;
        let committer_queue = args.committer_queue;

        threads.push(thread::Builder::new()
            .name("monitor".to_string())
            .spawn(move || {
                let monitor = Monitor {
                    stats: &thread_stat,
                    receive_queue: rcv_rx_weak,
                    receive_queue_size: receiver_queue,
                    store_queue: str_rx_weak,
                    store_queue_size: storer_queue,
                    commit_queue: cmt_rx_weak,
                    commit_queue_size: committer_queue,
                };
                monitor.start_worker(Duration::from_secs(monitor_interval));
            })
            .unwrap());
    }

    // create counter thread
    {
        let thread_stat = thread_stat.clone();
        let conn = counter_pg_conns.into_iter().next().unwrap();
        thread::Builder::new()
            .name("counter".to_string())
            .spawn(move || {
                let counter = Counter::new(&thread_stat, &conn);
                counter.start_worker().unwrap();
            })
            .unwrap();
    }

    let mut failure_count = 0;
    for thread in threads {
        let name = thread.thread().name().unwrap_or("UNNAMED").to_string();

        if let Err(ref e) = thread.join() {
            failure_count += 1;
            if let Some(e) = e.downcast_ref::<String>() {
                println!("ERROR: Thread {} panicked: {}", name, e);
            } else if let Some(e) = e.downcast_ref::<&'static str>() {
                println!("ERROR: Thread {} panicked: {}", name, e);
            } else {
                println!("ERROR: Thread {} panicked: {:?}", name, e);
            }
        };
    }

    if failure_count > 0 {
        println!();
        println!("ERROR: At least one thread reported a failure, you must rerun the migration to \
                  ensure all binary are transferred to S3");
        process::exit(1);
    }
    if thread_stat.lo_failed() > 0 {
        println!();
        println!("ERROR: At least one object failed to be migrated. Rerun the migration and check /
                  for errors.");
        process::exit(1);
    }
    print!("Adding NOT NULL constraint and UNIQUE INDEX ... ");
    if args.finalize {
        add_constraints(&connect_to_postgres(&args.postgres_url, 1)
                .into_iter()
                .next()
                .unwrap())
            .unwrap();
        println!("done");
    } else {
        println!("skipping (--finalize not given)");
    }
    println!("Migration completed");
}
