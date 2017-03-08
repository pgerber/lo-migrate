use error::Result;
use hyper::client::Client;
use s3::s3client::S3Client;
use aws::common::credentials::AwsCredentialsProvider;
use std::sync::Arc;
use thread::ThreadStat;
use two_lock_queue::{Receiver, Sender};
use super::*;

pub struct Storer<'a> {
    stats: &'a ThreadStat,
}

impl<'a> Storer<'a> {
    pub fn new(thread_stat: &'a ThreadStat) -> Self {
        Storer { stats: thread_stat }
    }

    pub fn start_worker<P>(&self,
                           rx: Arc<Receiver<Lo>>,
                           tx: Arc<Sender<Lo>>,
                           client: &S3Client<P, Client>,
                           bucket: &str)
                           -> Result<()>
        where P: AwsCredentialsProvider
    {
        while let Ok(mut lo) = rx.recv() {
            debug!("processing large object: {:?}", lo);

            // receive from storer thread
            lo.store(client, bucket)?;

            // global counter of stored objects
            self.stats.lo_stored.fetch_add(1, Ordering::Relaxed);

            // forward `Lo`s to committer thread
            tx.send(lo)?;

            // thread cancellation point
            self.stats.cancellation_point()?;
        }

        info!("thread has completed its mission, rx queue hang up");
        Ok(())
    }
}
