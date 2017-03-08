//! receive thread implementation
//!
//! The receiver thread receives [`Lo`]s from the observer thread via queue. Then it retrieves the
//! Large Object and store them in memory or as temporary file, depending on size. Once this is done
//! it pushes the [`Lo`] via queue to the storer thread.

use postgres::Connection;
use error::Result;
use two_lock_queue;
use std::sync::Arc;
use super::*;

pub struct Receiver<'a> {
    stats: &'a ThreadStat,
    conn: &'a Connection,
}

impl<'a> Receiver<'a> {
    pub fn new(thread_stat: &'a ThreadStat, conn: &'a postgres::Connection) -> Self {
        Receiver {
            stats: thread_stat,
            conn: conn,
        }
    }

    pub fn start_worker(&self,
                        rx: Arc<two_lock_queue::Receiver<Lo>>,
                        tx: Arc<two_lock_queue::Sender<Lo>>,
                        size_threshold: i64)
                        -> Result<()> {
        while let Ok(mut lo) = rx.recv() {
            debug!("processing large object: {:?}", lo);

            // receive from observer thread
            lo.retrieve_lo_data(self.conn, size_threshold)?;

            // global counter of received objects
            self.stats.lo_received.fetch_add(1, Ordering::Relaxed);

            // pass on `Lo` to storer thread
            tx.send(lo)?;

            // thread cancellation point
            self.stats.cancellation_point()?;
        }

        info!("thread has completed its mission, rx queue hang up");
        Ok(())
    }
}
