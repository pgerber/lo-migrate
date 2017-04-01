//! Implementation of the worker threads

mod commit;
pub use self::commit::Committer;

mod count;
pub use self::count::Counter;

mod monitor;
pub use self::monitor::Monitor;

mod observe;
pub use self::observe::Observer;

mod receive;
pub use self::receive::Receiver;

mod store;
pub use self::store::Storer;

use error::{MigrationError, Result};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::atomic::Ordering;
use lo::Lo;
use postgres;

/// Thread stats shared amongst all threads
#[derive(Clone)]
pub struct ThreadStat {
    /// Used to implement cancellation points within the threads
    cancelled: Arc<AtomicBool>,

    /// Number of large object that need to be migrated
    ///
    /// Number of entries in _nice_binary that need to be migrated if already known.
    lo_remaining: Arc<Mutex<Option<u64>>>,

    /// total_number of large objects
    ///
    /// Number of large object incl. objects already migrated.
    lo_total: Arc<Mutex<Option<u64>>>,

    /// Number of Large Object observed
    ///
    /// This is the number of binary entries extracted from Postgres. This does not imply that the
    /// object has been read yet.
    lo_observed: Arc<AtomicU64>,

    /// Number of Large Objects read
    ///
    /// This is the number of Large Object received from Postgres
    lo_received: Arc<AtomicU64>,

    /// Number of Large Object written to S3
    lo_stored: Arc<AtomicU64>,

    /// Number of sha2 hashes commit to Postgres
    lo_committed: Arc<AtomicU64>,

    /// Number of Large Object that could not be copied successfully
    ///
    /// Count of any object that could not be received, stored or whose hash
    /// could not be commit to the database.
    lo_failed: Arc<AtomicU64>,
}

impl ThreadStat {
    /// Create new `ThreadStat` object
    ///
    /// `receive`, `store` and `commit` parameter are the size, number of [`Lo`]s, of the
    /// corresponding queue.
    pub fn new() -> Self {
        ThreadStat {
            cancelled: Arc::new(AtomicBool::new(false)),
            lo_remaining: Arc::new(Mutex::new(None)),
            lo_total: Arc::new(Mutex::new(None)),
            lo_observed: Arc::new(AtomicU64::new(0)),
            lo_received: Arc::new(AtomicU64::new(0)),
            lo_stored: Arc::new(AtomicU64::new(0)),
            lo_committed: Arc::new(AtomicU64::new(0)),
            lo_failed: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn lo_remaining(&self) -> Option<u64> {
        *self.lo_remaining.lock().expect("failed to aquire lock")
    }

    pub fn lo_total(&self) -> Option<u64> {
        *self.lo_total.lock().expect("failed to aquire lock")
    }

    pub fn lo_observed(&self) -> u64 {
        self.lo_observed.load(Ordering::Relaxed)
    }

    pub fn lo_received(&self) -> u64 {
        self.lo_received.load(Ordering::Relaxed)
    }

    pub fn lo_stored(&self) -> u64 {
        self.lo_stored.load(Ordering::Relaxed)
    }

    pub fn lo_committed(&self) -> u64 {
        self.lo_committed.load(Ordering::Relaxed)
    }

    /// True if threads have been cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Tell threads to cancel at earliest convenience
    pub fn cancel(&self) {
        debug!("canceling threads");
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Cancellation point for threads
    ///
    /// Returns `Err(MigrationError::ThreadCancelled)` if thread should be cancelled and Ok(())
    /// otherwise.
    pub fn cancellation_point(&self) -> Result<()> {
        if self.is_cancelled() {
            debug!("cancellation point: threads have been cancelled");
            Err(MigrationError::ThreadCancelled)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_point() {
        let stat = ThreadStat::new();
        assert!(stat.cancellation_point().is_ok());
        stat.cancel();
        match stat.cancellation_point().unwrap_err() {
            MigrationError::ThreadCancelled => (),
            _ => panic!(),
        }
    }

    #[test]
    fn is_cancelled() {
        let stat = ThreadStat::new();
        assert!(!stat.is_cancelled());
        stat.cancel();
        assert!(stat.is_cancelled());
    }

    #[test]
    fn clone() {
        let stat1 = ThreadStat::new();
        let stat2 = stat1.clone();

        stat2.cancel();
        assert!(stat1.is_cancelled());

        stat1.lo_observed.fetch_add(252, Ordering::Relaxed);
        assert_eq!(stat2.lo_observed(), 252);

        stat2.lo_received.fetch_add(2, Ordering::Relaxed);
        assert_eq!(stat1.lo_received(), 2);

        stat1.lo_stored.fetch_add(159, Ordering::Relaxed);
        assert_eq!(stat2.lo_stored(), 159);

        stat2.lo_committed.fetch_add(2, Ordering::Relaxed);
        assert_eq!(stat1.lo_committed(), 2);

        *stat2.lo_remaining.lock().unwrap() = Some(12);
        assert_eq!(stat1.lo_remaining(), Some(12));

        *stat1.lo_total.lock().unwrap() = Some(66);
        assert_eq!(stat2.lo_total(), Some(66));
    }
}
