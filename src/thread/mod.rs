//! Implementation of the worker threads

mod commit;
pub use self::commit::Committer;

mod monitor;
pub use self::monitor::Monitor;

mod observe;
pub use self::observe::Observer;

mod receive;
pub use self::receive::Receiver;

mod store;
pub use self::store::Storer;

use error::{MigrationError, Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::atomic::Ordering;
use std::time::Instant;
use lo::Lo;
use spin;
use postgres;

/// Thread stats shared amongst all threads
#[derive(Clone)]
pub struct ThreadStat {
    /// Used to implement cancellation points within the threads
    cancelled: Arc<AtomicBool>,

    /// Start instant
    ///
    /// Contains the time when transfering was started. Set by the observer thread.
    start: Arc<spin::Mutex<Option<Instant>>>,

    /// Total number of large object
    ///
    /// Number of entries in _nice_binary that need to be migrated if already known.
    lo_total: Arc<spin::Mutex<Option<u64>>>,

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
    /// Count of any object that could not be received, storred or whose hash
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
            start: Arc::new(spin::Mutex::new(None)),
            lo_total: Arc::new(spin::Mutex::new(None)),
            lo_observed: Arc::new(AtomicU64::new(0)),
            lo_received: Arc::new(AtomicU64::new(0)),
            lo_stored: Arc::new(AtomicU64::new(0)),
            lo_committed: Arc::new(AtomicU64::new(0)),
            lo_failed: Arc::new(AtomicU64::new(0)),
        }
    }

    /// True if threads have been cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Tell threads to cancel at earliest convenience
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Cancellation point for threads
    ///
    /// Returns `Err(MigrationError::ThreadCancelled)` if thread should be cancelled and Ok(())
    /// otherwise.
    pub fn cancellation_point(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(MigrationError::ThreadCancelled)
        } else {
            Ok(())
        }
    }
}
