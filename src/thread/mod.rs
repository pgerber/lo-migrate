//! Implementation of the worker threads
//!
//! How [`Lo`] travel:
//! 1. List of large object is read from Postgres by single thread. New [`Lo`] are created
//!    and feed to [`lo_receive_tx`].
//! 2. Multiple threads read [`Lo`] from [`lo_receive_rx`] and fetch the corresponding
//!    Large Object, storing it in memory or a temporary file. While reading, the Object's
//!    sha2 hash is generated. Once done, the [`Lo`]s are feed to [`lo_store_tx`].
//! 3. Multiple threads read [`Lo`]s from [`lo_store_rx`] and an S3 object with `Lo`'s data is
//!    created. Temporary files and memory used to buffer data is dropped afterwards. [`Lo`] are
//!    then pushed to [`lo_commit_tx`].
//! 4. One thread reads [`Lo`]s from [`lo_commit_rx`] and the sha2 hash is written to the database.
//!    Multiple hashes are committed in one transaction.

pub mod commit;
pub use self::commit::Committer;

pub mod monitor;
pub use self::monitor::Monitor;

pub mod observe;
pub use self::observe::Observer;

pub mod receive;
pub use self::receive::Receiver;

pub mod store;
pub use self::store::Storer;

use common::{MigrationError, Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::atomic::Ordering;
use lo::Lo;
use spin;
use postgres;

/// Thread stats and queue shared amongst all threads
#[derive(Clone)]
pub struct ThreadStat {
    /// Used to implement cancellation points within the threads
    cancelled: Arc<AtomicBool>,

    /// Total number of large object
    ///
    /// Number of entries in _nice_binary if already known.
    pub lo_total: Arc<spin::Mutex<Option<u64>>>,

    /// Number of Large Object observed
    ///
    /// This is the number of binary entries extracted from Postgres. This
    /// does not imply that the object has been read yet.
    pub lo_observed: Arc<AtomicU64>,

    /// Number of Large Objects read
    ///
    /// This is the number of Large Object received
    /// from Postgres and buffered locally
    pub lo_received: Arc<AtomicU64>,

    /// Number of Large Object written to S3
    pub lo_stored: Arc<AtomicU64>,

    /// Number of sha2 hashes commit to Postgres
    pub lo_committed: Arc<AtomicU64>,

    /// Number of Large Object that could not be copied successfully
    ///
    /// Count of any object that could not be received, storred or whose hash
    /// could not be commit to the database.
    pub lo_failed: Arc<AtomicU64>,
}

impl ThreadStat {
    /// Create new `ThreadStat` object
    ///
    /// `receive`, `store` and `commit` parameter are the size, number of [`Lo`]s, of the
    /// corresponding queue.
    pub fn new() -> Self {
        ThreadStat {
            cancelled: Arc::new(AtomicBool::new(false)),
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
