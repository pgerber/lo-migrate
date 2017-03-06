//! Implementation of the worker threads
//!
//! How [`Lo`] travel:
//! 1. List of large object is read from Postgres by single thread. New [`Lo`] are created
//!    and feed to [`lo_receive_tx`].
//! 2. Multiple threads read [`Lo`] from [`lo_receive_rx`] and fetch the corresponding 
//!    Large Object, storing it in memory or a temporary file. While reading, the Object's
//!    sha2 hash is generated. Once done, the [`Lo`]s are feed to [`lo_store_tx`].
//! 3. Multiple threads read [`Lo`]s from [`lo_store_rx`] and an S3 object with `Lo`'s data is
//!    created. Temporary files and memory used to buffer data is dropped afterwards. [`Lo`] are then
//!    pushed to [`lo_commit_tx`].
//! 4. One thread reads [`Lo`]s from [`lo_commit_rx`] and the sha2 hash is written to the database.
//!    Multiple hashes are committed in one transaction.

use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::atomic::Ordering;
use two_lock_queue as queue;
use self::queue::{Receiver, Sender};
use lo::Lo;

/// Thread stats and queue shared amongst all threads
struct ThreadStat {
    /// Used to implement cancellation points within the threads
    cancelled: AtomicBool,

    /// Number of Large Object observed
    ///
    /// This is the number of binary entries extracted from Postgres. This
    /// does not imply that the object has been read yet.
    pub lo_observed: AtomicU64,

    /// Number of Large Objects read
    ///
    /// This is the number of Large Object received
    /// from Postgres and buffered locally
    pub lo_received: AtomicU64,

    /// Number of Large Object written to S3
    pub lo_stored: AtomicU64,

    /// Number of sha2 hashes commit to Postgres
    pub lo_committed: AtomicU64,

    /// Number of Large Object that could not be copied successfully
    ///
    /// Count of any object that could not be received, storred or whose hash
    /// could not be commit to the database.
    pub lo_failed: AtomicU64,

    /// Sender side of the queue sending [`Lo`]s to the receiver threads
    pub lo_receive_tx: Sender<Lo>,

    /// Receiver side of the queue sending [`Lo`]s to the receiver threads
    pub lo_receive_rx: Receiver<Lo>,

    /// Sender side of the queue sending [`Lo`]s to the storer threads
    pub lo_store_tx: Sender<Lo>,

    /// Receiver side of the queue sending [`Lo`]s to the store threads
    pub lo_store_rx: Receiver<Lo>,

    /// Sender side of the queue sending [`Lo`]s to the committer thread
    pub lo_commit_tx: Sender<Lo>,

    /// Receiver side of the queue sending [`Lo`]s to the committer thread
    pub lo_commit_rx: Receiver<Lo>
}

impl ThreadStat {
    /// Create new `ThreadStat` object
    ///
    /// `receive`, `store` and `commit` parameter are the size, number of [`Lo`]s, of the
    /// corresponding queue.
    pub fn new(receive: usize, store: usize, commit: usize) -> Self {
        let (rcv_tx, rcv_rx) = queue::channel(receive);
        let (str_tx, str_rx) = queue::channel(store);
        let (cmt_tx, cmt_rx) = queue::channel(commit);
        ThreadStat {
            cancelled: AtomicBool::new(false),
            lo_observed: AtomicU64::new(0),
            lo_received: AtomicU64::new(0),
            lo_stored: AtomicU64::new(0),
            lo_committed: AtomicU64::new(0),
            lo_failed: AtomicU64::new(0),
            lo_receive_tx: rcv_tx,
            lo_receive_rx: rcv_rx,
            lo_store_tx: str_tx,
            lo_store_rx: str_rx,
            lo_commit_tx: cmt_tx,
            lo_commit_rx: cmt_rx
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
}
