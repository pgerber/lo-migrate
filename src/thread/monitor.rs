//! Thread that shows stats to the user

use lo::Lo;
use std::sync::atomic::Ordering;
use std::thread;
use std::sync::Weak;
use std::time::{Duration, Instant};
use thread::ThreadStat;
use two_lock_queue::Receiver;

pub struct Monitor<'a> {
    pub stats: &'a ThreadStat,
    pub receive_queue: Weak<Receiver<Lo>>,
    pub receive_queue_size: usize,
    pub store_queue: Weak<Receiver<Lo>>,
    pub store_queue_size: usize,
    pub commit_queue: Weak<Receiver<Lo>>,
    pub commit_queue_size: usize,
}

impl<'a> Monitor<'a> {
    #[cfg_attr(feature = "clippy", allow(print_stdout))]
    pub fn start_worker(&self, interval: Duration) {
        let cancel_interval = Duration::from_millis(500);

        'outter: loop {
            let total = match *self.stats.lo_total.lock() {
                Some(v) => format!("{}", v),
                None => "UNKNOWN".to_string(),
            };

            println!("Status: copied {} out of {} objects - observed: {}, \
                      received: {}, stored: {}, committed: {}",
                     self.stats.lo_committed.load(Ordering::Relaxed),
                     total,
                     self.stats.lo_observed.load(Ordering::Relaxed),
                     self.stats.lo_received.load(Ordering::Relaxed),
                     self.stats.lo_stored.load(Ordering::Relaxed),
                     self.stats.lo_committed.load(Ordering::Relaxed));

            info!("queue usage stats: receive queue: {} of {}, store queue: {} of {}, commit \
                  queue: {} of {}",
                  Self::queue_length(&self.receive_queue),
                  self.receive_queue_size,
                  Self::queue_length(&self.store_queue),
                  self.store_queue_size,
                  Self::queue_length(&self.commit_queue),
                  self.commit_queue_size);

            // wait for `interval` but check every `cancel_interval` if thread was cancelled
            let instant = Instant::now();
            while instant.elapsed() < interval {
                thread::sleep(cancel_interval);
                if self.are_all_queues_dropped() {
                    // all queues have been dropped, so no other working thread is still running
                    info!("all queues have seized to exist, nothing left to monitor, terminating \
                           thread");
                    break 'outter;
                }
            }

        }
    }

    fn queue_length(queue: &Weak<Receiver<Lo>>) -> String {
        match queue.upgrade() {
            Some(v) => format!("{}", v.len()),
            None => "DROPPED".to_string(),
        }
    }

    fn are_all_queues_dropped(&self) -> bool {
        self.receive_queue.upgrade().is_none() && self.store_queue.upgrade().is_none() &&
        self.commit_queue.upgrade().is_none()
    }
}
