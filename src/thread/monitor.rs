//! Implementation of the Monitor thread
//!
//! The monitor thread show stats to the user

#![cfg_attr(feature = "clippy", allow(float_arithmetic))]
#![cfg_attr(feature = "clippy", allow(cast_precision_loss))]
#![cfg_attr(feature = "clippy", allow(print_stdout))]

use chrono;
use lo::Lo;
use std::sync::atomic::Ordering;
use std::thread;
use std::sync::Weak;
use std::time::{Duration, Instant};
use thread::ThreadStat;
use two_lock_queue::Receiver;

/// Status information
///
/// used to be able to show the difference since last status update
struct Stats {
    instant: Instant,
    difference: Duration,
    duration: Duration,

    // processed `Lo`s
    lo_observed: u64,
    lo_received: u64,
    lo_stored: u64,
    lo_committed: u64,

    // queue status
    lo_received_queue_len: usize,
    lo_stored_queue_len: usize,
    lo_committed_queue_len: usize,
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            instant: Instant::now(),
            difference: Default::default(),
            duration: Default::default(),
            lo_observed: 0,
            lo_received: 0,
            lo_stored: 0,
            lo_committed: 0,
            lo_received_queue_len: 0,
            lo_stored_queue_len: 0,
            lo_committed_queue_len: 0,
        }
    }
}

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
    pub fn start_worker(&self, interval: Duration) {
        let cancel_interval = Duration::from_secs(1);
        let start_instant = Instant::now();
        let mut before: Stats = Default::default();
        let mut total = None;

        loop {
            if total.is_none() {
                // only fetch total once to avoid locking
                total = *self.stats.lo_total.lock();
            }

            let now = Stats {
                instant: Instant::now(),
                difference: before.instant.elapsed(), // time passed since last loop
                duration: start_instant.elapsed(), // time passed since start
                lo_observed: self.stats.lo_observed.load(Ordering::Relaxed),
                lo_received: self.stats.lo_received.load(Ordering::Relaxed),
                lo_stored: self.stats.lo_stored.load(Ordering::Relaxed),
                lo_committed: self.stats.lo_committed.load(Ordering::Relaxed),
                lo_received_queue_len: self.receive_queue.upgrade().map_or(0, |i| i.len()),
                lo_stored_queue_len: self.store_queue.upgrade().map_or(0, |i| i.len()),
                lo_committed_queue_len: self.commit_queue.upgrade().map_or(0, |i| i.len()),
            };

            println!("*******************************************************************");
            println!("    Status at {} (updated every: {}s)",
                     chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                     interval.as_secs());
            println!();

            println!("Progress Overview:");
            println!("    {}, {} of {} object have been migrated, ETA: {}",
                     Self::progress(now.lo_committed, total),
                     now.lo_committed,
                     total.map(|v| format!("{}", v)).unwrap_or_else(|| "UNKOWN".to_string()),
                     Self::calculate_eta(now.lo_committed, total, now.duration));
            println!();

            println!("Processed Objects by Thread Groups:");
            Self::print_thread_stats("observer thread",
                                     before.lo_observed,
                                     now.lo_observed,
                                     now.difference,
                                     now.duration);
            Self::print_thread_stats("receiver thread",
                                     before.lo_received,
                                     now.lo_received,
                                     now.difference,
                                     now.duration);
            Self::print_thread_stats("storer thread",
                                     before.lo_stored,
                                     now.lo_stored,
                                     now.difference,
                                     now.duration);
            Self::print_thread_stats("committer thread",
                                     before.lo_committed,
                                     now.lo_committed,
                                     now.difference,
                                     now.duration);
            println!();

            println!("Queue Usage:");
            Self::print_queue_stats("receive queue",
                                    before.lo_received_queue_len,
                                    now.lo_received_queue_len,
                                    self.receive_queue_size);
            Self::print_queue_stats("store queue",
                                    before.lo_stored_queue_len,
                                    now.lo_stored_queue_len,
                                    self.store_queue_size);
            Self::print_queue_stats("commit queue",
                                    before.lo_committed_queue_len,
                                    now.lo_committed_queue_len,
                                    self.commit_queue_size);
            println!();

            // `now` is the `before` status in the next loop
            before = now;

            // thread cancellation point
            if self.wait_for_at_most(interval, cancel_interval).is_err() {
                info!("all queues have seized to exist, nothing left to monitor, terminating \
                           thread");
                break;
            }
        }
    }

    fn are_all_queues_dropped(&self) -> bool {
        self.receive_queue.upgrade().is_none() && self.store_queue.upgrade().is_none() &&
        self.commit_queue.upgrade().is_none()
    }

    fn progress(completed: u64, total: Option<u64>) -> String {
        if let Some(total) = total {
            let percentage = completed as f32 / total as f32 * 100_f32;
            format!("{:.2}%", percentage)
        } else {
            "UNKNOWN".to_string()
        }
    }

    fn calculate_eta(lo_committed: u64, total: Option<u64>, duration: Duration) -> String {
        let secs = duration.as_secs();
        match total {
            Some(total) if lo_committed > 0 && secs > 0 => {
                let eta_secs = (total as f32 / lo_committed as f32 * secs as f32 - secs as f32) as
                               i64;
                let eta = chrono::Local::now() + chrono::Duration::seconds(eta_secs);
                let (h, m, s) = (eta_secs / 3600, eta_secs / 60 % 60, eta_secs % 60);
                format!("{} ({}h {:02}m {:02}s)",
                        eta.format("%Y-%m-%d %H:%M:%S"),
                        h,
                        m,
                        s)
            }
            _ => "UNKNOWN".to_string(),
        }
    }

    fn print_queue_stats(queue_name: &str, used_last: usize, used_now: usize, size: usize) {
        let percentage = used_now as f32 / size as f32 * 100_f32;
        println!("    {:16} - used {:6} of {:6}, {:6.2}% full, changed by: {:+6}",
                 queue_name,
                 used_now,
                 size,
                 percentage,
                 used_now as i64 - used_last as i64)
    }

    fn print_thread_stats(thread_name: &str,
                          seen_last: u64,
                          seen_now: u64,
                          difference: Duration,
                          duration: Duration) {
        // since start
        let avg_duration = duration.as_secs() as f32 +
                           duration.subsec_nanos() as f32 / 1_000_000_f32;
        let avg_speed = seen_now as f32 / avg_duration;

        // since last status update
        let diff_duration = difference.as_secs() as f32 +
                            difference.subsec_nanos() as f32 / 1_000_000_f32;
        let diff_speed = (seen_now - seen_last) as f32 / diff_duration;

        println!("    {:16} - processed: {:7}, current speed: {:7.1} Lo/s, average speed: {:7.1} \
                  Lo/s",
                 thread_name,
                 seen_now,
                 diff_speed,
                 avg_speed)
    }

    /// wait for `interval` but check every `cancel_interval` if thread should be cancelled
    ///
    /// returns Err(_) immediatly if thread should be cancelled and Ok(_) after `interval`
    /// otherwise.
    fn wait_for_at_most(&self, duration: Duration, cancel_interval: Duration) -> Result<(), ()> {
        let instant = Instant::now();
        while instant.elapsed() < duration {
            thread::sleep(cancel_interval);
            if self.are_all_queues_dropped() {
                return Err(());
            }
        }
        Ok(())
    }
}
