extern crate lo_migrate;
extern crate two_lock_queue as queue;

use lo_migrate::Lo;
use lo_migrate::thread::{Monitor, ThreadStat};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::thread::{self, JoinHandle};
use queue::Receiver;

type Receivers = (Option<Arc<Receiver<Lo>>>, Option<Arc<Receiver<Lo>>>, Option<Arc<Receiver<Lo>>>);

#[test]
fn queues_hang_up() {
    let (handle, mut rx, stats) = start_monitor(86400);
    let start = Instant::now();
    thread::spawn(move || {
        thread::sleep(Duration::from_secs(1));
        drop(rx.0.take()); // hang up first queue
        thread::sleep(Duration::from_secs(1));
        drop(rx.1.take()); // hang up second queue
        thread::sleep(Duration::from_secs(1));
        // hang up third queue
    });
    handle.join().unwrap();
    assert!(start.elapsed() >= Duration::from_secs(3));
    assert!(!stats.is_cancelled());
}

#[test]
fn cancelled() {
    let (handle, _rx, stats) = start_monitor(1);
    let start = Instant::now();
    let stats_thread = stats.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_secs(3));
        stats_thread.cancel();
    });
    handle.join().unwrap();
    assert!(start.elapsed() >= Duration::from_secs(3));
    assert!(stats.is_cancelled());
}

fn start_monitor(secs: u64) -> (JoinHandle<()>, Receivers, ThreadStat) {
    let (_, rx1) = queue::unbounded();
    let rx1 = Arc::new(rx1);
    let (_, rx2) = queue::unbounded();
    let rx2 = Arc::new(rx2);
    let (_, rx3) = queue::unbounded();
    let rx3 = Arc::new(rx3);
    let stats = ThreadStat::new();

    let rx1_weak = Arc::downgrade(&rx1);
    let rx2_weak = Arc::downgrade(&rx2);
    let rx3_weak = Arc::downgrade(&rx3);
    let stats_thread = stats.clone();
    let handle = thread::spawn(move || {
        let monitor = Monitor {
            stats: &stats_thread,
            receive_queue: rx1_weak,
            receive_queue_size: 10,
            store_queue: rx2_weak,
            store_queue_size: 15,
            commit_queue: rx3_weak,
            commit_queue_size: 20,
        };
        monitor.start_worker(Duration::from_secs(secs));
    });
    (handle, (Some(rx1), Some(rx2), Some(rx3)), stats)
}
