extern crate lo_migrate;
extern crate two_lock_queue as queue;

use lo_migrate::Lo;
use lo_migrate::thread::{Monitor, ThreadStat};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::thread::{self, JoinHandle};
use queue::Receiver;

#[test]
fn queues_hang_up() {
    let (handle, rx, stats) = start_monitor(86400);
    let start = Instant::now();
    thread::spawn(|| {
        thread::sleep(Duration::from_secs(3));
        drop(rx);
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

fn start_monitor(secs: u64) -> (JoinHandle<()>, Arc<Receiver<Lo>>, ThreadStat) {
    let (_, rx) = queue::unbounded();
    let rx = Arc::new(rx);
    let stats = ThreadStat::new();

    let rx_weak = Arc::downgrade(&rx);
    let stats_thread = stats.clone();
    let handle = thread::spawn(move || {
        let monitor = Monitor {
            stats: &stats_thread,
            receive_queue: rx_weak.clone(),
            receive_queue_size: 10,
            store_queue: rx_weak.clone(),
            store_queue_size: 15,
            commit_queue: rx_weak,
            commit_queue_size: 20,
        };
        monitor.start_worker(Duration::from_secs(secs));
    });
    (handle, rx, stats)
}
