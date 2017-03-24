extern crate lo_migrate;
extern crate rand;
extern crate two_lock_queue as queue;

use lo_migrate::Lo;
use lo_migrate::thread::{Monitor, ThreadStat};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::thread::{self, JoinHandle};
use queue::Receiver;
use rand::Rng;

#[test]
fn queues_hang_up() {
    let (handle, mut rxes, stats) = start_monitor(86400);
    let start = Instant::now();
    thread::spawn(move || {
        let mut rng = rand::thread_rng();
        rng.shuffle(&mut rxes);
        for _rx in rxes {
            thread::sleep(Duration::from_secs(1));
            // _rx is dropped here (Only once when last `Receiver` is dropped the `Monitor`
            // thread must exit.)
        }
    });
    handle.join().unwrap();
    assert!(start.elapsed() >= Duration::from_secs(3));
    assert!(!stats.is_cancelled());
}

#[test]
fn cancelled() {
    let (handle, _rxes, stats) = start_monitor(1);
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

fn start_monitor(secs: u64) -> (JoinHandle<()>, Vec<Arc<Receiver<Lo>>>, ThreadStat) {
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
    (handle, vec![rx1, rx2, rx3], stats)
}
