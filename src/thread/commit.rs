//! committer thread implementation
//!
//! The committer threads receives `Lo`s from the storer thread and commits the sha2 hashes
//! to the database.

use error::Result;
use lo::Lo;
use postgres::Connection;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use thread::ThreadStat;
use commit;
use two_lock_queue::Receiver;

pub struct Committer<'a> {
    stats: &'a ThreadStat,
    conn: &'a Connection,
}

impl<'a> Committer<'a> {
    pub fn new(thread_stat: &'a ThreadStat, conn: &'a Connection) -> Self {
        Committer {
            stats: thread_stat,
            conn: conn,
        }
    }

    pub fn start_worker(&self, rx: Arc<Receiver<Lo>>, chunk_size: usize) -> Result<()> {
        let mut lo_chunk: Vec<_> =
            (0..chunk_size).map(|_| Lo::new(vec![], 0, i64::min_value(), "".to_string())).collect();

        loop {
            let size = Self::receive_next_chunk(&rx, &mut lo_chunk[..]);

            // commit sha2 hash to DB
            commit::commit(self.conn, &lo_chunk[..size])?;

            // increase counter of committed `Lo`s
            self.stats.lo_committed.fetch_add(size as u64, Ordering::Relaxed);

            if size < chunk_size {
                break; // sender hung up queue
            }

            // thread cancellation point
            self.stats.cancellation_point()?;
        }

        debug_assert_eq!(lo_chunk.capacity(), chunk_size, "capacity of `Vec` changed");
        info!("thread has completed its mission, sender hung up queue");
        Ok(())
    }

    fn receive_next_chunk(rx: &Receiver<Lo>, lo_chunk: &mut [Lo]) -> usize {
        for (i, mut item) in lo_chunk.iter_mut().enumerate() {
            match rx.recv() {
                Ok(lo) => *item = lo,
                _ => return i,
            }
        }
        lo_chunk.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use two_lock_queue::{self, Sender};

    #[test]
    fn receive_next_chunk() {
        let (tx, rx) = two_lock_queue::channel(5);
        thread::spawn(move || { send_objects(tx, 12); });

        let mut buffer: Vec<_> =
            (0..10).map(|_| Lo::new(vec![], 1000, 1000, "".to_string())).collect();
        let size = Committer::receive_next_chunk(&rx, &mut buffer[..]);
        assert!(buffer[..size].iter().map(|i| i.size()).eq(0..10));

        let size = Committer::receive_next_chunk(&rx, &mut buffer[..]);
        assert!(buffer[..size].iter().map(|i| i.size()).eq(10..12));

        let count = Committer::receive_next_chunk(&rx, &mut buffer);
        assert_eq!(count, 0);
    }

    fn send_objects(tx: Sender<Lo>, count: i64) {
        for i in 0..count {
            let lo = Lo::new(vec![], 0, i, "octet/stream".to_string());
            tx.send(lo).unwrap();
        }
    }
}
