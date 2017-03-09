//! committer thred implementation
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
        let mut lo_chunk = Vec::with_capacity(chunk_size);

        loop {
            self.receive_next_chunk(&rx, &mut lo_chunk, chunk_size);

            // commit sha2 hash to DB
            commit::commit(self.conn, &lo_chunk)?;

            // increase counter of committed `Lo`s
            self.stats.lo_committed.fetch_add(lo_chunk.len() as u64, Ordering::Relaxed);

            if lo_chunk.len() < chunk_size {
                break; // sender hung up queue
            }

            // clear committed `Lo`s
            lo_chunk.clear();

            // thread cancellation point
            self.stats.cancellation_point()?;
        }

        debug_assert_eq!(lo_chunk.capacity(), chunk_size, "capacity of `Vec` changed");
        info!("thread has completed its mission, rx queue hang up");
        Ok(())
    }

    pub fn receive_next_chunk(&self,
                              rx: &Receiver<Lo>,
                              lo_chunk: &mut Vec<Lo>,
                              chunk_size: usize) {
        while let Ok(lo) = rx.recv() {
            lo_chunk.push(lo);

            if lo_chunk.len() >= chunk_size {
                break;
            }
        }
    }
}
