//! counter thread implementation
//!
//! Count the number of Large Objects.

use error::Result;
use postgres::Connection;
use thread::ThreadStat;

pub struct Counter<'a> {
    stats: &'a ThreadStat,
    conn: &'a Connection,
}

impl<'a> Counter<'a> {
    pub fn new(stats: &'a ThreadStat, conn: &'a Connection) -> Self {
        Counter {
            stats: stats,
            conn: conn,
        }
    }

    pub fn start_worker(&self) -> Result<()> {
        let (remaining, total) = self.count_objects()?;
        *self.stats.lo_remaining.lock().expect("failed to acquire lock") = Some(remaining);
        *self.stats.lo_total.lock().expect("failed to acquire lock") = Some(total);
        info!("thread has completed its mission");
        Ok(())
    }

    /// count large object in database that still need to be moved to S3
    ///
    /// note: we pass in the transaction to be sure that the count is correct; Count must occur in
    ///       same transaction as retrieving the rows to be correct.
    fn count_objects(&self) -> Result<(u64, u64)> {
        info!("counting large objects");
        let rows = self.conn
            .query("SELECT\n\
                        (SELECT count(*) FROM _nice_binary WHERE sha2 IS NULL),\n\
                        (SELECT count(*) from _nice_binary)",
                   &[])?;
        let row = rows.get(0);
        let remaining: i64 = row.get(0);
        let total: i64 = row.get(1);

        #[cfg_attr(feature = "clippy", allow(cast_sign_loss))]
        Ok((remaining as u64, total as u64))
    }
}
