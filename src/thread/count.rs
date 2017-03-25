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
        let count = Some(self.count_objects()?);
        *self.stats.lo_total.lock() = count;
        Ok(())
    }

    /// count large object in database that still need to be moved to S3
    ///
    /// note: we pass in the transaction to be sure that the count is correct; Count must occur in
    ///       same transaction as retrieving the rows to be correct.
    fn count_objects(&self) -> Result<u64> {
        info!("counting large objects");
        let rows = self.conn
            .query("SELECT count(*) FROM _nice_binary where sha2 is NULL \
                    AND hash ~ '^[0-9A-Fa-f]{40}$'",
                   &[])?;
        let count: i64 = rows.get(0).get(0);

        #[cfg_attr(feature = "clippy", allow(cast_sign_loss))]
        Ok(count as u64)
    }
}
