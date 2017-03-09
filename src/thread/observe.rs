//! observer thread implementation
//!
//! The observer thread retrieves the list of Largo Objects and passes them to the receiver thread.

use fallible_iterator::FallibleIterator;
use postgres::Connection;
use postgres::rows::Row;
use postgres::transaction::Transaction;
use postgres::types::Oid;
use serialize::hex::FromHex;
use std::convert::TryFrom;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use two_lock_queue::Sender;

use error::Result;
use super::*;

pub struct Observer<'a> {
    stats: &'a ThreadStat,
    conn: &'a Connection,
}

impl<'a> Observer<'a> {
    pub fn new(thread_stat: &'a ThreadStat, conn: &'a Connection) -> Self {
        Observer {
            stats: thread_stat,
            conn: conn,
        }
    }

    pub fn start_worker(&self, tx: Arc<Sender<Lo>>, buffer_size: i32) -> Result<()> {
        let trx = self.conn.transaction()?;

        // count Large Objects
        let count = Some(self.count_objects(&trx)?);
        *self.stats.lo_total.lock() = count;

        let stmt = self.conn
            .prepare("SELECT hash, data, size, mime_type FROM _nice_binary where sha2 is NULL")?;
        let rows = stmt.lazy_query(&trx, &[], buffer_size)?;
        for row in rows.iterator() {
            self.queue(&tx, row?)?;

            // count received objects
            self.stats.lo_observed.fetch_add(1, Ordering::Relaxed);

            // thread cancellation point
            self.stats.cancellation_point()?;
        }

        info!("thread has completed its mission");
        Ok(())
    }

    /// count large object in database that still need to be moved to S3
    ///
    /// note: we pass in the transaction to be sure that the count is correct; Count must occur in
    ///       same transaction as retrieving the rows to be correct.
    fn count_objects(&self, _tx: &Transaction) -> Result<u64> {
        info!("counting large objects");
        let rows = self.conn.query("SELECT count(*) FROM _nice_binary where sha2 is NULL", &[])?;
        let count: i64 = rows.get(0).get(0);
        Ok(u64::try_from(count).expect("count should not be negative"))
    }

    /// add [`Lo`] to receiver queue
    fn queue(&self, tx: &Sender<Lo>, row: Row) -> Result<()> {
        let sha1_hex: String = row.get(0);
        let sha1 = sha1_hex.from_hex();
        let oid: Oid = row.get(1);
        let size: i64 = row.get(2);
        let mime_type: String = row.get(3);

        if let Ok(sha1) = sha1 {
            let lo = Lo::new(sha1, oid, size, mime_type);
            debug!("adding Lo to queue: {:?}", lo);
            tx.send(lo)?;
        } else {
            warn!("encountered _nice_binary entry with invalid hash {:?}",
                  sha1_hex)
        }
        Ok(())
    }
}
