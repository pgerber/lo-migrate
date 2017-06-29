//! Collections of small utility functions

use std::io::Write;
use postgres::Connection;
use postgres::error::Error as PgError;

const BATCH_NAME: &str = "nice2.dms.DeleteUnreferencedBinariesBatchJob";

/// Disable Nice's `DeleteUnreferencedBinariesBatchJob`
///
/// Marks the batch job `DeleteUnreferencedBinariesBatchJob` as inactive. A user-friendly status
/// message is written to `output`.
///
/// The absents of the batch or it being inactive already is not considered an error.
pub fn disable_batch_job(output: &mut Write, conn: &Connection) -> Result<(), PgError> {
    let _ = write!(output, "Disabling batchjob {:?} ... ", BATCH_NAME);
    let result = conn.execute("UPDATE nice_batch_job SET active = false WHERE id = $1",
                              &[&BATCH_NAME]);

    match result {
        Ok(1) => {
            let _ = write!(output, "done");
            Ok(())
        }
        Ok(0) => {
            let _ = write!(output, "skipped (no such batchjob)");
            Ok(())
        }
        Err(e) => {
            let _ = write!(output, "failed");
            Err(e)
        }
        _ => unreachable!(),
    }
}
