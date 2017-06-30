//! Collections of small utility functions

use postgres::Connection;

const BATCH_NAME: &str = "nice2.dms.DeleteUnreferencedBinariesBatchJob";

/// Ensure Nice's `DeleteUnreferencedBinariesBatchJob` is no longer active
///
/// An error is returned if the batch job is still active or if it doesn't exists.
pub fn check_batch_job_is_disabled(conn: &Connection) -> Result<(), String> {
    let rows = conn.query("SELECT active FROM nice_batch_job WHERE id = $1",
               &[&BATCH_NAME])
        .expect("SQL query failed");

    match rows.len() {
        0 => Err(format!("Batch job {:?} not found", BATCH_NAME)),
        1 => {
            let active: bool = rows.get(0).get(0);
            if active {
                Err(format!("Batch job {:?} must be deactivated before the migration can be \
                             started",
                            BATCH_NAME))
            } else {
                Ok(())
            }
        }
        _ => unreachable!("query returned {} rows instead of zero or one", rows.len()),
    }
}
