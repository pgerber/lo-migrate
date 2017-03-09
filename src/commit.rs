//! Commit sha2 hashes to the database
//!
//! used by commiter thread

use lo::Lo;
use postgres::Connection;
use error::Result;

/// Commit the sha2 hashes of the given [`Lo`]s to database.
pub fn commit(conn: &Connection, objects: &[Lo]) -> Result<()> {
    let stmt = conn.prepare_cached("UPDATE _nice_binary SET sha2 = $1 WHERE hash = $2")?;
    let tx = conn.transaction()?;

    for lo in objects {
        if stmt.execute(&[lo.sha2().expect("SHA2 hash unknown"), &lo.sha1_hex()])? == 0 {
            info!("could not update sha2 hash for lo (did it vanish?): {:?}",
                  &lo);
        }
    }

    tx.commit()?;
    Ok(())
}
