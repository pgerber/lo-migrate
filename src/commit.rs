use lo::Lo;
use postgres::Connection;
use common::Result;

/// Commit the sha2 hashes of the given [`Lo`]s to database.
pub fn commit(objects: &[Lo], conn: &Connection) -> Result<()> {
    let stmt = conn.prepare("UPDATE _nice_binary SET sha2 = $1 WHERE hash = $2")?;
    let tx = conn.transaction()?;

    for lo in objects {
        if stmt.execute(&[lo.sha2().unwrap(), &lo.sha1_hex()])? == 0 {
            info!("could not update sha2 hash for lo (did it vanish?): {:?}", &lo);
        }
    }

    tx.commit()?;
    Ok(())
}
