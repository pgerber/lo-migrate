#![cfg(feature = "postgres_tests")]
extern crate lo_migrate;
extern crate postgres;

mod common;
use common::*;

#[test]
fn batch_job_active() {
    assert_eq!(batch_job_exists_helper(true).unwrap_err(),
               "Batch job \"nice2.dms.DeleteUnreferencedBinariesBatchJob\" must be deactivated \
                before the migration can be started");
}

#[test]
fn batch_job_inactive() {
    assert!(batch_job_exists_helper(false).is_ok());
}

fn batch_job_exists_helper(active: bool) -> Result<(), String> {
    let conn = postgres_conn();
    create_batch_job_table(&conn);
    create_batch_job(&conn, active);
    lo_migrate::utils::check_batch_job_is_disabled(&conn)
}

#[test]
fn batch_job_missing() {
    let conn = postgres_conn();
    create_batch_job_table(&conn);
    assert_eq!(lo_migrate::utils::check_batch_job_is_disabled(&conn).unwrap_err(),
               r#"Batch job "nice2.dms.DeleteUnreferencedBinariesBatchJob" not found"#);
}

fn create_batch_job_table(conn: &postgres::Connection) {
    conn.batch_execute(r##"
                           CREATE TABLE nice_batch_job (
                               id VARCHAR(255) NOT NULL,
                               active BOOLEAN NOT NULL
                           );

                           INSERT INTO
                               nice_batch_job (id, active)
                           VALUES
                               ('nice2.dms.RandomBatchJob', true),
                               ('nice2.optional.an.Arbitrary', false);
                       "##)
        .unwrap();
}

fn create_batch_job(conn: &postgres::Connection, active: bool) {
    conn.execute(r##"
                    INSERT INTO
                        nice_batch_job (id, active)
                    VALUES
                        ('nice2.dms.DeleteUnreferencedBinariesBatchJob', $1);
                 "##,
                 &[&active])
        .unwrap();
}
