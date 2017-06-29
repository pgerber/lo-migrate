#![cfg(feature = "postgres_tests")]
extern crate lo_migrate;
extern crate postgres;

mod common;
use common::*;

#[test]
fn batch_job_active() {
    batch_job_exists_helper(true);
}

#[test]
fn batch_job_inactive() {
    batch_job_exists_helper(false);
}

fn batch_job_exists_helper(active: bool) {
    let conn = postgres_conn();
    create_batch_job_table(&conn);
    create_batch_job(&conn, active);
    let mut output = Vec::new();
    lo_migrate::utils::disable_batch_job(&mut output, &conn).unwrap();
    assert_eq!(String::from_utf8_lossy(&output),
               r#"Disabling batchjob "nice2.dms.DeleteUnreferencedBinariesBatchJob" ... done"#);
}

#[test]
fn batch_job_missing() {
    let conn = postgres_conn();
    create_batch_job_table(&conn);
    let mut output = Vec::new();
    lo_migrate::utils::disable_batch_job(&mut output, &conn).unwrap();
    assert_eq!(String::from_utf8_lossy(&output),
               "Disabling batchjob \"nice2.dms.DeleteUnreferencedBinariesBatchJob\" ... \
               skipped (no such batchjob)");
}

#[test]
fn batch_job_failure() {
    let conn = postgres_conn();
    let mut output = Vec::new();
    assert!(lo_migrate::utils::disable_batch_job(&mut output, &conn).is_err());
    assert_eq!(String::from_utf8_lossy(&output),
               r#"Disabling batchjob "nice2.dms.DeleteUnreferencedBinariesBatchJob" ... failed"#);
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
