use postgres;
use std::io;
use std::result;

pub type Result<T> = result::Result<T, MigrationError>;

#[derive(Debug)]
pub enum MigrationError {
    PgConnError(postgres::error::ConnectError),
    PgError(postgres::error::Error),
    IoError(io::Error),
}

impl From<postgres::error::ConnectError> for MigrationError {
    fn from(err: postgres::error::ConnectError) -> MigrationError {
        MigrationError::PgConnError(err)
    }
}

impl From<postgres::error::Error> for MigrationError {
    fn from(err: postgres::error::Error) -> MigrationError {
        MigrationError::PgError(err)
    }
}

impl From<io::Error> for MigrationError {
    fn from(err: io::Error) -> MigrationError {
        MigrationError::IoError(err)
    }
}
