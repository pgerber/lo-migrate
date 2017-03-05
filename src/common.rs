use postgres;
use aws::errors::s3;
use std::io;
use std::result;

pub type Result<T> = result::Result<T, MigrationError>;

#[derive(Debug)]
pub enum MigrationError {
    PgConnError(postgres::error::ConnectError),
    PgError(postgres::error::Error),
    IoError(io::Error),
    S3Error(s3::S3Error),
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

impl From<s3::S3Error> for MigrationError {
    fn from(err: s3::S3Error) -> MigrationError {
        MigrationError::S3Error(err)
    }
}
