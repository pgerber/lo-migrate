//! Custom error type

use postgres;
use aws::errors::s3;
use std::io;
use std::result;
use std::sync::mpsc::SendError;
use lo::Lo;

pub type Result<T> = result::Result<T, MigrationError>;

#[derive(Debug)]
pub enum MigrationError {
    IoError(io::Error),
    PgConnError(postgres::error::ConnectError),
    PgError(postgres::error::Error),
    S3Error(s3::S3Error),
    SendError(SendError<Lo>),
    ThreadCancelled,
}

impl MigrationError {
    pub fn is_cancelled(&self) -> bool {
        if let MigrationError::ThreadCancelled = *self {
            true
        } else {
            false
        }
    }

    pub fn is_queue_hangup(&self) -> bool {
        if let MigrationError::SendError(_) = *self {
            true
        } else {
            false
        }
    }
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

impl From<SendError<Lo>> for MigrationError {
    fn from(err: SendError<Lo>) -> MigrationError {
        MigrationError::SendError(err)
    }
}
