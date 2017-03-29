//! Error handling

use postgres;
use aws::errors::s3;
use std::io;
use std::result;
use std::sync::mpsc::SendError;
use lo::Lo;

/// `Result` expecting `MigrationError` as `Err`.
pub type Result<T> = result::Result<T, MigrationError>;

/// Custom error type to be used by all public functions and method
#[derive(Debug, Error)]
pub enum MigrationError {
    /// I/O error
    IoError(io::Error),
    /// Postgres connection error
    PgConnError(postgres::error::ConnectError),
    /// Postgres error
    PgError(postgres::error::Error),
    /// AWS S3 error
    S3Error(s3::S3Error),
    /// Queue send error
    SendError(SendError<Lo>),
    /// Thread cancelled error
    ThreadCancelled,
}

impl MigrationError {
    /// true if error was caused by a thread cancelling
    pub fn is_cancelled(&self) -> bool {
        if let MigrationError::ThreadCancelled = *self {
            true
        } else {
            false
        }
    }

    /// true if the error was caused by a hang up queue
    pub fn is_queue_hangup(&self) -> bool {
        if let MigrationError::SendError(_) = *self {
            true
        } else {
            false
        }
    }
}
