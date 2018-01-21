//! Error handling

use postgres;
use rusoto_s3::{ CompleteMultipartUploadError, CreateMultipartUploadError, PutObjectError,
                 UploadPartError };
use std::io;
use std::result;
use std::sync::mpsc::SendError;
use lo::Lo;

/// `Result` expecting `MigrationError` as `Err`.
pub type Result<T> = result::Result<T, MigrationError>;

/// Custom error type to be used by all public functions and method
#[derive(Debug, Error)]
pub enum MigrationError {
    /// Failed to complete upload
    CompleteMultipartUploadError(CompleteMultipartUploadError),
    /// Failed to create multipart upload
    CreateMultipartUploadError(CreateMultipartUploadError),
    /// I/O error
    IoError(io::Error),
    /// Postgres connection error
    PgConnError(postgres::error::ConnectError),
    /// Postgres error
    PgError(postgres::error::Error),
    /// Failed to store object
    PutObjectError(PutObjectError),
    /// Queue send error
    SendError(SendError<Lo>),
    /// Thread cancelled error
    ThreadCancelled,
    /// Failed to upload part
    UploadPartError(UploadPartError),
    /// Invalid object
    #[error(msg_embedded, no_from, non_std)]
    InvalidObject(String)
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
