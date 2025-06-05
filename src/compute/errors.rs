// src/compute/errors.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ComputeError {
    #[error("Task submission failed: {0}")]
    SubmissionFailed(String),

    #[error("Task execution failed: {0}")]
    ExecutionFailed(String),

    #[error("Result retrieval failed: {0}")]
    ResultRetrievalFailed(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Function not found: {0}")]
    FunctionNotFound(String),

    #[error("Channel send error: {0}")]
    ChannelSendError(String),

    #[error("Channel receive error: {0}")]
    ChannelReceiveError(String),

    #[error("Unknown compute error")]
    Unknown,
}

// Implement From for common errors to make it easier to convert
// For example, for oneshot::error::RecvError
impl From<tokio::sync::oneshot::error::RecvError> for ComputeError {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        ComputeError::ChannelReceiveError(err.to_string())
    }
}

// Add similar From implementations if other specific errors become common
